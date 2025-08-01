#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <gflags/gflags.h>
#include <iomanip>
#include <numeric>
#include <thread>
#include <zmq.hpp>

using json = nlohmann::json;
using namespace slime;
using namespace std::chrono;

DEFINE_string(DEVICE_NAME, "rxe_0", "RDMA device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");
DEFINE_int32(IB_PORT, 1, "RDMA port number");
DEFINE_string(PEER_ADDR, "127.0.0.1", "peer IP address");
DEFINE_int32(PORT_DATA, 5557, "ZMQ control port");
DEFINE_int32(PORT_META, 5558, "ZMQ control port");
DEFINE_string(OUTPUT_FILE, "rdma_test_results.csv", "output file for performance results");

DEFINE_bool(send, false, "Run in send mode");
DEFINE_bool(recv, false, "Run in recv mode");

DEFINE_int32(num_qp, 2, "Number of QPs");
DEFINE_int32(num_packets, 100, "Number of packets");
DEFINE_int32(min_packet_size, 12, "Minimum size of packet size (2^(min_packet_size) bytes)");
DEFINE_int32(max_packet_size, 16, "Maximum size of packet size (2^(max_packet_size) bytes)");

typedef struct Result {
    size_t packet_size;
    size_t total_bytes;
    size_t packet_num;
    double min_latency_ms;
    double max_latency_ms;
    double avg_latency_ms;
    double stddev_latency;
    double min_bandwidth_MBs;
    double max_bandwidth_MBs;
    double avg_bandwidth_MBs;
    double stddev_bandwidth;
    double success_rate;

} Result_t;

void initConnection(std::shared_ptr<RDMAEndpoint>& end_point)
{
    if (FLAGS_send) {
        std::cout << "Initializing RDMA endpoint in SEND mode..." << std::endl;

        zmq::context_t zmq_ctx_data(2);
        zmq::context_t zmq_ctx_meta(2);

        zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REP);
        zmq::socket_t sock_meta(zmq_ctx_meta, ZMQ_REP);

        sock_data.bind("tcp://*:" + std::to_string(FLAGS_PORT_DATA));
        sock_meta.bind("tcp://*:" + std::to_string(FLAGS_PORT_META));

        zmq::message_t peer_data_info;
        zmq::message_t peer_meta_info;
        auto           data_res = sock_data.recv(peer_data_info);
        auto           meta_res = sock_meta.recv(peer_meta_info);

        zmq::message_t local_data_info(end_point->getDataContextInfo().dump());
        zmq::message_t local_meta_info(end_point->getMetaContextInfo().dump());

        sock_data.send(local_data_info, zmq::send_flags::none);
        sock_meta.send(local_meta_info, zmq::send_flags::none);

        end_point->connect(json::parse(peer_data_info.to_string()), json::parse(peer_meta_info.to_string()));
    }

    else if (FLAGS_recv) {
        std::cout << "Initializing RDMA endpoint in RECV mode..." << std::endl;

        zmq::context_t zmq_ctx_data(2);
        zmq::context_t zmq_ctx_meta(2);

        zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REQ);
        zmq::socket_t sock_meta(zmq_ctx_meta, ZMQ_REQ);

        sock_data.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_DATA));
        sock_meta.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_META));

        zmq::message_t local_data_info(end_point->getDataContextInfo().dump());
        zmq::message_t local_meta_info(end_point->getMetaContextInfo().dump());

        sock_data.send(local_data_info, zmq::send_flags::none);
        sock_meta.send(local_meta_info, zmq::send_flags::none);

        zmq::message_t peer_data_info;
        zmq::message_t peer_meta_info;
        auto           data_res = sock_data.recv(peer_data_info);
        auto           meta_res = sock_meta.recv(peer_meta_info);

        end_point->connect(json::parse(peer_data_info.to_string()), json::parse(peer_meta_info.to_string()));
    }

    std::cout << "RDMA Endpoint connection has been successfully established." << std::endl;
}

int singleTest(std::shared_ptr<RDMAEndpoint> end_point,
               std::shared_ptr<RDMABuffer>   buf,
               size_t                        iterations,
               size_t                        packet_size,
               double&                       latency,
               double&                       bandwidth)
{

    std::vector<std::future<void>> futures;
    std::atomic<int>               completed(0);
    // warm up
    if (FLAGS_send) {
        buf->send();
        buf->waitSend();
    }
    else if (FLAGS_recv) {
        buf->recv();
        buf->waitRecv();
    }
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < iterations; i++) {
        if (FLAGS_send) {
            buf->send();
            futures.emplace_back(std::async(std::launch::async, [&buf, &completed]() {
                buf->waitSend();
                completed++;
            }));
        }
        else if (FLAGS_recv) {
            buf->recv();
            futures.emplace_back(std::async(std::launch::async, [&buf, &completed]() {
                buf->waitRecv();
                completed++;
            }));
        }
    }
    for (auto& fut : futures) {
        fut.wait();
    }
    auto                          end      = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    double total_bytes = packet_size * iterations;
    latency            = (duration.count() * 1000) / FLAGS_num_packets;
    bandwidth          = (total_bytes / duration.count()) / (1024 * 1024);
    return completed;
}

void runTest(std::shared_ptr<RDMAEndpoint> end_point, size_t packet_size, size_t total_bytes, Result_t& result)
{
    const size_t           num_packets = total_bytes / packet_size;
    std::vector<char>      data_buffer(packet_size, FLAGS_send ? 'A' : '0');
    std::vector<uintptr_t> ptrs    = {reinterpret_cast<uintptr_t>(data_buffer.data())};
    std::vector<size_t>    offsets = {0};
    std::vector<size_t>    sizes   = {data_buffer.size()};
    auto                   buf     = std::make_shared<RDMABuffer>(end_point, ptrs, offsets, sizes);

    std::vector<double> latencies;
    std::vector<double> bandwidths;
    std::vector<double> success_rates;

    size_t num_tests  = 10;
    size_t iterations = FLAGS_num_packets / num_tests;
    for (size_t i = 0; i < num_tests; ++i) {
        double latency;
        double bandwidth;
        int    success_count = singleTest(end_point, buf, iterations, packet_size, latency, bandwidth);
        latencies.push_back(latency);
        bandwidths.push_back(bandwidth);
        success_rates.push_back((double)success_count / num_tests);
    }
    // statistic
    auto [min_lat, max_lat] = std::minmax_element(latencies.begin(), latencies.end());
    double sum_lat          = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double mean_lat         = sum_lat / FLAGS_num_packets;
    double sq_sum_lat       = std::inner_product(latencies.begin(), latencies.end(), latencies.begin(), 0.0);
    double stdev_lat        = std::sqrt(sq_sum_lat / FLAGS_num_packets - mean_lat * mean_lat);

    auto [min_bw, max_bw] = std::minmax_element(bandwidths.begin(), bandwidths.end());
    double sum_bw         = std::accumulate(bandwidths.begin(), bandwidths.end(), 0.0);
    double mean_bw        = sum_bw / FLAGS_num_packets;
    double sq_sum_bw      = std::inner_product(bandwidths.begin(), bandwidths.end(), bandwidths.begin(), 0.0);
    double stdev_bw       = std::sqrt(sq_sum_bw / FLAGS_num_packets - mean_bw * mean_bw);

    double avg_success = std::accumulate(success_rates.begin(), success_rates.end(), 0.0) / FLAGS_num_packets;

    // Store results
    result.packet_size       = packet_size;
    result.total_bytes       = total_bytes;
    result.packet_num        = FLAGS_num_packets;
    result.min_latency_ms    = *min_lat;
    result.max_latency_ms    = *max_lat;
    result.avg_latency_ms    = mean_lat;
    result.stddev_latency    = stdev_lat;
    result.min_bandwidth_MBs = *min_bw;
    result.max_bandwidth_MBs = *max_bw;
    result.avg_bandwidth_MBs = mean_bw;
    result.stddev_bandwidth  = stdev_bw;
    result.success_rate      = avg_success;
}

void print(const std::vector<Result_t>& results)
{

    std::cout << "\nPerformance Results:\n";
    std::cout << std::left << std::setw(16) << "Size (Bytes)" << std::setw(16) << "Total Size (Bytes)" << std::setw(24)
              << "Avg Lat (ms)" << std::setw(24) << "Avg BW (MB/s)" << std::endl;

    for (const auto& res : results) {
        std::cout << std::left << std::setw(16) << res.packet_size << std::setw(16) << res.total_bytes << std::setw(24)
                  << std::setprecision(4) << res.avg_latency_ms << std::setw(24) << std::setprecision(4)
                  << res.avg_bandwidth_MBs << std::endl;
    }
}

void save(const std::vector<Result_t>& results, const std::string& filename)
{
    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        std::cerr << "Failed to open output file: " << filename << std::endl;
        return;
    }

    outfile << "Packet Size (Bytes),Total Bytes (Bytes),Amount of Packet"
            << "Min Latency (ms),Max Latency (ms),Avg Latency (ms), Latency StdDev (ms),"
            << "Min Bandwidth (MB/s),Max Bandwidth (MB/s),Avg Bandwidth (MB/s),Bandwidth StdDev (MB/s),"
            << "Success Rate (%)\n";

    for (const auto& res : results) {
        outfile << res.packet_size << "," << res.total_bytes << "," << res.packet_num << "," << std::setprecision(9)
                << res.min_latency_ms << "," << res.max_latency_ms << "," << res.avg_latency_ms << ","
                << res.stddev_latency << "," << res.min_bandwidth_MBs << "," << res.max_bandwidth_MBs << ","
                << res.avg_bandwidth_MBs << "," << res.stddev_bandwidth << "," << res.success_rate << "\n";
    }

    outfile.close();
    std::cout << "Results saved to " << filename << std::endl;
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (!FLAGS_send && !FLAGS_recv) {
        std::cerr << "Please specify mode: --send or --recv" << std::endl;
        return 1;
    }

    if (FLAGS_send && FLAGS_recv) {
        std::cerr << "Cannot specify both --send and --recv" << std::endl;
        return 1;
    }

    auto end_point = std::make_shared<RDMAEndpoint>(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, FLAGS_num_qp);
    initConnection(end_point);

    std::vector<size_t> packet_sizes;
    for (int i = FLAGS_min_packet_size; i <= FLAGS_max_packet_size; i++) {
        packet_sizes.push_back(1 << i);
    }

    std::vector<Result_t> results;

    for (size_t size : packet_sizes) {

        size_t total_bytes = size * FLAGS_num_packets;
        std::cout << "\nTesting with packet size: " << size << " bytes (" << (size >> 10)
                  << " KB), total: " << (double)total_bytes / (1024 * 1024)
                  << " MB, number of packets: " << FLAGS_num_packets << std::endl;

        Result_t result;
        runTest(end_point, size, total_bytes, result);
        results.push_back(result);
    }

    print(results);
    // save(results, FLAGS_OUTPUT_FILE);

    return 0;
}