#include "slime_backend.h"

namespace slime {
namespace c10d {

void logAndThrow(const std::string& logMessage, const std::string& errorMessage)
{
    LOG(ERROR) << logMessage;
    TORCH_CHECK(false, errorMessage);
}

slimeBackend::slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank, int size):
    Backend(rank, size), store_(store)
{

    // available_nic() is only used to get the device name
    std::vector<std::string> available_devices = available_nic();
    size_t                   idx               = rank_ % available_devices.size();

    // TODO: maybe we need a structure to transfer the RDMA device info
    const std::string dev_name  = available_devices[idx];
    const std::string link_type = "RoCE";
    uint8_t           ib_port   = 1;
    size_t            qp_num    = 4;
    auto              end_point = std::make_shared<RDMAEndpoint>(dev_name, ib_port, link_type, qp_num);

    json data_channel_info = end_point->getDataContextInfo();
    json meta_channel_info = end_point->getMetaContextInfo();

    channel_info_["data_channel"] = data_channel_info;
    channel_info_["meta_channel"] = meta_channel_info;

    exchangeChannelInfo();

    // Establish the connection among the RDMA Endpoints
    try {

        for (int i = 0; i < size_; ++i) {
            if (i == rank_)
                continue;

            json peer_channel_info = peers_channel_info_[i];
            end_point->contextConnect(peer_channel_info["data_channel"], peer_channel_info["meta_channel"]);
        }
    }
    catch (const std::runtime_error& e) {
        auto err = e.what();
        // TORCH_CHECK to print the cpp stacktrace.
        auto msg = c10::str("Gloo connectFullMesh failed with ", err);
        logAndThrow(msg, msg);
    }
}

void slimeBackend::exchangeChannelInfo()
{

    auto        channel_info = channel_info_.dump();
    std::string key          = "SLIME_ENDPOINT_" + std::to_string(rank_);
    store_->set(key, channel_info);

    std::vector<std::string> peers_keys;
    for (size_t i = 0; i < size_; ++i) {
        peers_keys.push_back("SLIME_ENDPOINT_" + std::to_string(i));
    }
    store_->wait(peers_keys);

    peers_channel_info_.resize(size_);
    for (size_t i = 0; i < size_; ++i) {
        auto peer_key          = store_->get(peers_keys[i]);
        peers_channel_info_[i] = json::parse(peer_key);
    }
}
// static c10::intrusive_ptr<Backend> createSlimeBackend(const c10::intrusive_ptr<::c10d::Store>& store,
//                                                       int                                      rank,
//                                                       int                                      size,
//                                                       const std::chrono::duration<float>&)

// {
//     return c10::make_intrusive<slimeBackend>(store, rank, size);
// }

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m)
{
    m.def("createDLSlimeBackend", &slimeBackend::createSlimeBackend);
}

}  // namespace c10d
}  // namespace slime