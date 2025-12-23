#include "slime_backend.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_context_pool.h"
#include "engine/rdma/rdma_endpoint.h"
#include "engine/rdma/rdma_env.h"
#include "engine/rdma/rdma_future.h"
#include "engine/rdma/rdma_utils.h"
#include "engine/rdma/rdma_worker.h"
#include "engine/rdma/rdma_worker_pool.h"
#include "logging.h"

#ifdef SLIME_USE_CUDA
#include <ATen/cuda/CUDAContext.h>
#include <c10/cuda/CUDAStream.h>
#endif

#include <memory>

namespace slime {
namespace c10d {

int mod_positive(int a, int b)
{
    int r = a % b;
    return (r < 0) ? r + b : r;
}
void logAndThrow(const std::string& logMessage, const std::string& errorMessage)
{
    LOG(ERROR) << logMessage;
    TORCH_CHECK(false, errorMessage);
}

static at::Tensor& checkSingleTensor(std::vector<at::Tensor>& tensors)
{
    if (tensors.size() != 1) {
        TORCH_CHECK(false, "ProcessGroupGloo::send takes a single tensor");
    }
    auto& tensor = tensors[0];
    if (!tensor.is_contiguous()) {
        TORCH_CHECK(false, "input tensor has to be contiguous");
    }
    if (tensor.is_sparse()) {
        TORCH_CHECK(false, "input tensor has to be dense");
    }
    return tensor;
}

static uint32_t checkTag(int32_t tag)
{
    TORCH_CHECK(tag >= 0, "Tag must be nonnegative");
    return (uint32_t)tag;
}

SendWork::SendWork(std::vector<at::Tensor>&      tensor,
                   std::shared_ptr<RDMAEndpoint> endpoint,
                   std::shared_ptr<SendFuture>   slot,
                   uint64_t                      seq):
    Work(-1, ::c10d::OpType::SEND), tensor_(tensor), endpoint_(endpoint), slot_(slot), seq_(seq)
{
}

bool SendWork::wait(std::chrono::milliseconds timeout)
{
    bool               sendCompleted = false;
    std::exception_ptr exception{nullptr};
    try {
        if (timeout == kNoTimeout) {
            sendCompleted = slot_->wait();
        }
        else {
            sendCompleted = slot_->wait();
        }
    }
    catch (...) {
        exception = std::current_exception();
    }

    finishAndThrow(exception);
    return sendCompleted;
}

RecvWork::RecvWork(std::vector<at::Tensor>&      tensor,
                   std::shared_ptr<RDMAEndpoint> endpoint,
                   std::shared_ptr<RecvFuture>   slot,
                   uint64_t                      seq):
    Work(-1, ::c10d::OpType::SEND), tensor_(tensor), endpoint_(endpoint), slot_(slot), seq_(seq)
{
}

bool RecvWork::wait(std::chrono::milliseconds timeout)
{
    bool               recvCompleted = false;
    std::exception_ptr exception{nullptr};
    try {
        if (timeout == kNoTimeout) {
            recvCompleted = slot_->wait();
        }
        else {
            recvCompleted = slot_->wait();
        }
    }
    catch (...) {
        exception = std::current_exception();
    }

    finishAndThrow(exception);
    return recvCompleted;
}
c10::intrusive_ptr<::c10d::Work> slimeBackend::send(std::vector<at::Tensor>& tensors, int dstRank, int tag)
{

    size_t                 batch_size = tensors.size();
    std::vector<uintptr_t> ptrs;
    std::vector<size_t>    data_size;
    std::vector<size_t>    offset;
    for (size_t i = 0; i < batch_size; ++i) {
        ptrs.push_back(reinterpret_cast<uintptr_t>(tensors[i].data_ptr()));
        offset.push_back(0);
        data_size.push_back(static_cast<size_t>(tensors[i].numel() * tensors[i].itemsize()));
    }

    auto  tensor        = tensors[0];
    void* stream_handle = nullptr;
    if (tensors[0].is_cuda()) {
#ifdef SLIME_USE_CUDA
        stream_handle = (void*)at::cuda::getCurrentCUDAStream().stream();
#endif
    }
    else if (tensor.is_cpu()) {
        stream_handle = nullptr;
    }

    auto endpoint = end_point_set_[mod_positive(dstRank - rank_, size_ - 1)];
    auto slot     = endpoint->send(ptrs[0], offset[0], data_size[0], stream_handle);

    ++seq_;
    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the recv.
    auto send_work = c10::make_intrusive<SendWork>(tensors, endpoint, slot, seq_);
    if (group_active_) {
        grouped_works_.emplace_back(send_work);
    }
    return send_work;
}

c10::intrusive_ptr<::c10d::Work> slimeBackend::recv(std::vector<at::Tensor>& tensors, int srcRank, int tag)
{
    size_t                 batch_size = tensors.size();
    std::vector<uintptr_t> ptrs;
    std::vector<size_t>    data_size;
    std::vector<size_t>    offset;
    for (size_t i = 0; i < batch_size; ++i) {
        ptrs.push_back(reinterpret_cast<uintptr_t>(tensors[i].data_ptr()));
        offset.push_back(0);
        data_size.push_back(static_cast<size_t>(tensors[i].numel() * tensors[i].itemsize()));
    }

    auto  tensor        = tensors[0];
    void* stream_handle = nullptr;
    if (tensors[0].is_cuda()) {
#ifdef SLIME_USE_CUDA
        stream_handle = (void*)at::cuda::getCurrentCUDAStream().stream();
#endif
    }
    else if (tensor.is_cpu()) {
        stream_handle = nullptr;
    }

    auto endpoint = end_point_set_[mod_positive(srcRank - rank_, size_ - 1)];
    auto slot     = endpoint->recv(ptrs[0], offset[0], data_size[0], stream_handle);
    ++seq_;

    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the send.
    auto recv_work = c10::make_intrusive<RecvWork>(tensors, endpoint, slot, seq_);
    if (group_active_) {
        grouped_works_.emplace_back(recv_work);
    }
    return recv_work;
}

slimeBackend::slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank, int size):
    Backend(rank, size), store_(store)
{

    std::vector<std::string> available_devices = available_nic();
    size_t                   idx               = rank_ % available_devices.size();

    // TODO: maybe we need a structure to transfer the RDMA device info
    const std::string dev_name  = available_devices[idx];
    const std::string link_type = "RoCE";
    uint8_t           ib_port   = 1;
    size_t            qp_num    = SLIME_QP_NUM;

    std::shared_ptr<RDMAContext> context = GlobalContextManager::instance().get_context(dev_name, ib_port, link_type);
    rdma_worker_                         = GlobalWorkerManager::instance().get_default_worker(socketId(dev_name));
    for (int i = 0; i < size - 1; ++i) {

        auto endpoint = std::make_shared<RDMAEndpoint>(context, qp_num, rdma_worker_);
        // TODO: the different end_point in the rank can use different RDMA dev to transmit the message.
        end_point_set_.push_back(endpoint);
        rdma_worker_->addEndpoint(endpoint);

        json channel_info;
        channel_info = end_point_set_[i]->endpointInfo();
        local_channel_info_.push_back(channel_info);
    }
    rdma_worker_->start();

    exchangeChannelInfo();

    try {
        for (int i = 0; i < size_ - 1; ++i) {
            json cur_channel_info = global_channel_info_[mod_positive(rank_ + i + 1, size_)][size_ - 2 - i];
            end_point_set_[i]->connect(cur_channel_info);
        }
    }
    catch (const std::runtime_error& e) {
        auto err = e.what();
        auto msg = c10::str("RDMA Endpoint connection is failed with ", err);
        logAndThrow(msg, msg);
    }
}

void slimeBackend::exchangeChannelInfo()
{
    json tx_channel_info(local_channel_info_);

    auto        str_channel_info = tx_channel_info.dump();
    std::string local_key        = "SLIME_ENDPOINT_" + std::to_string(rank_);
    store_->set(local_key, str_channel_info);

    std::vector<std::string> global_keys;
    for (size_t i = 0; i < size_; ++i) {
        global_keys.push_back("SLIME_ENDPOINT_" + std::to_string(i));
    }
    store_->wait(global_keys);

    global_channel_info_.resize(size_);
    for (size_t i = 0; i < size_; ++i) {
        auto recv_channel_info  = store_->get(global_keys[i]);
        global_channel_info_[i] = json::parse(recv_channel_info);
    }
}
c10::intrusive_ptr<::c10d::Backend> slimeBackend::createSlimeBackend(const c10::intrusive_ptr<::c10d::Store>& store,
                                                                     int                                      rank,
                                                                     int                                      size,
                                                                     const std::chrono::duration<float>&)

{
    return c10::make_intrusive<slimeBackend>(store, rank, size);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m)
{
    m.def("createSlimeBackend", &slimeBackend::createSlimeBackend);
}

}  // namespace c10d
}  // namespace slime
