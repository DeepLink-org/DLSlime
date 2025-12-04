#include "slime_backend.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint_v0.h"
#include "engine/rdma/rdma_env.h"
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

SendWork::SendWork(std::vector<at::Tensor>& tensor, std::shared_ptr<::slime::RDMABuffer> buffer, uint64_t seq):
    Work(-1, ::c10d::OpType::SEND), tensor_(tensor), buffer_(std::move(buffer)), seq_(seq)
{
}

bool SendWork::wait(std::chrono::milliseconds timeout)
{
    bool               sendCompleted = false;
    std::exception_ptr exception{nullptr};
    try {
        if (timeout == kNoTimeout) {
            sendCompleted = buffer_->waitSend();
        }
        else {
            sendCompleted = buffer_->waitSend();
        }
    }
    catch (...) {
        exception = std::current_exception();
    }

    finishAndThrow(exception);
    return sendCompleted;
}

RecvWork::RecvWork(std::vector<at::Tensor>& tensor, std::shared_ptr<::slime::RDMABuffer> buffer, uint64_t seq):
    Work(-1, ::c10d::OpType::RECV), tensor_(tensor), buffer_(std::move(buffer)), srcRank_(-1), seq_(seq)
{
}

bool RecvWork::wait(std::chrono::milliseconds timeout)
{
    bool               recvCompleted = false;
    std::exception_ptr exception{nullptr};
    try {
        if (timeout == kNoTimeout) {
            recvCompleted = buffer_->waitRecv();
        }
        else {
            recvCompleted = buffer_->waitRecv();
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

    auto buf = std::make_shared<RDMABuffer>(
        end_point_set_[mod_positive(dstRank - rank_, size_ - 1)], ptrs[0], offset[0], data_size[0]);
    buf->send();

    ++seq_;
    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the recv.
    auto send_work = c10::make_intrusive<SendWork>(tensors, std::move(buf), seq_);
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
    auto buf = std::make_shared<RDMABuffer>(
        end_point_set_[mod_positive(srcRank - rank_, size_ - 1)], ptrs[0], offset[0], data_size[0]);
    buf->recv();
    ++seq_;

    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the send.
    auto recv_work = c10::make_intrusive<RecvWork>(tensors, std::move(buf), seq_);
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

    for (int i = 0; i < size - 1; ++i) {

        // TODO: the different end_point in the rank can use different RDMA dev to transmit the message.
        end_point_set_.push_back(std::make_shared<RDMAEndpointV0>(dev_name, ib_port, link_type, qp_num));

        json channel_info;
        channel_info["data_channel"] = end_point_set_[i]->dataCtxInfo();
        channel_info["meta_channel"] = end_point_set_[i]->metaCtxInfo();
        local_channel_info_.push_back(channel_info);
    }

    exchangeChannelInfo();

    try {
        for (int i = 0; i < size_ - 1; ++i) {
            json cur_channel_info = global_channel_info_[mod_positive(rank_ + i + 1, size_)][size_ - 2 - i];
            end_point_set_[i]->connect(cur_channel_info["data_channel"], cur_channel_info["meta_channel"]);
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
