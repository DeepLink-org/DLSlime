#include "slime_backend.h"

namespace slime {
namespace c10d {

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

SendWork::SendWork(std::vector<at::Tensor>& tensor, std::unique_ptr<::slime::RDMABuffer> buffer, uint64_t seq):
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

RecvWork::RecvWork(std::vector<at::Tensor>& tensor, std::unique_ptr<::slime::RDMABuffer> buffer, uint64_t seq):
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
        data_size.push_back(static_cast<size_t>(tensors[i].numel()));
        offset.push_back(0);
    }

    auto it = end_point_set_.find(tag);

    if (it != end_point_set_.end()) {
        auto end_point = it->second;
        auto buf       = std::make_unique<RDMABuffer>(end_point, ptrs, offset, data_size);
        buf->send();
        ++seq_;
        return c10::make_intrusive<SendWork>(tensors, std::move(buf), seq_);
    }
    else {
        std::cout << "Endpoint not found in send!" << std::endl;
        throw std::runtime_error("there are some bugs in backend in send function");
    }
}

c10::intrusive_ptr<::c10d::Work> slimeBackend::recv(std::vector<at::Tensor>& tensors, int srcRank, int tag)
{
    size_t                 batch_size = tensors.size();
    std::vector<uintptr_t> ptrs;
    std::vector<size_t>    data_size;
    std::vector<size_t>    offset;
    for (size_t i = 0; i < batch_size; ++i) {
        ptrs.push_back(reinterpret_cast<uintptr_t>(tensors[i].data_ptr()));
        data_size.push_back(static_cast<size_t>(tensors[i].numel()));
        offset.push_back(0);
    }
    auto it = end_point_set_.find(tag);
    if (it != end_point_set_.end()) {
        auto end_point = it->second;
        auto buf       = std::make_unique<RDMABuffer>(end_point, ptrs, offset, data_size);
        buf->recv();
        ++seq_;
        return c10::make_intrusive<RecvWork>(tensors, std::move(buf), seq_);
    }
    else {
        std::cout << "Endpoint not found in recv!" << std::endl;
        throw std::runtime_error("there are some bugs in backend in send function");
    }
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
        auto msg = c10::str("RDMA Endpoint connection is failed with ", err);
        logAndThrow(msg, msg);
    }

    end_point_set_[rank_] = std::move(end_point);
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
