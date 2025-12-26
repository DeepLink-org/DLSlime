#include "gloo_backend.h"
#include "c10/util/intrusive_ptr.h"
#include "torch/csrc/distributed/c10d/PrefixStore.hpp"

#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/extension.h>

#include "gloo/rendezvous/context.h"
#include "gloo/rendezvous/prefix_store.h"
#include "gloo/transport/ibverbs/device.h"

#include "dlslime/engine/rdma/rdma_env.h"
#include "dlslime/engine/rdma/utils.h"
#include "dlslime/logging.h"


namespace dlslime {
namespace c10d {

// static
void AsyncWork::execute(const c10::intrusive_ptr<AsyncWork>& work)
{
    if (work->recordFunctionBeforeCallback_) {
        work->recordFunctionBeforeCallback_();
    }
    try {
        at::ThreadLocalStateGuard g(work->getTLS());
        work->run();
    }
    catch (...) {
        work->finishWorkGlooError(std::current_exception());
        return;
    }

    // FIXME: We need to call it here since Future completion requires all
    // the work to be synchronized to CUDA.
    work->synchronize();
    work->finishWorkGloo();
}

std::vector<at::Tensor> AsyncWork::result()
{
    TORCH_CHECK(isCompleted(),
                "Work needs to be completed before calling result(). "
                "Should call wait() before result().");
    TORCH_CHECK(outputTensors_.size() <= 1, "work result does not support list of lists, use .getFuture() and value()");
    return outputTensors_.empty() ? std::vector<at::Tensor>() : outputTensors_.at(0);
}

c10::intrusive_ptr<c10::ivalue::Future> AsyncWork::getFuture()
{
    return future_;
}

namespace {
c10::intrusive_ptr<c10::ivalue::Future> createFutureAsOutput(const std::vector<std::vector<at::Tensor>>& outputTensors)
{
    if (outputTensors.size() > 1) {
        return c10::make_intrusive<c10::ivalue::Future>(
            c10::ListType::create(c10::ListType::create(c10::TensorType::get())));
    }
    return c10::make_intrusive<c10::ivalue::Future>(c10::ListType::create(c10::TensorType::get()));
}

void returnFutureWithOutput(c10::intrusive_ptr<c10::ivalue::Future>&    future,
                            const std::vector<std::vector<at::Tensor>>& outputTensors)
{
    if (outputTensors.empty()) {
        future->markCompleted(c10::IValue(std::vector<at::Tensor>()));
        return;
    }
    if (outputTensors.size() > 1) {
        future->markCompleted(c10::IValue(outputTensors));
        return;
    }
    future->markCompleted(c10::IValue(outputTensors[0]));
}
}  // namespace

AsyncWork::AsyncWork(std::vector<std::vector<at::Tensor>> outputTensors, ::c10d::OpType opType, uint64_t seq)
    // Profiler: Pass nullptr as profilingTitle to parent constructor to
    // replace default profiler implementation with async version that reports
    // correct timestamps for work that is asynchronously executed.
    :
    Work(-1, opType), outputTensors_(std::move(outputTensors)), future_(createFutureAsOutput(outputTensors_)), seq_(seq)
{
    // if (profilingTitle != nullptr) {
    //     recordAsyncWorkProfilingInfo(profilingTitle, inputTensors);
    // }
}

void AsyncWork::finishWorkGlooError(const std::exception_ptr& eptr)
{
    future_->setError(eptr);
    finish(eptr);
}

void AsyncWork::finishWorkGloo()
{
    returnFutureWithOutput(future_, outputTensors_);
    finish();
}

SendWork::SendWork(at::Tensor& tensor, std::unique_ptr<::gloo::transport::UnboundBuffer> buffer, uint64_t seq):
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
            sendCompleted = buffer_->waitSend(timeout);
        }
    }
    catch (...) {
        exception = std::current_exception();
    }

    // Completes the Work object and throws the exception.
    finishAndThrow(exception);
    return sendCompleted;
}

void SendWork::abort()
{
    buffer_->abortWaitSend();
}

RecvWork::RecvWork(at::Tensor&                                       tensor,
                   std::unique_ptr<::gloo::transport::UnboundBuffer> buffer,
                   ::c10d::OpType                                    opType,
                   uint64_t                                          seq):
    Work(-1, opType), tensor_(tensor), buffer_(std::move(buffer)), srcRank_(-1), seq_(seq)
{
}

int RecvWork::sourceRank() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return srcRank_;
}

bool RecvWork::wait(std::chrono::milliseconds timeout)
{
    bool               recvCompleted = false;
    std::exception_ptr exception{nullptr};
    try {
        if (timeout == kNoTimeout) {
            recvCompleted = buffer_->waitRecv(&srcRank_);
        }
        else {
            recvCompleted = buffer_->waitRecv(&srcRank_, timeout);
        }
    }
    catch (...) {
        exception = std::current_exception();
    }

    // Completes the Work object and throws the exception.
    finishAndThrow(exception);
    return recvCompleted;
}

void RecvWork::abort()
{
    buffer_->abortWaitRecv();
}

void logAndThrow(const std::string& logMessage, const std::string& errorMessage)
{
    LOG(ERROR) << logMessage;
    TORCH_CHECK(false, errorMessage);
}

// If necessary, pass store/rank/size to the ctor and exchange connection
// information here
slimeBackend::slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank, int size):
    Backend(rank, size), store_(new GlooStore(store)), stop_(false), collectiveCounter_(0)
{
    // auto& devices = "cuda";
    // if (devices.empty()) {
    //     TORCH_CHECK(false, "No device(s) specified");
    // }

    // Create and connect a context for every device.
    //
    // Note that the same device can be specified multiple times, either
    // the same object, or the same logical device as different objects.
    // Either mode is fine and only has performance implications.
    //
    // Using the same object multiple times means all contexts share a
    // single I/O thread. If you use different objects for the same
    // logical device they will have independent I/O threads. The latter
    // option is needed if you have a fast NIC that cannot be saturated
    // by a single I/O thread.
    //
    // contexts_.reserve(options_->devices.size());
    // for (const auto i : c10::irange(options_->devices.size())) {
    auto context = std::make_shared<::gloo::rendezvous::Context>(rank_, size_);

#ifdef GLOO_SHARED_STORE
    auto underlyingStore = store_;
#else
    auto& underlyingStore = *store_;
#endif

    auto prefix_store = std::make_shared<::gloo::rendezvous::PrefixStore>("0", underlyingStore);

#ifdef GLOO_SHARED_STORE
    auto connectStore = prefix_store;
#else
    auto& connectStore    = *prefix_store;
#endif

    // context->setTimeout(options_->timeout);
    try {
        ::gloo::transport::ibverbs::attr attr;

        std::vector<std::string> available_devices = available_nic();
        size_t                   idx               = rank_ % available_devices.size();
        attr.name                                  = available_devices[idx];
        attr.port                                  = 1;

        attr.index = get_gid_index(available_devices[idx]);
        SLIME_LOG_INFO("rank: " << rank_ << ", idx: " << idx << ", device: " << available_devices[idx]
                                << ", gidx: " << attr.index << ".");

        auto dev = gloo::transport::ibverbs::CreateDevice(attr);
        context->connectFullMesh(connectStore, dev);
    }
    catch (const std::runtime_error& e) {
        auto err = e.what();
        // TORCH_CHECK to print the cpp stacktrace.
        auto msg = c10::str("Gloo connectFullMesh failed with ", err);
        logAndThrow(msg, msg);
    }
    contexts_.push_back(std::move(context));
    // }

    // Every worker thread stores the AsyncWork object it's currently
    // working on in the workInProgress_ vector. It must have size equal
    // to the number of workers such that they can simply index into it
    // using the worker index they are started with.
    workInProgress_.resize(32);

    threads_.resize(32);
    for (const auto i : c10::irange(threads_.size())) {
        threads_[i] = std::thread(&slimeBackend::runLoop, this, i);
    }
}

slimeBackend::~slimeBackend()
{
    std::unique_lock<std::mutex> lock(workMutex_);
    workConsumeCV_.wait(lock, [&] { return workQueue_.empty(); });

    // Queue is empty, signal stop
    stop_ = true;

    // Release lock to allow threads to terminate
    lock.unlock();

    workProduceCV_.notify_all();

    // Wait for worker threads to terminate
    for (auto& thread : threads_) {
        thread.join();
    }
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

c10::intrusive_ptr<::c10d::Work> slimeBackend::send(std::vector<at::Tensor>& tensors, int dstRank, int tag)
{
    auto& tensor = checkSingleTensor(tensors);
    auto  utag   = checkTag(tag);
    auto  ptr    = tensor.const_data_ptr();
    auto  size   = tensor.numel() * tensor.element_size();

    // Construct unbound buffer.
    auto context = getContext(tag);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    auto buf = context->createUnboundBuffer(const_cast<void*>(ptr), size);
    buf->send(dstRank, utag);
    ++seq_;

    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the send.
    auto send_work = c10::make_intrusive<SendWork>(tensor, std::move(buf), seq_);
    if (group_active_) {
        grouped_works_.emplace_back(send_work);
    }
    return send_work;
}

c10::intrusive_ptr<::c10d::Work> slimeBackend::recv(std::vector<at::Tensor>& tensors, int srcRank, int tag)
{
    auto& tensor = checkSingleTensor(tensors);
    auto  utag   = checkTag(tag);
    auto  ptr    = tensor.mutable_data_ptr();
    auto  size   = tensor.numel() * tensor.element_size();

    // Construct unbound buffer.
    auto context = getContext(tag);
    auto buf     = context->createUnboundBuffer(ptr, size);
    buf->recv(srcRank, utag);
    ++seq_;

    // The work captures the tensor to prevent it being deallocated and
    // the unbound buffer to synchronize on completion of the recv.
    auto recv_work = c10::make_intrusive<RecvWork>(tensor, std::move(buf), ::c10d::OpType::RECV, seq_);
    if (group_active_) {
        grouped_works_.emplace_back(recv_work);
    }
    return recv_work;
}

void slimeBackend::runLoop(int workerIndex)
{
    std::unique_lock<std::mutex> lock(workMutex_);

    while (!stop_) {
        if (workQueue_.empty()) {
            workProduceCV_.wait(lock);
            continue;
        }

        auto work = std::move(workQueue_.front());
        workQueue_.pop_front();
        workInProgress_[workerIndex] = work;
        lock.unlock();

        // Notify after releasing the lock so that the waiter
        // does not immediately block.
        workConsumeCV_.notify_one();

        AsyncWork::execute(work);
        lock.lock();
        workInProgress_[workerIndex].reset();
    }
}

c10::intrusive_ptr<::c10d::Backend> slimeBackend::createDLSlimeBackend(c10::intrusive_ptr<::c10d::Store> store,
                                                                       int64_t                           rank,
                                                                       int64_t                           size,
                                                                       const std::chrono::duration<float>& /* unused */)
{
    return c10::make_intrusive<slimeBackend>(store, rank, size);
}

PYBIND11_MODULE(TORCH_EXTENSION_NAME, m)
{
    m.def("createDLSlimeBackend", &slimeBackend::createDLSlimeBackend);
}
}  // namespace c10d
}  // namespace dlslime
