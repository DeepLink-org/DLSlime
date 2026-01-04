/*************************************************************************
 * Copyright (c) 2025 by MetaX Integrated Circuits (Shanghai) Co., Ltd. All
 *Rights Reserved. Copyright (c) 2025 by DU. All Rights Reserved.
 ************************************************************************/
#pragma once

#include <pybind11/chrono.h>
#include <torch/extension.h>

#include <memory>
#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Utils.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <unordered_map>
#include <vector>

#include "c10/util/intrusive_ptr.h"
#include "dlslime/logging.h"
#include "gloo/context.h"
#include "gloo/rendezvous/store.h"
#include "gloo/transport/device.h"
#include "gloo/transport/unbound_buffer.h"

namespace dlslime {
namespace c10d {

constexpr const char* SLIME_BACKEND_NAME = "dlslime";

class TORCH_API AsyncWork: public ::c10d::Work {
public:
    explicit AsyncWork(std::vector<std::vector<at::Tensor>> outputTensors, ::c10d::OpType opType, uint64_t seq);

    ~AsyncWork() override = default;

    static void execute(const c10::intrusive_ptr<AsyncWork>& work);

    virtual void run() = 0;

    std::vector<at::Tensor> result() override;

    c10::intrusive_ptr<c10::ivalue::Future> getFuture() override;

    inline at::ThreadLocalState getTLS() const
    {
        return tls_;
    }

protected:
    friend class ProcessGroupGloo;

private:
    void finishWorkGloo();
    void finishWorkGlooError(const std::exception_ptr& eptr);

    const std::vector<std::vector<at::Tensor>> outputTensors_;
    c10::intrusive_ptr<at::ivalue::Future>     future_;
    std::function<void()>                      recordFunctionBeforeCallback_;
    const uint64_t                             seq_;
    at::ThreadLocalState                       tls_;
};

class TORCH_API SendWork: public ::c10d::Work {
    friend class slimeBackend;

public:
    explicit SendWork(at::Tensor& tensor, std::unique_ptr<::gloo::transport::UnboundBuffer> buffer, uint64_t seq);

    bool wait(std::chrono::milliseconds timeout = kNoTimeout) override;

    void abort() override;

protected:
    at::Tensor                                        tensor_;
    std::unique_ptr<::gloo::transport::UnboundBuffer> buffer_;
    const uint64_t                                    seq_;
};

struct TORCH_API Options: public ::c10d::Backend::Options {
    explicit Options(std::chrono::milliseconds timeout = kBackendDefaultTimeout);

    // return intrusive_ptr of the object
    static c10::intrusive_ptr<Options> create(std::chrono::milliseconds timeout = kBackendDefaultTimeout)
    {
        return c10::make_intrusive<Options>(timeout);
    }

    std::vector<std::shared_ptr<::gloo::transport::Device>> devices;
    int                                                     threads;
};

class TORCH_API RecvWork: public ::c10d::Work {
public:
    explicit RecvWork(at::Tensor&                                       tensor,
                      std::unique_ptr<::gloo::transport::UnboundBuffer> buffer,
                      ::c10d::OpType                                    opType,
                      uint64_t                                          seq);

    int sourceRank() const override;

    bool wait(std::chrono::milliseconds timeout = kNoTimeout) override;

    void abort() override;

protected:
    at::Tensor                                        tensor_;
    std::unique_ptr<::gloo::transport::UnboundBuffer> buffer_;
    int                                               srcRank_;
    const uint64_t                                    seq_;
};

class TORCH_API GlooStore: public ::gloo::rendezvous::Store {
    friend class slimeBackend;

public:
    GlooStore(c10::intrusive_ptr<::c10d::Store> store): store_(std::move(store)) {}

    void setUint(const std::string& key, const std::vector<uint8_t>& value)
    {
        store_->set(key, value);
    }

    void set(const std::string& key, const std::vector<char>& value) override
    {
        std::vector<uint8_t> tmp(value.begin(), value.end());
        store_->set(key, tmp);
    }

    std::vector<uint8_t> getUint(const std::string& key)
    {
        auto value = store_->get(key);
        return value;
    }

    std::vector<char> get(const std::string& key) override
    {
        auto value = store_->get(key);
        return std::vector<char>(value.begin(), value.end());
    }

    void wait(const std::vector<std::string>& keys) override
    {
        store_->wait(keys, ::c10d::Store::kDefaultTimeout);
    }

    void wait(const std::vector<std::string>& keys, const std::chrono::milliseconds& timeout) override
    {
        store_->wait(keys, timeout);
    }

#ifdef GLOO_STORE_HAS_STORE_V2
    bool has_v2_support() override
    {
        return store_->hasExtendedApi();
    }

    std::vector<std::vector<char>> multi_get(const std::vector<std::string>& keys) override
    {
        std::vector<std::vector<char>> res;
        for (auto& value : store_->multiGet(keys)) {
            res.emplace_back(value.begin(), value.end());
        }
        return res;
    }

    void multi_set(const std::vector<std::string>& keys, const std::vector<std::vector<char>>& values) override
    {
        std::vector<std::vector<uint8_t>> u_values;
        u_values.reserve(values.size());
        for (auto& value : values) {
            u_values.emplace_back(value.begin(), value.end());
        }
        store_->multiSet(keys, u_values);
    }

    void append(const std::string& key, const std::vector<char>& value) override
    {
        std::vector<uint8_t> tmp(value.begin(), value.end());
        return store_->append(key, tmp);
    }

    int64_t add(const std::string& key, int64_t value) override
    {
        return store_->add(key, value);
    }
#endif

protected:
    c10::intrusive_ptr<::c10d::Store> store_;
};

class GroupWork: public ::c10d::Work {
public:
    GroupWork(std::vector<c10::intrusive_ptr<::c10d::Work>>& grouped_works): grouped_works_(std::move(grouped_works)) {}
    bool wait(std::chrono::milliseconds timeout = kNoTimeout) override
    {
        for (size_t i = 0; i < grouped_works_.size(); ++i)
            grouped_works_[i]->wait(timeout);
        return true;
    }

protected:
    std::vector<c10::intrusive_ptr<::c10d::Work>> grouped_works_;
};

class slimeBackend: public ::c10d::Backend {
public:
    explicit slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank = -1, int size = -1);

    ~slimeBackend() override;

    const std::string getBackendName() const override
    {
        return std::string(SLIME_BACKEND_NAME);
    }

    void startCoalescing() override
    {
        group_active_ = true;
    }

    c10::intrusive_ptr<::c10d::Work> endCoalescing() override
    {
        group_active_   = false;
        auto group_work = c10::make_intrusive<GroupWork>(grouped_works_);
        return group_work;
    }

    c10::intrusive_ptr<::c10d::Work> send(std::vector<at::Tensor>& tensors, int dstRank, int tag) override;

    c10::intrusive_ptr<::c10d::Work> recv(std::vector<at::Tensor>& tensors, int srcRank, int tag) override;

    c10::intrusive_ptr<::c10d::Work>
    broadcast(std::vector<at::Tensor>& data, const ::c10d::BroadcastOptions& opts = ::c10d::BroadcastOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    allreduce(std::vector<at::Tensor>&        tensors,
              const ::c10d::AllreduceOptions& opts = ::c10d::AllreduceOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    allreduce_coalesced(std::vector<at::Tensor>&                 tensors,
                        const ::c10d::AllreduceCoalescedOptions& opts = ::c10d::AllreduceCoalescedOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> reduce(std::vector<at::Tensor>&     tensors,
                                            const ::c10d::ReduceOptions& opts = ::c10d::ReduceOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    allgather(std::vector<std::vector<at::Tensor>>& outputTensors,
              std::vector<at::Tensor>&              inputTensors,
              const ::c10d::AllgatherOptions&       opts = ::c10d::AllgatherOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    _allgather_base(at::Tensor&                     outputBuffer,
                    at::Tensor&                     inputBuffer,
                    const ::c10d::AllgatherOptions& opts = ::c10d::AllgatherOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    allgather_into_tensor_coalesced(std::vector<at::Tensor>&        outputs,
                                    std::vector<at::Tensor>&        inputs,
                                    const ::c10d::AllgatherOptions& opts = ::c10d::AllgatherOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> barrier(const ::c10d::BarrierOptions& opts = ::c10d::BarrierOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> gather(std::vector<std::vector<at::Tensor>>& outputTensors,
                                            std::vector<at::Tensor>&              inputTensors,
                                            const ::c10d::GatherOptions& opts = ::c10d::GatherOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> scatter(std::vector<at::Tensor>&              outputTensors,
                                             std::vector<std::vector<at::Tensor>>& inputTensors,
                                             const ::c10d::ScatterOptions& opts = ::c10d::ScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    reduce_scatter(std::vector<at::Tensor>&              outputTensors,
                   std::vector<std::vector<at::Tensor>>& inputTensors,
                   const ::c10d::ReduceScatterOptions&   opts = ::c10d::ReduceScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    _reduce_scatter_base(at::Tensor&                         outputTensor,
                         at::Tensor&                         inputTensor,
                         const ::c10d::ReduceScatterOptions& opts = ::c10d::ReduceScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    reduce_scatter_tensor_coalesced(std::vector<at::Tensor>&            outputs,
                                    std::vector<at::Tensor>&            inputs,
                                    const ::c10d::ReduceScatterOptions& opts = ::c10d::ReduceScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work>
    alltoall_base(at::Tensor&                    outputTensor,
                  at::Tensor&                    inputTensor,
                  std::vector<int64_t>&          outputSplitSizes,
                  std::vector<int64_t>&          inputSplitSizes,
                  const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> alltoall(std::vector<at::Tensor>&       outputTensors,
                                              std::vector<at::Tensor>&       inputTensors,
                                              const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override
    {
        throw std::runtime_error("not supported");
    };

    c10::intrusive_ptr<::c10d::Work> recvAnysource(std::vector<at::Tensor>& tensors, int tag) override
    {
        throw std::runtime_error("not supported");
    };

    static c10::intrusive_ptr<Backend> createDLSlimeBackend(c10::intrusive_ptr<::c10d::Store>   store,
                                                            int64_t                             rank,
                                                            int64_t                             size,
                                                            const std::chrono::duration<float>& timeout);

    static void dlslimeBackendConstructor() __attribute__((constructor))
    {
        py::object module          = py::module::import("torch.distributed");
        py::object registerBackend = module.attr("Backend").attr("register_backend");
        registerBackend("dlslime",
                        py::cpp_function(createDLSlimeBackend),
                        false,
                        py::arg("devices") = py::make_tuple("cuda", "cpu"));
    }

protected:
    // Every Gloo context represents a set of connections to its peers.
    // In order to use more than one device (or allow for parallelism on
    // a single device), you need multiple contexts.
    std::vector<std::shared_ptr<::gloo::Context>> contexts_;
    std::vector<std::thread>                      threads_;
    bool                                          stop_;

    void initComm(at::Device dev) {};
    void initComm() {};
    void syncStream(at::Device device, int index = 0) {};
    void groupStart() {};
    void groupEnd() {};
    // Entrypoint for worker threads.
    void runLoop(int workerIndex);

    std::shared_ptr<::gloo::Context> getContext(uint32_t tag)
    {
        return contexts_[tag % contexts_.size()];
    }

    std::shared_ptr<::gloo::rendezvous::Store> store_;
    int                                        nDevs_;
    int                                        deviceId_;
    int                                        status_;  // 0: allocated, 1: initialized
    uint64_t                                   activeGroupCounter_;
    uint32_t                                   collectiveCounter_;
    std::deque<c10::intrusive_ptr<AsyncWork>>  workQueue_;
    std::vector<c10::intrusive_ptr<AsyncWork>> workInProgress_;
    std::mutex                                 workMutex_;
    std::condition_variable                    workProduceCV_;
    std::condition_variable                    workConsumeCV_;
    uint64_t                                   seq_{0};

    bool                                          group_active_{false};
    std::vector<c10::intrusive_ptr<::c10d::Work>> grouped_works_;

private:
    template<typename Fn>
    c10::intrusive_ptr<::c10d::Work>
    collectiveCoalesced(std::vector<at::Tensor>& input, std::vector<at::Tensor>& output, Fn fn, ::c10d::OpType opType);
};

}  // namespace c10d
}  // namespace dlslime
