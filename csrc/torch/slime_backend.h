#pragma once

#include <torch/python.h>

#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Utils.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>

#include <pybind11/chrono.h>

#include "engine/rdma/rdma_endpoint.h"
#include "utils/utils.h"

namespace slime {
namespace c10d {

class TORCH_API SendWork: public ::c10d::Work {
    friend class slimeBackend;

public:
    explicit SendWork(std::vector<at::Tensor>& tensor, std::unique_ptr<::slime::RDMABuffer> buffer, uint64_t seq);
    bool wait(std::chrono::milliseconds timeout = kNoTimeout) override;
    void abort() override
    {
        throw std::runtime_error("not supported");
    }

protected:
    std::vector<at::Tensor>              tensor_;
    std::unique_ptr<::slime::RDMABuffer> buffer_;
    int                                  dstRank_;
    const uint64_t                       seq_;
};

class TORCH_API RecvWork: public ::c10d::Work {
    friend class slimeBackend;

public:
    explicit RecvWork(std::vector<at::Tensor>& tensor, std::unique_ptr<::slime::RDMABuffer> buffer, uint64_t seq);
    bool wait(std::chrono::milliseconds timeout = kNoTimeout) override;
    void abort() override
    {
        throw std::runtime_error("not supported");
    }

protected:
    std::vector<at::Tensor>              tensor_;
    std::unique_ptr<::slime::RDMABuffer> buffer_;
    int                                  srcRank_;
    const uint64_t                       seq_;
};

// Backend:
class TORCH_API slimeBackend: public ::c10d::Backend {

public:
    slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank = -1, int size = -1);

    virtual ~slimeBackend() override = default;

    c10::intrusive_ptr<::c10d::Work> send(std::vector<at::Tensor>& tensors, int dstRank, int tag) override;
    c10::intrusive_ptr<::c10d::Work> recv(std::vector<at::Tensor>& tensors, int srcRank, int tag) override;
    c10::intrusive_ptr<::c10d::Work> recvAnysource(std::vector<at::Tensor>& tensors, int tag) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    broadcast(std::vector<at::Tensor>& data, const ::c10d::BroadcastOptions& opts = ::c10d::BroadcastOptions()) override
    {
        throw std::runtime_error("not supported");
    }

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
    }

    c10::intrusive_ptr<::c10d::Work> reduce(std::vector<at::Tensor>&     tensors,
                                            const ::c10d::ReduceOptions& opts = ::c10d::ReduceOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    allgather(std::vector<std::vector<at::Tensor>>& outputTensors,
              std::vector<at::Tensor>&              inputTensors,
              const ::c10d::AllgatherOptions&       opts = ::c10d::AllgatherOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    _allgather_base(at::Tensor&                     outputBuffer,
                    at::Tensor&                     inputBuffer,
                    const ::c10d::AllgatherOptions& opts = ::c10d::AllgatherOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work> barrier(const ::c10d::BarrierOptions& opts = ::c10d::BarrierOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work> gather(std::vector<std::vector<at::Tensor>>& outputTensors,
                                            std::vector<at::Tensor>&              inputTensors,
                                            const ::c10d::GatherOptions& opts = ::c10d::GatherOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work> scatter(std::vector<at::Tensor>&              outputTensors,
                                             std::vector<std::vector<at::Tensor>>& inputTensors,
                                             const ::c10d::ScatterOptions& opts = ::c10d::ScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    reduce_scatter(std::vector<at::Tensor>&              outputTensors,
                   std::vector<std::vector<at::Tensor>>& inputTensors,
                   const ::c10d::ReduceScatterOptions&   opts = ::c10d::ReduceScatterOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work>
    alltoall_base(at::Tensor&                    outputTensor,
                  at::Tensor&                    inputTensor,
                  std::vector<int64_t>&          outputSplitSizes,
                  std::vector<int64_t>&          inputSplitSizes,
                  const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    c10::intrusive_ptr<::c10d::Work> alltoall(std::vector<at::Tensor>&       outputTensors,
                                              std::vector<at::Tensor>&       inputTensors,
                                              const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override
    {
        throw std::runtime_error("not supported");
    }

    static c10::intrusive_ptr<::c10d::Backend> createSlimeBackend(const c10::intrusive_ptr<::c10d::Store>& store,
                                                                  int                                      rank,
                                                                  int                                      size,
                                                                  const std::chrono::duration<float>&);

    static void slimeBackendConstructor() __attribute__((constructor))
    {
        py::object module          = py::module::import("torch.distributed");
        py::object registerBackend = module.attr("Backend").attr("register_backend");
        registerBackend(
            "dlslime", py::cpp_function(createSlimeBackend), false, py::arg("devices") = py::make_tuple("cuda", "cpu"));
    }

private:
    void                                                        exchangeChannelInfo();
    c10::intrusive_ptr<::c10d::Store>                           store_;
    std::unordered_map<uint32_t, std::shared_ptr<RDMAEndpoint>> end_point_set_;
    json                                                        channel_info_;
    std::vector<json>                                           peers_channel_info_;
    uint64_t                                                    seq_{0};
};

}  // namespace c10d
}  // namespace slime
