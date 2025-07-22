#pragma once

#include <torch/python.h>

#include <torch/csrc/distributed/c10d/Backend.hpp>
#include <torch/csrc/distributed/c10d/Store.hpp>
#include <torch/csrc/distributed/c10d/Types.hpp>
#include <torch/csrc/distributed/c10d/Utils.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>

// Backend:
class slimeBackend: public ::c10d::Backend {
public:
    slimeBackend(const c10::intrusive_ptr<::c10d::Store>& store, int rank = -1, int size = -1);

    ~slimeBackend() override;

    c10::intrusive_ptr<::c10d::Work> send(std::vector<at::Tensor>& tensors, int dstRank, int tag) override;
    c10::intrusive_ptr<::c10d::Work> recv(std::vector<at::Tensor>& tensors, int srcRank, int tag) override;
    c10::intrusive_ptr<::c10d::Work> recvAnysource(std::vector<at::Tensor>& tensors, int tag) override;

    c10::intrusive_ptr<::c10d::Work>
    broadcast(std::vector<at::Tensor>&        data,
              const ::c10d::BroadcastOptions& opts = ::c10d::BroadcastOptions()) override;

    c10::intrusive_ptr<::c10d::Work>
    allreduce(std::vector<at::Tensor>&        tensors,
              const ::c10d::AllreduceOptions& opts = ::c10d::AllreduceOptions()) override;
    c10::intrusive_ptr<::c10d::Work>
    allreduce_coalesced(std::vector<at::Tensor>&                 tensors,
                        const ::c10d::AllreduceCoalescedOptions& opts = ::c10d::AllreduceCoalescedOptions()) override;

    c10::intrusive_ptr<::c10d::Work> reduce(std::vector<at::Tensor>&     tensors,
                                            const ::c10d::ReduceOptions& opts = ::c10d::ReduceOptions()) override;

    c10::intrusive_ptr<::c10d::Work>
    allgather(std::vector<std::vector<at::Tensor>>& outputTensors,
              std::vector<at::Tensor>&              inputTensors,
              const ::c10d::AllgatherOptions&       opts = ::c10d::AllgatherOptions()) override;

    c10::intrusive_ptr<::c10d::Work>
    _allgather_base(at::Tensor&                     outputBuffer,
                    at::Tensor&                     inputBuffer,
                    const ::c10d::AllgatherOptions& opts = ::c10d::AllgatherOptions()) override;

    c10::intrusive_ptr<::c10d::Work> barrier(const ::c10d::BarrierOptions& opts = ::c10d::BarrierOptions()) override;

    c10::intrusive_ptr<::c10d::Work> gather(std::vector<std::vector<at::Tensor>>& outputTensors,
                                            std::vector<at::Tensor>&              inputTensors,
                                            const ::c10d::GatherOptions& opts = ::c10d::GatherOptions()) override;

    c10::intrusive_ptr<::c10d::Work> scatter(std::vector<at::Tensor>&              outputTensors,
                                             std::vector<std::vector<at::Tensor>>& inputTensors,
                                             const ::c10d::ScatterOptions& opts = ::c10d::ScatterOptions()) override;

    c10::intrusive_ptr<::c10d::Work>
    reduce_scatter(std::vector<at::Tensor>&              outputTensors,
                   std::vector<std::vector<at::Tensor>>& inputTensors,
                   const ::c10d::ReduceScatterOptions&   opts = ::c10d::ReduceScatterOptions()) override;

    c10::intrusive_ptr<::c10d::Work>
    alltoall_base(at::Tensor&                    outputTensor,
                  at::Tensor&                    inputTensor,
                  std::vector<int64_t>&          outputSplitSizes,
                  std::vector<int64_t>&          inputSplitSizes,
                  const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override;

    c10::intrusive_ptr<::c10d::Work> alltoall(std::vector<at::Tensor>&       outputTensors,
                                              std::vector<at::Tensor>&       inputTensors,
                                              const ::c10d::AllToAllOptions& opts = ::c10d::AllToAllOptions()) override;

    static c10::intrusive_ptr<Backend> createSlimeBackend(const c10::intrusive_ptr<::c10d::Store>& store,
                                                          int                                      rank,
                                                          int                                      size,
                                                          const std::chrono::duration<float>&      timeout);

    static void slimeBackendConstructor() __attribute__((constructor))
    {
        py::object module           = py::module::import("torch.distributed");
        py::object register_backend = module.attr("Backend").attr("register_backend");
        register_backend("dlslime", py::cpp_function(createSlimeBackend));
    }
};

class WorkSlime: public c10d::Work {
    friend class slimeBackend;
};
