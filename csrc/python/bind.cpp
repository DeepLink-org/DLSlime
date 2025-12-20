#include <cstdint>
#include <functional>
#include <memory>

#include <pybind11/cast.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "engine/assignment.h"
#include "engine/dlpack.h"

#ifdef BUILD_NVLINK
#include "engine/nvlink/memory_pool.h"
#include "engine/nvlink/nvlink_transport.h"
#endif

#ifdef BUILD_NVSHMEM
#include "engine/nvshmem/nvshmem_context.h"
#endif

#ifdef BUILD_RDMA
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_utils.h"
#endif

#if defined(BUILD_INTRA_OPS) || defined(BUILD_INTER_OPS)
#include <torch/torch.h>

#ifdef BUILD_INTRA_OPS
#include "ops/intra_ll/all_to_all/all_to_all_intra_ll_buffer.h"
#endif

#ifdef BUILD_INTER_OPS
#include "ops/inter_ll/all_gather_inter_ll/all_gather_inter_ll_buffer.h"
#endif

#endif

#include "json.hpp"
#include "logging.h"
#include "pybind_json/pybind_json.hpp"

using json = nlohmann::json;

namespace py = pybind11;

#ifdef BUILD_NVSHMEM

namespace slime {

py::object alloc_dlpack_tensor(slime::NVShmemContext& self, size_t size, size_t alignment)
{
    void*            ptr        = self.allocBuffer(size, alignment);
    DLManagedTensor* dlm_tensor = new DLManagedTensor();
    DLTensor&        tensor     = dlm_tensor->dl_tensor;
    tensor.data                 = ptr;
    tensor.device               = DLDevice{.device_type = DLDeviceType::kDLCUDA, .device_id = self.gpu_device_id()};
    tensor.dtype                = DLDataType{.code = 0, .bits = 8, .lanes = 1};
    tensor.ndim                 = static_cast<int>(1);
    long    aligned_size        = (size + alignment - 1) / alignment * alignment;
    int64_t shape[]             = {aligned_size};
    int64_t strides[]           = {1};
    tensor.shape                = shape;
    tensor.strides              = strides;
    tensor.byte_offset          = 0;
    dlm_tensor->manager_ctx     = nullptr;
    dlm_tensor->deleter         = nullptr;

    py::capsule capsule(dlm_tensor, "dltensor", [](PyObject* obj) {
        if (PyCapsule_IsValid(obj, "dltensor")) {
            DLManagedTensor* mt = static_cast<DLManagedTensor*>(PyCapsule_GetPointer(obj, "dltensor"));
            if (mt && mt->deleter) {
                mt->deleter(mt);
            }
        }
    });

    return capsule;
}

}  // namespace slime

#endif

#ifdef BUILD_RDMA
#define BUILD_RDMA_ENABLED true
#else
#define BUILD_RDMA_ENABLED false
#endif

#ifdef BUILD_NVSHMEM
#define BUILD_NVSHMEM_ENABLED true
#else
#define BUILD_NVSHMEM_ENABLED false
#endif

#ifdef BUILD_NVLINK
#define BUILD_NVLINK_ENABLED true
#else
#define BUILD_NVLINK_ENABLED false
#endif

#ifdef BUILD_INTRA_OPS
#define BUILD_INTRA_OPS_ENABLED true
#else
#define BUILD_INTRA_OPS_ENABLED false
#endif

#ifdef BUILD_INTER_OPS
#define BUILD_INTER_OPS_ENABLED true
#else
#define BUILD_INTER_OPS_ENABLED false
#endif

#define EXPOSE_BUILD_FLAG(m, flag) m.attr("_" #flag) = flag##_ENABLED

PYBIND11_MODULE(_slime_c, m)
{
    EXPOSE_BUILD_FLAG(m, BUILD_RDMA);
    EXPOSE_BUILD_FLAG(m, BUILD_NVSHMEM);
    EXPOSE_BUILD_FLAG(m, BUILD_NVLINK);
    EXPOSE_BUILD_FLAG(m, BUILD_INTRA_OPS);
    EXPOSE_BUILD_FLAG(m, BUILD_INTER_OPS);

    py::enum_<slime::OpCode>(m, "OpCode")
        .value("READ", slime::OpCode::READ)
        .value("WRITE", slime::OpCode::WRITE)
        .value("WRITE_WITH_IMM_DATA", slime::OpCode::WRITE_WITH_IMM)
        .value("SEND", slime::OpCode::SEND)
        .value("RECV", slime::OpCode::RECV);

    py::class_<slime::Assignment>(m, "Assignment")
        .def(py::init<const uintptr_t&, uint64_t, uint64_t, uint64_t>())
        .def(py::init<const uintptr_t&, const uintptr_t&, uint64_t, uint64_t, uint64_t>());

#ifdef BUILD_RDMA
    py::class_<slime::RDMAAssign, std::shared_ptr<slime::RDMAAssign>>(m, "RDMAAssign")
        .def("wait", &slime::RDMAAssign::wait, py::call_guard<py::gil_scoped_release>())
        .def("latency", &slime::RDMAAssign::latency, py::call_guard<py::gil_scoped_release>());

    py::class_<slime::RDMAAssignHandler, std::shared_ptr<slime::RDMAAssignHandler>>(m, "RDMAAssignHandler")
        .def("wait", &slime::RDMAAssignHandler::wait, py::call_guard<py::gil_scoped_release>())
        .def("latency", &slime::RDMAAssignHandler::latency, py::call_guard<py::gil_scoped_release>());

    py::class_<slime::RDMAContext, std::shared_ptr<slime::RDMAContext>>(m, "rdma_context")
        .def(py::init<>())
        .def("init_rdma_context", &slime::RDMAContext::init)
        .def("register_memory_region",
             static_cast<int64_t (slime::RDMAContext::*)(const uintptr_t&, uintptr_t, uint64_t)>(
                 &slime::RDMAContext::registerOrAccessMemoryRegion))
        .def("register_remote_memory_region",
             static_cast<int64_t (slime::RDMAContext::*)(const uintptr_t&, json)>(
                 &slime::RDMAContext::registerOrAccessRemoteMemoryRegion))
        .def("reload_memory_pool", &slime::RDMAContext::reloadMemoryPool)
        .def("launch_future", &slime::RDMAContext::launch_future)
        .def("stop_future", &slime::RDMAContext::stop_future);

    py::class_<slime::RDMAEndpointV0, std::shared_ptr<slime::RDMAEndpointV0>>(m, "rdma_endpoint")
        .def(py::init<std::shared_ptr<slime::RDMAContext>, size_t>())
        .def("connect", &slime::RDMAEndpointV0::connect)
        .def("endpoint_info", &slime::RDMAEndpointV0::endpointInfo)
        .def("send", &slime::RDMAEndpointV0::send)
        .def("recv", &slime::RDMAEndpointV0::recv)
        .def("wait_send", &slime::RDMAEndpointV0::waitSend)
        .def("wait_recv", &slime::RDMAEndpointV0::waitRecv);

    m.def("available_nic", &slime::available_nic);
#endif

#ifdef BUILD_NVSHMEM
    py::class_<slime::NVShmemContext, std::shared_ptr<slime::NVShmemContext>>(m, "NVShmemContext")
        .def(py::init<const int, const int, const int>())
        .def("connect_full_mesh", &slime::NVShmemContext::connectFullMesh)
        .def("get_local_nvshmem_unique_id", &slime::NVShmemContext::getLocalNVShmemUniqueId)
        .def("register_memory_region", &slime::NVShmemContext::registerMemoryRegion)
        .def("send", &slime::NVShmemContext::send)
        .def("recv", &slime::NVShmemContext::recv)
        .def("alloc_dlpack_tensor", &slime::alloc_dlpack_tensor);
#endif

#ifdef BUILD_NVLINK
    py::class_<slime::NVLinkContext>(m, "nvlink_context")
        .def(py::init<>())
        .def("register_memory_region", &slime::NVLinkContext::register_memory_region)
        .def("register_remote_memory_region", &slime::NVLinkContext::register_remote_memory_region)
        .def("endpoint_info", &slime::NVLinkContext::endpoint_info)
        .def("connect", &slime::NVLinkContext::connect)
        .def("read_batch", &slime::NVLinkContext::read_batch);
#endif

#ifdef BUILD_INTRA_OPS
    py::class_<slime::AllToAllIntraLLBuffer>(m, "AllToAllIntraLLBuffer")
        .def(py::init<int32_t, int32_t, int32_t, int32_t, int64_t>())
        .def("buffer_info", &slime::AllToAllIntraLLBuffer::buffer_info)
        .def("connect_full_mesh", &slime::AllToAllIntraLLBuffer::connectFullMesh)
        .def("get_local_buffer", &slime::AllToAllIntraLLBuffer::getLocalBuffer)
        .def("get_buffer_size_hint", &slime::AllToAllIntraLLBuffer::get_buffer_size_hint)
        .def("set_max_bs", &slime::AllToAllIntraLLBuffer::setMaxBs)
        .def("all_to_all_ll",
             &slime::AllToAllIntraLLBuffer::allToAllLL2D,
             py::arg("x"),
             py::arg("is_transpose") = false,
             py::arg("mask")         = py::none(),
             "AllGather with optional mask");
#endif

#ifdef BUILD_INTER_OPS
    py::class_<slime::AllGatherInterLLBuffer>(m, "AllGatherInterLLBuffer")
        .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t>())
        .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t, bool>())
        .def("buffer_info", &slime::AllGatherInterLLBuffer::bufferInfo)
        .def("connect_full_mesh", &slime::AllGatherInterLLBuffer::connectFullMesh)
        .def("all_gather_ll", &slime::AllGatherInterLLBuffer::allGatherLL)
        .def("all_gather_ll_hook", &slime::AllGatherInterLLBuffer::allGatherLLHook);
#endif
}
