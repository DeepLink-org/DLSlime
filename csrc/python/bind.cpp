#include <pybind11/attr.h>
#include <pybind11/cast.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <functional>
#include <memory>

#include "dlslime/engine/assignment.h"
#include "dlslime/engine/dlpack.h"

#ifdef BUILD_NVLINK
#include "dlslime/engine/nvlink/memory_pool.h"
#include "dlslime/engine/nvlink/nvlink_endpoint.h"
#include "dlslime/engine/nvlink/nvlink_future.h"

#endif

#ifdef BUILD_NVSHMEM
#include "dlslime/engine/nvshmem/nvshmem_context.h"
#endif

#include "dlslime/device/signal.h"

#ifdef BUILD_RDMA
#include "dlslime/engine/rdma/rdma_assignment.h"
#include "dlslime/engine/rdma/rdma_config.h"
#include "dlslime/engine/rdma/rdma_context.h"
// Include the new Unified Endpoint
#include "dlslime/engine/rdma/rdma_endpoint.h"
// We still need these headers if UnifiedRDMAEndpoint implementation depends on them being complete types,
// or if we expose them directly (which we are phasing out).
#include "dlslime/engine/rdma/rdma_future.h"
#include "dlslime/engine/rdma/rdma_io_endpoint.h"
#include "dlslime/engine/rdma/rdma_msg_endpoint.h"
#include "dlslime/engine/rdma/rdma_utils.h"
#include "dlslime/engine/rdma/rdma_worker.h"
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

#include "dlslime/json.hpp"
#include "dlslime/logging.h"
#include "pybind_json/pybind_json.hpp"

using json = nlohmann::json;

namespace py = pybind11;

#ifdef BUILD_NVSHMEM

namespace dlslime {

py::object alloc_dlpack_tensor(dlslime::NVShmemContext& self, size_t size, size_t alignment)
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

}  // namespace dlslime

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

    py::enum_<dlslime::OpCode>(m, "OpCode")
        .value("READ", dlslime::OpCode::READ)
        .value("WRITE", dlslime::OpCode::WRITE)
        .value("WRITE_WITH_IMM_DATA", dlslime::OpCode::WRITE_WITH_IMM)
        .value("SEND", dlslime::OpCode::SEND)
        .value("RECV", dlslime::OpCode::RECV);

    py::class_<dlslime::Assignment>(m, "Assignment")
        .def(py::init<const uintptr_t&, uint64_t, uint64_t, uint64_t>())
        .def(py::init<const uintptr_t&, const uintptr_t&, uint64_t, uint64_t, uint64_t>());

    py::class_<dlslime::device::DeviceSignal, std::shared_ptr<dlslime::device::DeviceSignal>>(m, "DeviceSignal")
        .def("wait", &dlslime::device::DeviceSignal::wait_comm_done_cpu);

#ifdef BUILD_RDMA
    py::class_<dlslime::RDMAContext, std::shared_ptr<dlslime::RDMAContext>>(m, "RDMAContext")
        .def(py::init<>())
        .def("init", &dlslime::RDMAContext::init)
        .def("launch_future", &dlslime::RDMAContext::launch_future)
        .def("stop_future", &dlslime::RDMAContext::stop_future);

    // =========================================================================
    // Unified RDMA Endpoint Binding
    // =========================================================================
    // Replaces both rdma_endpoint (V0) and rdma_io_endpoint bindings
    py::class_<dlslime::SendFuture, std::shared_ptr<dlslime::SendFuture>>(m, "SlimeSendFuture")
        .def("wait", &dlslime::SendFuture::wait, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::RecvFuture, std::shared_ptr<dlslime::RecvFuture>>(m, "SlimeRecvFuture")
        .def("wait", &dlslime::RecvFuture::wait, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::ReadWriteFuture, std::shared_ptr<dlslime::ReadWriteFuture>>(m, "SlimeReadWriteFuture")
        .def("wait", &dlslime::ReadWriteFuture::wait, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::ImmRecvFuture, std::shared_ptr<dlslime::ImmRecvFuture>>(m, "SlimeImmRecvFuture")
        .def("wait", &dlslime::ImmRecvFuture::wait, py::call_guard<py::gil_scoped_release>())
        .def("imm_data", &dlslime::ImmRecvFuture::immData, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::RDMAEndpoint, std::shared_ptr<dlslime::RDMAEndpoint>>(m, "RDMAEndpoint")
        .def(py::init<std::shared_ptr<dlslime::RDMAContext>, size_t, std::shared_ptr<dlslime::RDMAWorker>>(),
             py::arg("context"),
             py::arg("num_qp") = 1,
             py::arg("worker") = nullptr)
        .def(py::init<std::string, int32_t, std::string, size_t, std::shared_ptr<dlslime::RDMAWorker>>(),
             py::arg("device_name") = "",
             py::arg("ib_port")     = 1,
             py::arg("link_type")   = "RoCE",
             py::arg("num_qp")      = 1,
             py::arg("worker")      = nullptr)
        .def("connect", &dlslime::RDMAEndpoint::connect, py::call_guard<py::gil_scoped_release>())
        .def("endpoint_info", &dlslime::RDMAEndpoint::endpointInfo)

        .def("register_memory_region",
             &dlslime::RDMAEndpoint::registerOrAccessMemoryRegion,
             py::arg("mr_key"),
             py::arg("data_ptr"),
             py::arg("offset"),
             py::arg("length"),
             py::call_guard<py::gil_scoped_release>())
        .def("register_remote_memory_region",
             &dlslime::RDMAEndpoint::registerOrAccessRemoteMemoryRegion,
             py::call_guard<py::gil_scoped_release>())

        // --- Msg Operations ---
        .def("send",
             &dlslime::RDMAEndpoint::send,
             py::arg("chunk"),
             py::arg("stream_handler") = nullptr,
             py::call_guard<py::gil_scoped_release>())

        .def("recv",
             &dlslime::RDMAEndpoint::recv,
             py::arg("chunk"),
             py::arg("stream_handler") = nullptr,
             py::call_guard<py::gil_scoped_release>())

        // --- IO Operations ---
        .def("read",
             &dlslime::RDMAEndpoint::read,
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>())
        .def("write",
             &dlslime::RDMAEndpoint::write,
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>())
        .def("write_with_imm",
             &dlslime::RDMAEndpoint::writeWithImm,
             py::arg("assign"),
             py::arg("imm_data") = 0,
             py::arg("stream")   = nullptr,
             py::call_guard<py::gil_scoped_release>())
        .def("imm_recv",
             &dlslime::RDMAEndpoint::immRecv,
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>())

        .def("process", &dlslime::RDMAEndpoint::process, py::call_guard<py::gil_scoped_release>());

    // =========================================================================
    // RDMA Worker (Scheduler)
    // =========================================================================
    py::class_<dlslime::RDMAWorker, std::shared_ptr<dlslime::RDMAWorker>>(m, "RDMAWorker")
        .def(py::init<std::string, int>(), py::arg("dev_name"), py::arg("id"))
        .def(py::init<int32_t, int>(), py::arg("socket_id"), py::arg("id"))
        .def("start", &dlslime::RDMAWorker::start)
        .def("stop", &dlslime::RDMAWorker::stop)

        // Now it accepts the Unified Endpoint
        .def("add_endpoint", &dlslime::RDMAWorker::addEndpoint, py::arg("endpoint"));

    m.def("available_nic", &dlslime::available_nic);
    m.def("socket_id", &dlslime::socketId);
#endif

#ifdef BUILD_NVSHMEM
    py::class_<dlslime::NVShmemContext, std::shared_ptr<dlslime::NVShmemContext>>(m, "NVShmemContext")
        .def(py::init<const int, const int, const int>())
        .def("connect_full_mesh", &dlslime::NVShmemContext::connectFullMesh)
        .def("get_local_nvshmem_unique_id", &dlslime::NVShmemContext::getLocalNVShmemUniqueId)
        .def("register_memory_region", &dlslime::NVShmemContext::registerMemoryRegion)
        .def("send", &dlslime::NVShmemContext::send)
        .def("recv", &dlslime::NVShmemContext::recv)
        .def("alloc_dlpack_tensor", &dlslime::alloc_dlpack_tensor);
#endif

#ifdef BUILD_NVLINK
    py::class_<dlslime::NVLinkFuture, std::shared_ptr<dlslime::NVLinkFuture>>(m, "SlimeNVLinkFuture")
        .def("wait", &dlslime::NVLinkFuture::wait, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::NVLinkEndpoint>(m, "NVLinkEndpoint")
        .def(py::init<>())
        .def("register_memory_region",
             &dlslime::NVLinkEndpoint::register_memory_region,
             py::arg("mr_key"),
             py::arg("data_ptr"),
             py::arg("offset"),
             py::arg("length"))
        .def("register_remote_memory_region",
             &dlslime::NVLinkEndpoint::register_remote_memory_region,
             py::call_guard<py::gil_scoped_release>())
        .def("endpoint_info", &dlslime::NVLinkEndpoint::endpoint_info)
        .def("connect", &dlslime::NVLinkEndpoint::connect)
        .def("read",
             &dlslime::NVLinkEndpoint::read,
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>());
#endif

#ifdef BUILD_INTRA_OPS
    py::class_<dlslime::AllToAllIntraLLBuffer>(m, "AllToAllIntraLLBuffer")
        .def(py::init<int32_t, int32_t, int32_t, int32_t, int64_t>())
        .def("buffer_info", &dlslime::AllToAllIntraLLBuffer::buffer_info)
        .def("connect_full_mesh", &dlslime::AllToAllIntraLLBuffer::connectFullMesh)
        .def("get_local_buffer", &dlslime::AllToAllIntraLLBuffer::getLocalBuffer)
        .def("get_buffer_size_hint", &dlslime::AllToAllIntraLLBuffer::get_buffer_size_hint)
        .def("set_max_bs", &dlslime::AllToAllIntraLLBuffer::setMaxBs)
        .def("all_to_all_ll",
             &dlslime::AllToAllIntraLLBuffer::allToAllLL2D,
             py::arg("x"),
             py::arg("is_transpose") = false,
             py::arg("mask")         = py::none(),
             "AllGather with optional mask");
#endif

#ifdef BUILD_INTER_OPS
    py::class_<dlslime::AllGatherInterLLBuffer>(m, "AllGatherInterLLBuffer")
        .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t>())
        .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t, bool>())
        .def("buffer_info", &dlslime::AllGatherInterLLBuffer::bufferInfo)
        .def("connect_full_mesh", &dlslime::AllGatherInterLLBuffer::connectFullMesh)
        .def("all_gather_ll", &dlslime::AllGatherInterLLBuffer::allGatherLL)
        .def("all_gather_ll_hook", &dlslime::AllGatherInterLLBuffer::allGatherLLHook);
#endif
}
