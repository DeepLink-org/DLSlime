#include "engine/assignment.h"

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
#include "engine/rdma/rdma_endpoint.h"
#include "engine/rdma/rdma_scheduler.h"
#endif

#include "utils/json.hpp"
#include "utils/logging.h"
#include "utils/utils.h"

#include <cstdint>
#include <functional>
#include <memory>

#include <pybind11/cast.h>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

#include "pybind_json/pybind_json.hpp"

using json = nlohmann::json;

namespace py = pybind11;

PYBIND11_MODULE(_slime_c, m)
{
    py::enum_<slime::OpCode>(m, "OpCode")
        .value("READ", slime::OpCode::READ)
        .value("WRITE", slime::OpCode::WRITE)
        .value("WRITE_WITH_IMM_DATA", slime::OpCode::WRITE_WITH_IMM)
        .value("SEND", slime::OpCode::SEND)
        .value("RECV", slime::OpCode::RECV);

    py::class_<slime::Assignment>(m, "Assignment").def(py::init<std::string, uint64_t, uint64_t, uint64_t>());

#ifdef BUILD_RDMA
    py::class_<slime::RDMAAssignment, slime::RDMAAssignmentSharedPtr>(m, "RDMAAssignment")
        .def("wait", &slime::RDMAAssignment::wait, py::call_guard<py::gil_scoped_release>())
        .def("latency", &slime::RDMAAssignment::latency, py::call_guard<py::gil_scoped_release>());

    py::class_<slime::RDMASchedulerAssignment, slime::RDMASchedulerAssignmentSharedPtr>(m, "RDMASchedulerAssignment")
        .def("wait", &slime::RDMASchedulerAssignment::wait, py::call_guard<py::gil_scoped_release>())
        .def("latency", &slime::RDMASchedulerAssignment::latency, py::call_guard<py::gil_scoped_release>());

    py::class_<slime::RDMAScheduler>(m, "RDMAScheduler")
        .def(py::init<const std::vector<std::string>&>())
        .def("register_memory_region", &slime::RDMAScheduler::register_memory_region)
        .def("connect", &slime::RDMAScheduler::connect)
        .def("submit_assignment", &slime::RDMAScheduler::submitAssignment)
        .def("scheduler_info", &slime::RDMAScheduler::scheduler_info);

    py::class_<slime::RDMAContext, std::shared_ptr<slime::RDMAContext>>(m, "rdma_context")
        .def(py::init<>())
        .def(py::init<size_t>())
        .def("init_rdma_context", &slime::RDMAContext::init)
        .def("register_memory_region", &slime::RDMAContext::register_memory_region)
        .def("register_remote_memory_region",
             static_cast<int64_t (slime::RDMAContext::*)(std::string, json)>(
                 &slime::RDMAContext::register_remote_memory_region))
        .def("reload_memory_pool", &slime::RDMAContext::reload_memory_pool)
        .def("endpoint_info", &slime::RDMAContext::endpoint_info)
        .def("connect", &slime::RDMAContext::connect)
        .def("launch_future", &slime::RDMAContext::launch_future)
        .def("stop_future", &slime::RDMAContext::stop_future)
        .def("submit", &slime::RDMAContext::submit, py::call_guard<py::gil_scoped_release>());

    py::class_<slime::RDMAEndpoint, std::shared_ptr<slime::RDMAEndpoint>>(m, "rdma_endpoint")
        .def(py::init<const std::string&, uint8_t, const std::string&, size_t>())
        .def("context_connect", &slime::RDMAEndpoint::connect)
        .def("get_data_context_info", &slime::RDMAEndpoint::getDataContextInfo)
        .def("get_meta_context_info", &slime::RDMAEndpoint::getMetaContextInfo);

    py::class_<slime::RDMABuffer, std::shared_ptr<slime::RDMABuffer>>(m, "rdma_buffer")
        .def(py::init<std::shared_ptr<slime::RDMAEndpoint>,
                      std::vector<uintptr_t>,
                      std::vector<size_t>,
                      std::vector<size_t>>())
        .def("send", &slime::RDMABuffer::send)
        .def("recv", &slime::RDMABuffer::recv)
        .def("wait_send", &slime::RDMABuffer::waitSend)
        .def("wait_recv", &slime::RDMABuffer::waitRecv);

    m.def("available_nic", &slime::available_nic);
#endif

#ifdef BUILD_NVSHMEM
    py::class_<slime::NVShmemContext, std::shared_ptr<slime::NVShmemContext>>(m, "NVShmemContext")
        .def(py::init<const int, const int, const int>())
        .def("connect_full_mesh", &slime::NVShmemContext::connectFullMesh)
        .def("get_local_nvshmem_unique_id", &slime::NVShmemContext::getLocalNVShmemUniqueId);
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
}
