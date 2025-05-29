#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_scheduler.h"

#include "gloo/rendezvous/context.h"
#include "gloo/rendezvous/file_store.h"
#include "gloo/rendezvous/prefix_store.h"
#include "gloo/rendezvous/store.h"
#include "gloo/transport/device.h"
#include "gloo/transport/ibverbs/device.h"

#include <functional>
#include <pybind11/cast.h>
#include <pybind11/pytypes.h>

#ifdef BUILD_NVLINK
#include "engine/nvlink/memory_pool.h"
#include "engine/nvlink/nvlink_transport.h"
#endif

#include "utils/json.hpp"
#include "utils/logging.h"
#include "utils/utils.h"

#include "pybind_json/pybind_json.hpp"

#include <cstdint>
#include <memory>
#include <pybind11/chrono.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

using json = nlohmann::json;

namespace py = pybind11;

PYBIND11_MODULE(_slime_c, m)
{
    py::enum_<slime::OpCode>(m, "OpCode")
        .value("READ", slime::OpCode::READ)
        .value("SEND", slime::OpCode::SEND)
        .value("RECV", slime::OpCode::RECV);

    py::class_<slime::Assignment>(m, "Assignment").def(py::init<std::string, uint64_t, uint64_t, uint64_t>());

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

    py::class_<slime::RDMAContext>(m, "rdma_context")
        .def(py::init<>())
        .def("init_rdma_context", &slime::RDMAContext::init)
        .def("register_memory_region", &slime::RDMAContext::register_memory_region)
        .def("register_remote_memory_region", &slime::RDMAContext::register_remote_memory_region)
        .def("endpoint_info", &slime::RDMAContext::endpoint_info)
        .def("connect", &slime::RDMAContext::connect)
        .def("launch_future", &slime::RDMAContext::launch_future)
        .def("stop_future", &slime::RDMAContext::stop_future)
        .def("submit", &slime::RDMAContext::submit, py::call_guard<py::gil_scoped_release>());

    m.def("available_nic", &slime::available_nic);

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
