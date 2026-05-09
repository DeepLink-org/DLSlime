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
#include <optional>

#include "dlslime/csrc/engine/assignment.h"

#ifdef BUILD_NVLINK
#include "dlslime/csrc/engine/nvlink/memory_pool.h"
#include "dlslime/csrc/engine/nvlink/nvlink_endpoint.h"
#include "dlslime/csrc/engine/nvlink/nvlink_future.h"
#endif

#ifdef BUILD_ASCEND_DIRECT
#include "dlslime/csrc/engine/ascend_direct/ascend_direct_endpoint.h"
#include "dlslime/csrc/engine/ascend_direct/ascend_future.h"
#include "dlslime/csrc/engine/ascend_direct/ascend_local_memory_pool.h"
#include "dlslime/csrc/engine/ascend_direct/ascend_remote_memory_pool.h"
#endif

#include "dlslime/csrc/device/signal.h"

#ifdef BUILD_RDMA
#include "dlslime/csrc/engine/rdma/rdma_assignment.h"
#include "dlslime/csrc/engine/rdma/rdma_context.h"
#include "dlslime/csrc/engine/rdma/rdma_endpoint.h"
#include "dlslime/csrc/engine/rdma/rdma_future.h"
#include "dlslime/csrc/engine/rdma/rdma_utils.h"
#include "dlslime/csrc/engine/rdma/rdma_worker.h"
#endif

#ifdef BUILD_RPC
#include "dlslime/csrc/rpc/rpc_constants.h"
#include "dlslime/csrc/rpc/rpc_session.h"
#endif

// Forward declaration: cache bindings live in a separate translation
// unit (cache/bindings.cpp) so they can be developed independently.
void bind_cache(pybind11::module_& m);

// Ops moved to NanoCCL - these includes are commented out
// #if defined(BUILD_INTRA_OPS) || defined(BUILD_INTER_OPS)
// #include <torch/torch.h>
//
// #ifdef BUILD_INTRA_OPS
// #include "nanoccl/csrc/ops/intra_ll/all_to_all/all_to_all_intra_ll_buffer.h"
// #endif
//
// #ifdef BUILD_INTER_OPS
// #include "nanoccl/csrc/ops/inter_ll/all_gather_inter_ll/all_gather_inter_ll_buffer.h"
// #endif
//
// #endif

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/common/pybind_json/pybind_json.hpp"
#include "dlslime/csrc/logging.h"
#include "dlslime/csrc/observability/obs.h"
#include "dlslime/csrc/topology.h"

using json = nlohmann::json;

namespace py = pybind11;

#ifdef BUILD_RDMA
#define BUILD_RDMA_ENABLED true
#else
#define BUILD_RDMA_ENABLED false
#endif

#ifdef BUILD_NVLINK
#define BUILD_NVLINK_ENABLED true
#else
#define BUILD_NVLINK_ENABLED false
#endif

#ifdef BUILD_RPC
#define BUILD_RPC_ENABLED true
#else
#define BUILD_RPC_ENABLED false
#endif

// Ops moved to NanoCCL
#define BUILD_INTRA_OPS_ENABLED false
#define BUILD_INTER_OPS_ENABLED false

#define EXPOSE_BUILD_FLAG(m, flag) m.attr("_" #flag) = flag##_ENABLED

PYBIND11_MODULE(_slime_c, m)
{
    EXPOSE_BUILD_FLAG(m, BUILD_RDMA);
    EXPOSE_BUILD_FLAG(m, BUILD_NVLINK);
    EXPOSE_BUILD_FLAG(m, BUILD_INTRA_OPS);
    EXPOSE_BUILD_FLAG(m, BUILD_INTER_OPS);
    EXPOSE_BUILD_FLAG(m, BUILD_RPC);

    m.def("discover_topology",
          &dlslime::topology::discoverTopology,
          py::arg("preferred_device")    = std::nullopt,
          py::arg("ib_port")             = 1,
          py::arg("preferred_link_type") = std::nullopt,
          py::arg("sysfs_root")          = "/sys",
          py::arg("devices")             = std::nullopt);

    py::enum_<dlslime::OpCode>(m, "OpCode")
        .value("READ", dlslime::OpCode::READ)
        .value("WRITE", dlslime::OpCode::WRITE)
        .value("WRITE_WITH_IMM_DATA", dlslime::OpCode::WRITE_WITH_IMM)
        .value("SEND", dlslime::OpCode::SEND)
        .value("RECV", dlslime::OpCode::RECV);

    py::class_<dlslime::Assignment>(m, "Assignment")
        .def(py::init<const uintptr_t&, uint64_t, uint64_t, uint64_t>())
        .def(py::init<const uintptr_t&, const uintptr_t&, uint64_t, uint64_t, uint64_t>())
        .def_readwrite("mr_key", &dlslime::Assignment::mr_key)
        .def_readwrite("remote_mr_key", &dlslime::Assignment::remote_mr_key)
        .def_readwrite("target_offset", &dlslime::Assignment::target_offset)
        .def_readwrite("source_offset", &dlslime::Assignment::source_offset)
        .def_readwrite("length", &dlslime::Assignment::length);

    // DLSlimeCache is always on. Its peer/version directory reuses the
    // public Assignment binding above, so bind it after Assignment.
    bind_cache(m);

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
        .def("imm_data", &dlslime::ImmRecvFuture::immData, py::call_guard<py::gil_scoped_release>())
        .def("time_trace_enabled", &dlslime::ImmRecvFuture::timeTraceEnabled, py::call_guard<py::gil_scoped_release>())
        .def("time_trace_start_ns", &dlslime::ImmRecvFuture::timeTraceStartNs, py::call_guard<py::gil_scoped_release>())
        .def("time_trace_end_ns", &dlslime::ImmRecvFuture::timeTraceEndNs, py::call_guard<py::gil_scoped_release>())
        .def("time_trace_elapsed_ns",
             &dlslime::ImmRecvFuture::timeTraceElapsedNs,
             py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::RDMAMemoryPool, std::shared_ptr<dlslime::RDMAMemoryPool>>(m, "RDMAMemoryPool")
        .def(py::init<std::shared_ptr<dlslime::RDMAContext>>(), py::arg("context"))
        .def(
            "register_memory_region",
            [](dlslime::RDMAMemoryPool& self, uintptr_t data_ptr, uint64_t length, py::object name_obj) {
                std::optional<std::string> name = std::nullopt;
                if (!name_obj.is_none()) {
                    name = name_obj.cast<std::string>();
                }
                return self.registerMemoryRegion(data_ptr, length, name);
            },
            py::arg("data_ptr"),
            py::arg("length"),
            py::arg("name") = py::none())
        .def("get_handle",
             static_cast<int32_t (dlslime::RDMAMemoryPool::*)(const std::string&)>(
                 &dlslime::RDMAMemoryPool::get_mr_handle))
        .def(
            "unregister_memory_region",
            [](dlslime::RDMAMemoryPool& self, py::object key) {
                if (py::isinstance<py::str>(key)) {
                    return self.unregisterMemoryRegion(key.cast<std::string>());
                }
                return self.unregisterMemoryRegion(key.cast<int32_t>());
            },
            py::arg("key"))
        .def("mr_info", &dlslime::RDMAMemoryPool::mrInfo);
    py::class_<dlslime::RDMAEndpoint, std::shared_ptr<dlslime::RDMAEndpoint>>(m, "RDMAEndpoint")
        .def(py::init<std::shared_ptr<dlslime::RDMAMemoryPool>, size_t, std::shared_ptr<dlslime::RDMAWorker>>(),
             py::arg("pool"),
             py::arg("num_qp") = 1,
             py::arg("worker") = nullptr)
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
        .def("mr_info", &dlslime::RDMAEndpoint::mrInfo)
        .def("get_pool", &dlslime::RDMAEndpoint::get_local_pool)

        .def("register_memory_region",
             py::overload_cast<uintptr_t, uintptr_t, uintptr_t, size_t>(
                 &dlslime::RDMAEndpoint::registerOrAccessMemoryRegion),
             py::arg("mr_key"),
             py::arg("data_ptr"),
             py::arg("offset"),
             py::arg("length"),
             py::call_guard<py::gil_scoped_release>())
        .def("register_memory_region",
             py::overload_cast<const std::string&, uintptr_t, uintptr_t, size_t>(
                 &dlslime::RDMAEndpoint::registerOrAccessMemoryRegion),
             py::arg("name"),
             py::arg("data_ptr"),
             py::arg("offset"),
             py::arg("length"),
             py::call_guard<py::gil_scoped_release>())

        .def("register_remote_memory_region",
             py::overload_cast<const std::string&, json>(&dlslime::RDMAEndpoint::registerOrAccessRemoteMemoryRegion),
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
        .def("process", &dlslime::RDMAEndpoint::process, py::call_guard<py::gil_scoped_release>())
        .def("shutdown", &dlslime::RDMAEndpoint::shutdown, py::call_guard<py::gil_scoped_release>());

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

#ifdef BUILD_RPC
    // =========================================================================
    // SlimeRPC C++ session bindings
    // =========================================================================
    // Wire-format constants exposed for cross-checking with Python.
    auto rpc_mod                   = m.def_submodule("rpc", "SlimeRPC C++ backend");
    rpc_mod.attr("WIRE_VERSION")   = dlslime::rpc::WIRE_VERSION;
    rpc_mod.attr("HEADER_SIZE")    = dlslime::rpc::HEADER_SIZE;
    rpc_mod.attr("EXC_BIT")        = dlslime::rpc::EXC_BIT;
    rpc_mod.attr("ACK_BIT")        = dlslime::rpc::ACK_BIT;
    rpc_mod.attr("REPLY_BIT")      = dlslime::rpc::REPLY_BIT;
    rpc_mod.attr("CHUNK_BIT")      = dlslime::rpc::CHUNK_BIT;
    rpc_mod.attr("LAST_BIT")       = dlslime::rpc::LAST_BIT;
    rpc_mod.attr("FLAG_MASK")      = dlslime::rpc::FLAG_MASK;
    rpc_mod.attr("MAX_SLOT_COUNT") = dlslime::rpc::MAX_SLOT_COUNT;

    py::class_<dlslime::rpc::ClientFuture, std::shared_ptr<dlslime::rpc::ClientFuture>>(rpc_mod, "ClientFuture")
        // GIL is dropped inside wait()/wait_for() themselves; do NOT use
        // py::call_guard here (it would double-release the GIL).
        .def("wait", &dlslime::rpc::ClientFuture::wait)
        .def(
            "wait_for",
            [](dlslime::rpc::ClientFuture& self, double timeout_seconds) -> py::object {
                py::bytes out;
                int64_t   ms = static_cast<int64_t>(timeout_seconds * 1000.0);
                if (ms < 0)
                    ms = 0;
                bool done = self.wait_for(ms, &out);
                if (!done) {
                    return py::none();
                }
                return out;
            },
            py::arg("timeout_seconds"))
        .def("done", &dlslime::rpc::ClientFuture::done)
        .def("has_exception", &dlslime::rpc::ClientFuture::has_exception)
        .def("rethrow_if_failed", &dlslime::rpc::ClientFuture::rethrow_if_failed)
        .def_property_readonly("is_remote_exception", &dlslime::rpc::ClientFuture::is_remote_exception)
        .def_property_readonly("request_id", &dlslime::rpc::ClientFuture::request_id)
        .def_property_readonly("slot_id", &dlslime::rpc::ClientFuture::slot_id);

    // InboundRequest is a value type; expose only readers.
    //
    // Tier A zero-copy semantics: payload_ptr/payload_nbytes borrow into
    // the C++ recv slot. They are valid only until the matching
    // post_reply has been posted; raw handlers should consume the bytes
    // before replying. The ``payload`` accessor below builds a fresh
    // py::bytes by copying once into Python heap (single copy, suitable
    // for typed/pickled handlers that must outlive the slot).
    py::class_<dlslime::rpc::InboundRequest>(rpc_mod, "InboundRequest")
        .def_readonly("raw_tag", &dlslime::rpc::InboundRequest::raw_tag)
        .def_readonly("base_tag", &dlslime::rpc::InboundRequest::base_tag)
        .def_readonly("request_id", &dlslime::rpc::InboundRequest::request_id)
        .def_readonly("slot_id", &dlslime::rpc::InboundRequest::slot_id)
        // Pointer + length into the recv slot. Borrowed; do NOT free.
        .def_property_readonly(
            "payload_ptr",
            [](const dlslime::rpc::InboundRequest& self) { return reinterpret_cast<uintptr_t>(self.payload_ptr); })
        .def_property_readonly("payload_nbytes",
                               [](const dlslime::rpc::InboundRequest& self) { return self.payload_nbytes; })
        // Lazy bytes accessor for typed handlers. Each call rebuilds.
        .def_property_readonly("payload", [](const dlslime::rpc::InboundRequest& self) {
            return py::bytes(self.payload_ptr, self.payload_nbytes);
        });

    py::class_<dlslime::rpc::RpcSession, std::shared_ptr<dlslime::rpc::RpcSession>>(rpc_mod, "RpcSession")
        .def(py::init<std::shared_ptr<dlslime::RDMAEndpoint>,
                      int,
                      uintptr_t,
                      int,
                      uintptr_t,
                      int,
                      size_t,
                      size_t,
                      bool>(),
             py::arg("endpoint"),
             py::arg("local_send_handler"),
             py::arg("local_send_ptr"),
             py::arg("local_recv_handler"),
             py::arg("local_recv_ptr"),
             py::arg("remote_recv_handler"),
             py::arg("slot_size"),
             py::arg("slot_count"),
             py::arg("start_recv_pump") = true)
        .def("call", &dlslime::rpc::RpcSession::call, py::arg("tag"), py::arg("payload"))
        .def("call_inplace", &dlslime::rpc::RpcSession::call_inplace, py::arg("tag"), py::arg("writer"))
        .def("post_reply",
             &dlslime::rpc::RpcSession::post_reply,
             py::arg("tag"),
             py::arg("request_id"),
             py::arg("client_slot_id"),
             py::arg("payload"))
        .def("post_reply_inplace",
             &dlslime::rpc::RpcSession::post_reply_inplace,
             py::arg("tag"),
             py::arg("request_id"),
             py::arg("client_slot_id"),
             py::arg("writer"))
        .def("poll_request",
             [](dlslime::rpc::RpcSession& self) -> py::object {
                 auto req = self.poll_request();
                 if (!req.has_value()) {
                     return py::none();
                 }
                 return py::cast(std::move(*req));
             })
        .def("close", &dlslime::rpc::RpcSession::close, py::call_guard<py::gil_scoped_release>())
        .def("detach_request", &dlslime::rpc::RpcSession::detach_request, py::arg("request_id"))
        .def_property_readonly("slot_count", &dlslime::rpc::RpcSession::slot_count)
        .def_property_readonly("slot_size", &dlslime::rpc::RpcSession::slot_size)
        .def_property_readonly("payload_capacity", &dlslime::rpc::RpcSession::payload_capacity)
        .def_property_readonly("closed", &dlslime::rpc::RpcSession::closed)
        .def_property_readonly("requests_sent", &dlslime::rpc::RpcSession::requests_sent)
        .def_property_readonly("replies_received", &dlslime::rpc::RpcSession::replies_received)
        .def_property_readonly("requests_received", &dlslime::rpc::RpcSession::requests_received);
#endif  // BUILD_RPC

#endif

#ifdef BUILD_NVLINK
    py::class_<dlslime::NVLinkFuture, std::shared_ptr<dlslime::NVLinkFuture>>(m, "SlimeNVLinkFuture")
        .def("wait", &dlslime::NVLinkFuture::wait, py::call_guard<py::gil_scoped_release>());
    py::class_<dlslime::NVLinkEndpoint>(m, "NVLinkEndpoint")
        .def(py::init<>())
        .def(
            "register_memory_region",
            [](dlslime::NVLinkEndpoint& self, uintptr_t addr, uint64_t offset, size_t length, py::object name_obj) {
                std::optional<std::string> name = std::nullopt;
                if (!name_obj.is_none()) {
                    name = name_obj.cast<std::string>();
                }
                return self.register_memory_region(addr, offset, length, name);
            },
            py::arg("addr"),
            py::arg("offset"),
            py::arg("length"),
            py::arg("name") = py::none(),
            py::call_guard<py::gil_scoped_release>())
        .def(
            "register_remote_memory_region",
            [](dlslime::NVLinkEndpoint& self, const json& mr_info, py::object name_obj) {
                std::optional<std::string> name = std::nullopt;
                if (!name_obj.is_none()) {
                    name = name_obj.cast<std::string>();
                }
                return self.register_remote_memory_region(mr_info, name);
            },
            py::arg("mr_info"),
            py::arg("name") = py::none(),
            py::call_guard<py::gil_scoped_release>())
        .def("get_mr_handle", &dlslime::NVLinkEndpoint::get_mr_handle, py::arg("name"))
        .def("get_remote_mr_handle", &dlslime::NVLinkEndpoint::get_remote_mr_handle, py::arg("name"))
        .def("endpoint_info", &dlslime::NVLinkEndpoint::endpoint_info)
        .def("mr_info", &dlslime::NVLinkEndpoint::mr_info)
        .def("connect", &dlslime::NVLinkEndpoint::connect)
        .def("read",
             static_cast<std::shared_ptr<dlslime::NVLinkFuture> (dlslime::NVLinkEndpoint::*)(
                 std::vector<dlslime::named_assign_tuple_t>&, void*)>(&dlslime::NVLinkEndpoint::read),
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>())
        .def("read",
             static_cast<std::shared_ptr<dlslime::NVLinkFuture> (dlslime::NVLinkEndpoint::*)(
                 std::vector<dlslime::assign_tuple_t>&, void*)>(&dlslime::NVLinkEndpoint::read),
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>());
#endif

#ifdef BUILD_ASCEND_DIRECT
    // =========================================================================
    // Ascend Direct (NPU P2P Transfer)
    // =========================================================================
    py::class_<dlslime::AscendFuture, std::shared_ptr<dlslime::AscendFuture>>(m, "SlimeAscendFuture")
        .def("wait", &dlslime::AscendFuture::wait, py::call_guard<py::gil_scoped_release>());

    py::class_<dlslime::AscendDirectEndpoint, std::shared_ptr<dlslime::AscendDirectEndpoint>>(m, "AscendDirectEndpoint")
        .def(py::init<>())
        .def("init",
             &dlslime::AscendDirectEndpoint::init,
             py::arg("host"),
             py::arg("port"),
             py::call_guard<py::gil_scoped_release>())
        .def(
            "register_memory_region",
            [](dlslime::AscendDirectEndpoint& self, uintptr_t addr, size_t offset, size_t length, py::object name_obj) {
                std::optional<std::string> name = std::nullopt;
                if (!name_obj.is_none()) {
                    name = name_obj.cast<std::string>();
                }
                return self.register_memory_region(addr, offset, length, name);
            },
            py::arg("addr"),
            py::arg("offset"),
            py::arg("length"),
            py::arg("name") = py::none(),
            py::call_guard<py::gil_scoped_release>())
        .def(
            "register_remote_memory_region",
            [](dlslime::AscendDirectEndpoint& self, const json& mr_info, py::object name_obj) {
                std::optional<std::string> name = std::nullopt;
                if (!name_obj.is_none()) {
                    name = name_obj.cast<std::string>();
                }
                return self.register_remote_memory_region(mr_info, name);
            },
            py::arg("mr_info"),
            py::arg("name") = py::none(),
            py::call_guard<py::gil_scoped_release>())
        .def("get_mr_handle", &dlslime::AscendDirectEndpoint::get_mr_handle, py::arg("name"))
        .def("get_remote_mr_handle", &dlslime::AscendDirectEndpoint::get_remote_mr_handle, py::arg("name"))
        .def("endpoint_info", &dlslime::AscendDirectEndpoint::endpoint_info, py::call_guard<py::gil_scoped_release>())
        .def("mr_info", &dlslime::AscendDirectEndpoint::mr_info, py::call_guard<py::gil_scoped_release>())
        .def("connect",
             &dlslime::AscendDirectEndpoint::connect,
             py::arg("remote_info"),
             py::call_guard<py::gil_scoped_release>())
        .def("read",
             static_cast<std::shared_ptr<dlslime::AscendFuture> (dlslime::AscendDirectEndpoint::*)(
                 std::vector<dlslime::named_assign_tuple_t>&, void*)>(&dlslime::AscendDirectEndpoint::read),
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>())
        .def("read",
             static_cast<std::shared_ptr<dlslime::AscendFuture> (dlslime::AscendDirectEndpoint::*)(
                 std::vector<dlslime::assign_tuple_t>&, void*)>(&dlslime::AscendDirectEndpoint::read),
             py::arg("assign"),
             py::arg("stream") = nullptr,
             py::call_guard<py::gil_scoped_release>());
#endif

    // Ops moved to NanoCCL - Python bindings should be in NanoCCL's Python module
    // #ifdef BUILD_INTRA_OPS
    //     py::class_<dlslime::AllToAllIntraLLBuffer>(m, "AllToAllIntraLLBuffer")
    //         .def(py::init<int32_t, int32_t, int32_t, int32_t, int64_t>())
    //         .def("buffer_info", &dlslime::AllToAllIntraLLBuffer::buffer_info)
    //         .def("connect_full_mesh", &dlslime::AllToAllIntraLLBuffer::connectFullMesh)
    //         .def("get_local_buffer", &dlslime::AllToAllIntraLLBuffer::getLocalBuffer)
    //         .def("get_buffer_size_hint", &dlslime::AllToAllIntraLLBuffer::get_buffer_size_hint)
    //         .def("set_max_bs", &dlslime::AllToAllIntraLLBuffer::setMaxBs)
    //         .def("all_to_all_ll",
    //              &dlslime::AllToAllIntraLLBuffer::allToAllLL2D,
    //              py::arg("x"),
    //              py::arg("is_transpose") = false,
    //              py::arg("mask")         = py::none(),
    //              py::arg("offsets")      = py::none(),
    //              "AllGather with optional mask and offsets");
    // #endif
    //
    // #ifdef BUILD_INTER_OPS
    //     py::class_<dlslime::AllGatherInterLLBuffer>(m, "AllGatherInterLLBuffer")
    //         .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t>())
    //         .def(py::init<int32_t, int32_t, torch::Dtype, int32_t, int32_t, int32_t, bool>())
    //         .def("buffer_info", &dlslime::AllGatherInterLLBuffer::bufferInfo)
    //         .def("connect_full_mesh", &dlslime::AllGatherInterLLBuffer::connectFullMesh)
    //         .def("all_gather_ll", &dlslime::AllGatherInterLLBuffer::allGatherLL)
    //         .def("all_gather_ll_hook", &dlslime::AllGatherInterLLBuffer::allGatherLLHook);
    // #endif

    // =========================================================================
    // Observability (always available, independent of backend)
    // =========================================================================
    m.def("obs_enabled", &dlslime::obs::obs_enabled, "Check if observability is enabled (DLSLIME_OBS=1)");
    m.def("obs_snapshot", []() { return dlslime::obs::obs_snapshot_json(); }, "Return a dict snapshot of all obs counters");
    m.def("obs_reset_for_test", &dlslime::obs::obs_reset_for_test, "Reset all obs counters (testing only)");
}
