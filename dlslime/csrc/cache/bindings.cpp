// bindings.cpp - pybind11 surface for dlslime.cache.
//
// Exposes a local, in-process CacheServer — enough for Python tests and
// microbenchmarks to exercise shallow metadata operations and the
// peer/version assignment directory. The network wrapper (gRPC or the
// DLSlime RPC session) lives in a separate file.
//
// Usage from Python:
//
//     from dlslime._slime_c import cache
//
//     srv = cache.CacheServer()
//     srv.store("k0",
//               [cache.Extent("peer-a", 0, 0, 8192),
//                cache.Extent("peer-a", 0, 8192, 8192)],
//               mode="shallow")
//     m = srv.load("k0")
//     print(m.mode, m.version, [(e.peer, e.offset, e.length) for e in m.extents])
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <optional>
#include <string>

#include "cache_server.h"
#include "extent.h"

namespace py = pybind11;
using dlslime::cache::CacheMode;
using dlslime::cache::CacheServer;
using dlslime::cache::CacheStats;
using dlslime::cache::Extent;
using dlslime::cache::Manifest;
using dlslime::cache::AssignmentManifest;

static CacheMode mode_from_str(const std::string& s)
{
    if (s == "deep")
        return CacheMode::Deep;
    if (s == "shallow")
        return CacheMode::Shallow;
    throw std::invalid_argument("CacheMode must be 'deep' or 'shallow', got: " + s);
}

void bind_cache(py::module_& m)
{
    py::module_ sub = m.def_submodule("cache", "DLSlimeCache primitives.");

    py::class_<Extent>(sub, "Extent")
        .def(py::init<>())
        .def(py::init([](std::string peer, uint64_t mr_handle, uint64_t offset, uint64_t length) {
                 return Extent{std::move(peer), mr_handle, offset, length};
             }),
             py::arg("peer"),
             py::arg("mr_handle"),
             py::arg("offset"),
             py::arg("length"))
        .def_readwrite("peer", &Extent::peer)
        .def_readwrite("mr_handle", &Extent::mr_handle)
        .def_readwrite("offset", &Extent::offset)
        .def_readwrite("length", &Extent::length)
        .def("__repr__", [](const Extent& e) {
            return "Extent(peer='" + e.peer + "', mr_handle=" + std::to_string(e.mr_handle)
                   + ", offset=" + std::to_string(e.offset) + ", length=" + std::to_string(e.length) + ")";
        });

    py::class_<Manifest>(sub, "Manifest")
        .def(py::init<>())
        .def_property(
            "mode",
            [](const Manifest& m) -> std::string { return m.mode == CacheMode::Deep ? "deep" : "shallow"; },
            [](Manifest& m, const std::string& s) { m.mode = mode_from_str(s); })
        .def_readwrite("extents", &Manifest::extents)
        .def_readwrite("version", &Manifest::version)
        .def("total_bytes", &Manifest::total_bytes)
        .def("__repr__", [](const Manifest& m) {
            return "Manifest(mode='" + std::string(m.mode == CacheMode::Deep ? "deep" : "shallow")
                   + "', version=" + std::to_string(m.version) + ", #extents=" + std::to_string(m.extents.size())
                   + ", total_bytes=" + std::to_string(m.total_bytes()) + ")";
        });

    py::class_<AssignmentManifest>(sub, "AssignmentManifest")
        .def(py::init<>())
        .def_readwrite("peer_agent_id", &AssignmentManifest::peer_agent_id)
        .def_readwrite("assignments", &AssignmentManifest::assignments)
        .def_readwrite("version", &AssignmentManifest::version)
        .def("total_bytes", &AssignmentManifest::total_bytes)
        .def("__repr__", [](const AssignmentManifest& m) {
            return "AssignmentManifest(peer_agent_id='" + m.peer_agent_id + "', version=" + std::to_string(m.version)
                   + ", #assignments=" + std::to_string(m.assignments.size())
                   + ", total_bytes=" + std::to_string(m.total_bytes()) + ")";
        });

    py::class_<CacheStats>(sub, "CacheStats")
        .def_readonly("num_keys", &CacheStats::num_keys)
        .def_readonly("num_shallow_keys", &CacheStats::num_shallow_keys)
        .def_readonly("num_deep_keys", &CacheStats::num_deep_keys)
        .def_readonly("num_extents", &CacheStats::num_extents)
        .def_readonly("bytes_addressed", &CacheStats::bytes_addressed)
        .def_readonly("num_assignment_peers", &CacheStats::num_assignment_peers)
        .def_readonly("num_assignment_entries", &CacheStats::num_assignment_entries)
        .def_readonly("num_assignments", &CacheStats::num_assignments)
        .def_readonly("assignment_bytes", &CacheStats::assignment_bytes)
        .def_readonly("slab_size", &CacheStats::slab_size)
        .def_readonly("memory_size", &CacheStats::memory_size)
        .def_readonly("num_slabs", &CacheStats::num_slabs)
        .def_readonly("used_slabs", &CacheStats::used_slabs)
        .def_readonly("free_slabs", &CacheStats::free_slabs)
        .def("__repr__", [](const CacheStats& s) {
            return "CacheStats(num_keys=" + std::to_string(s.num_keys)
                   + ", shallow=" + std::to_string(s.num_shallow_keys) + ", deep=" + std::to_string(s.num_deep_keys)
                   + ", extents=" + std::to_string(s.num_extents) + ", bytes=" + std::to_string(s.bytes_addressed)
                   + ", assignment_entries=" + std::to_string(s.num_assignment_entries) + ", assignments="
                   + std::to_string(s.num_assignments) + ", memory_size=" + std::to_string(s.memory_size)
                   + ", slabs=" + std::to_string(s.used_slabs) + "/" + std::to_string(s.num_slabs) + ")";
        });

    py::class_<CacheServer>(sub, "CacheServer")
        .def(py::init<uint64_t, uint64_t>(),
             py::arg("slab_size")   = CacheServer::kDefaultSlabSize,
             py::arg("memory_size") = CacheServer::kDefaultMemorySize)
        .def("slab_size", &CacheServer::slab_size)
        .def("memory_size", &CacheServer::memory_size)
        .def("num_slabs", &CacheServer::num_slabs)
        .def(
            "store",
            [](CacheServer& srv, const std::string& key, std::vector<Extent> extents, const std::string& mode) {
                return srv.store(key, std::move(extents), mode_from_str(mode));
            },
            py::arg("key"),
            py::arg("extents"),
            py::arg("mode") = "shallow",
            R"(Record `key -> extents`.

mode='shallow' records supplied extents as-is. Deep copies are not in
this metadata path; store assignment batches for the fast path.

Returns the resulting Manifest, so callers can propagate the new
version to downstream consumers without a follow-up `load`.)")
        .def(
            "store_assignments",
            [](CacheServer& srv, const std::string& peer_agent_id, dlslime::AssignmentBatch assignments) {
                return srv.store_assignments(peer_agent_id, std::move(assignments));
            },
            py::arg("peer_agent_id"),
            py::arg("assignments"),
            R"(Record a PeerAgent-owned AssignmentBatch and return its generated version.

Store-time normalization splits large assignments into slab-sized
chunks. Query by (peer_agent_id, version) to retrieve the batch.)")
        .def("query_assignments",
             &CacheServer::query_assignments,
             py::arg("peer_agent_id"),
             py::arg("version"),
             "Return the stored AssignmentManifest for (peer_agent_id, version), or None on miss.")
        .def("delete_assignments",
             &CacheServer::erase_assignments,
             py::arg("peer_agent_id"),
             py::arg("version"),
             "Drop one assignment manifest. Returns True if it existed.")
        .def("load", &CacheServer::load, py::arg("key"), "Return the stored Manifest for `key`, or None on miss.")
        .def("delete", &CacheServer::erase, py::arg("key"), "Drop the mapping for `key`. Returns True if it existed.")
        .def("stats", &CacheServer::stats, "Return a snapshot of cache-wide counters.")
        .def("clear", &CacheServer::clear, "Drop every key. Test-only; never call in production.");
}
