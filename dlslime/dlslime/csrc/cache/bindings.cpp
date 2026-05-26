// bindings.cpp - pybind11 surface for dlslime.cache.
//
// Exposes a local, in-process CacheServer — enough for Python tests,
// microbenchmarks, and the Python HTTP cache service to exercise the
// peer/version assignment directory. Assignment manifests point at bytes
// that have already been written into the cache service's registered MR.
//
// Usage from Python:
//
//     from dlslime._slime_c import cache
//
//     srv = cache.CacheServer()
//     m = srv.store_assignments("engine-a", [Assignment(1, 2, 0, 0, 8192)])
//     print(m.peer_agent_id, m.version, m.total_bytes())
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <optional>
#include <string>

#include "cache_server.h"

namespace py = pybind11;
using dlslime::cache::AssignmentManifest;
using dlslime::cache::CacheServer;
using dlslime::cache::CacheStats;

void bind_cache(py::module_& m)
{
    py::module_ sub = m.def_submodule("cache", "DLSlimeCache primitives.");

    py::class_<AssignmentManifest>(sub, "AssignmentManifest")
        .def(py::init<>())
        .def_readwrite("peer_agent_id", &AssignmentManifest::peer_agent_id)
        .def_readwrite("assignments", &AssignmentManifest::assignments)
        .def_readwrite("slab_ids", &AssignmentManifest::slab_ids)
        .def_readwrite("version", &AssignmentManifest::version)
        .def("total_bytes", &AssignmentManifest::total_bytes)
        .def("__repr__", [](const AssignmentManifest& m) {
            return "AssignmentManifest(peer_agent_id='" + m.peer_agent_id + "', version=" + std::to_string(m.version)
                   + ", #assignments=" + std::to_string(m.assignments.size()) + ", #slabs="
                   + std::to_string(m.slab_ids.size()) + ", total_bytes=" + std::to_string(m.total_bytes()) + ")";
        });

    py::class_<CacheStats>(sub, "CacheStats")
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
            return "CacheStats(assignment_entries=" + std::to_string(s.num_assignment_entries) + ", assignments="
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
            "store_assignments",
            [](CacheServer& srv, const std::string& peer_agent_id, dlslime::AssignmentBatch assignments) {
                return srv.store_assignments(peer_agent_id, std::move(assignments));
            },
            py::arg("peer_agent_id"),
            py::arg("assignments"),
            R"(Record a PeerAgent-owned AssignmentBatch and return its generated version.

Store-time normalization splits large assignments into slab-sized
chunks, allocates slab ids, and rewrites cache-side source offsets into
the service's registered cache MR. Query by (peer_agent_id, version) to
retrieve the batch.)")
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
        .def("stats", &CacheServer::stats, "Return a snapshot of cache-wide counters.")
        .def("clear", &CacheServer::clear, "Drop every key. Test-only; never call in production.");
}
