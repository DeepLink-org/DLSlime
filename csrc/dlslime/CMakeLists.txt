add_subdirectory(device)

add_subdirectory(engine)

if(BUILD_INTRA_OPS OR BUILD_INTER_OPS)
    add_subdirectory(ops)
endif()

if(USE_GLOO_BACKEND)
    add_subdirectory(gloo)
endif()

add_library(dlslime INTERFACE)
target_link_libraries(dlslime INTERFACE _slime_engine _slime_device)

if(BUILD_RDMA)
    target_link_libraries(dlslime INTERFACE _slime_rdma)
endif()

install(
    TARGETS dlslime
    EXPORT dlslimeTargets
)
