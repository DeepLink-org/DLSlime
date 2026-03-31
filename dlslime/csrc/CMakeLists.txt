add_subdirectory(device)
add_subdirectory(engine)

add_library(dlslime INTERFACE)
target_link_libraries(dlslime INTERFACE _slime_engine _slime_device)

if(BUILD_RDMA)
    target_link_libraries(dlslime INTERFACE _slime_rdma)
endif()

install(
    TARGETS dlslime
    EXPORT dlslimeTargets
)

if(BUILD_PYTHON)
    add_subdirectory(python)
endif()

if(BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif()
