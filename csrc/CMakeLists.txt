add_subdirectory(engine)

add_subdirectory(ops)

if (BUILD_PYTHON)
    add_subdirectory(python)
endif()

if (USE_GLOO_BACKEND)
    add_subdirectory(gloo)
endif()

if (BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif()
