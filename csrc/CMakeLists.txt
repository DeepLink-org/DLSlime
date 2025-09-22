add_subdirectory(engine)

if (BUILD_OPS)
    add_subdirectory(ops)
endif()

if (BUILD_PYTHON)
    add_subdirectory(python)
endif()

if (USE_GLOO_BACKEND)
    add_subdirectory(gloo)
endif()

if (BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif()
