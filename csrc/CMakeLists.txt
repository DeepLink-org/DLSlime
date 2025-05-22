add_subdirectory(engine)
add_subdirectory(gloo)
add_subdirectory(utils)

if (BUILD_PYTHON)
add_subdirectory(python)
endif (BUILD_PYTHON)

if (BUILD_TORCH_PLUGIN)
add_subdirectory(torch)
endif (BUILD_TORCH_PLUGIN)
