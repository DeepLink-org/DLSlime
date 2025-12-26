add_subdirectory(dlslime)

if(BUILD_PYTHON)
    add_subdirectory(python)
endif()

if(BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif()
