add_subdirectory(engine)
add_subdirectory(gloo)
add_subdirectory(utils)

if (BUILD_PYTHON)
add_subdirectory(python)
endif (BUILD_PYTHON)
