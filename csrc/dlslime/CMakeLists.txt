add_subdirectory(device)

add_subdirectory(engine)

if(BUILD_INTRA_OPS OR BUILD_INTER_OPS)
    add_subdirectory(ops)
endif()

if(USE_GLOO_BACKEND)
    add_subdirectory(gloo)
endif()
