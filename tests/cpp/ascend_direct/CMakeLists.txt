if (BUILD_ASCEND_DIRECT)
    file(GLOB ASCEND_TOOLKIT_ROOT "/usr/local/Ascend/ascend-toolkit/latest/*-linux")
    set(ASCEND_INCLUDE_DIR "${ASCEND_TOOLKIT_ROOT}/include")

    include_directories(/usr/local/include
                        /usr/include
                        ${ASCEND_INCLUDE_DIR}
                        ${ASCEND_INCLUDE_DIR}/hccl
                        ${ASCEND_INCLUDE_DIR}/experiment
                        ${ASCEND_INCLUDE_DIR}/experiment/hccl
                        ${ASCEND_INCLUDE_DIR}/experiment/slog/toolchain
                        ${ASCEND_INCLUDE_DIR}/experiment/metadef/common/util/error_manager
                        ${ASCEND_INCLUDE_DIR}/experiment/runtime
                        ${ASCEND_INCLUDE_DIR}/experiment/msprof
                        ${ASCEND_INCLUDE_DIR}/
                        ${MPI_INCLUDE_DIR}
                        /usr/local/Ascend/ascend-toolkit/latest/tools/hccl_test/common/src/
                        /usr/local/Ascend/ascend-toolkit/latest/acllib/include/
                        )
    link_directories(/usr/local/Ascend/ascend-toolkit/latest/lib64
                    /usr/local/Ascend/ascend-toolkit/latest/acllib/lib64/
                    /usr/local/Ascend/ascend-toolkit/latest/aarch64-linux/devlib/
                    )
    add_executable(ascend_direct_perf ascend_direct_perf.cpp)
    target_link_libraries(ascend_direct_perf PUBLIC ascend_direct ascendcl gflags zmq)
endif()

