file(GLOB COMMON_SRCS "common/*.cc")
file(GLOB RENDEZVOUS_SRCS "rendezvous/*.cc")
file(GLOB TRANSPORT_SRCS "transport/*.cc")
file(GLOB IBVERBS_SRCS "transport/ibverbs/*.cc")

add_library(
    _slime_gloo
    SHARED
    ${COMMON_SRCS}
    ${RENDEZVOUS_SRCS}
    ${TRANSPORT_SRCS}
    ${IBVERBS_SRCS}
    algorithm.cc
    context.cc
    allreduce.cc
    math.cc
    types.cc
)

target_link_libraries(_slime_gloo ibverbs)

set_target_properties(
    _slime_gloo
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_gloo
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
