add_library(
    _slime_gloo_transport_ibverbs
    SHARED
    address.cc
    buffer.cc
    context.cc
    device.cc
    memory_region.cc
    pair.cc
    unbound_buffer.cc
)

target_link_libraries(_slime_gloo_transport_ibverbs _slime_gloo_transport _slime_gloo_common ibverbs)

set_target_properties(
    _slime_gloo_transport_ibverbs
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_gloo_transport_ibverbs
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
