add_library(
    _slime_gloo_transport
    SHARED
    address.cc
    buffer.cc
    context.cc
    device.cc
    pair.cc
    unbound_buffer.cc
)

set_target_properties(
    _slime_gloo_transport
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_gloo_transport
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
