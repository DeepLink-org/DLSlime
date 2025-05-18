add_library(
    _slime_gloo_common
    SHARED
    linux.cc
    logging.cc
    utils.cc
)

set_target_properties(
    _slime_gloo_common
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_gloo_common
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
