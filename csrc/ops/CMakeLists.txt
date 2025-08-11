add_library(
    _slime_ops
    SHARED
    ep_m2n.cpp
)

target_link_libraries(_slime_ops PUBLIC _slime_engine)

set_target_properties(
    _slime_ops
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_ops
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
