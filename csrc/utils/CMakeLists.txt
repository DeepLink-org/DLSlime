add_library(
    _slime_utils
    SHARED
    ibv_helper.cpp
    utils.cpp
)

target_link_libraries(_slime_utils PUBLIC ibverbs)

set_target_properties(
    _slime_utils
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_utils
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
