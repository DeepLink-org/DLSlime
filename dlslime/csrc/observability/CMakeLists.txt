add_library(_slime_obs SHARED obs.cpp)

set_target_properties(
    _slime_obs
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

target_include_directories(_slime_obs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../..>
    $<INSTALL_INTERFACE:include/dlslime>
)

install(
    TARGETS
    _slime_obs
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
