add_library(
    _slime_gloo
    SHARED
    context.cc
)

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

add_subdirectory(common)
add_subdirectory(rendezvous)
add_subdirectory(transport)
