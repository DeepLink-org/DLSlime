add_library(
    _slime_gloo_rendezvous
    SHARED
    context.cc
    file_store.cc
    hash_store.cc
    prefix_store.cc
    store.cc
)

target_link_libraries(_slime_gloo_rendezvous pthread)

set_target_properties(
    _slime_gloo_rendezvous
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_gloo_rendezvous
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
