file(GLOB ASCEND_SOURCES "*.cpp")

add_library(ascend_direct OBJECT ${ASCEND_SOURCES})
target_link_libraries(ascend_direct PUBLIC _slime_engine)