PROJECT(arrowgentest)
CMAKE_MINIMUM_REQUIRED (VERSION 3.16.3)



# SET(Protobuf_DEBUG "1") # If you ever need to debug protobuf
SET(CMAKE_CXX_FLAGS "-g -std=c++11")

SET(Boost_USE_STATIC_LIBS ON)

SET(Protobuf_PROTOC_EXECUTABLE "/usr/bin/protoc") # Some issue with snap protoc
INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(Arrow REQUIRED)

FIND_PACKAGE(Boost COMPONENTS system filesystem unit_test_framework REQUIRED)


FILE(GLOB ProtofFiles ${CMAKE_CURRENT_SOURCE_DIR}/*.proto)
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ${ProtofFiles})

SET(PYTHON PYTHONPATH=${CMAKE_SOURCE_DIR}/.. ${CMAKE_SOURCE_DIR}/../venv/bin/python)
SET(ARROWGEN ${CMAKE_SOURCE_DIR}/..)

add_custom_command(
        OUTPUT ${CMAKE_BINARY_DIR}/simple.arrow.cc ${CMAKE_BINARY_DIR}/simple.arrow.h
        COMMAND ${PYTHON} ${ARROWGEN}/arrowgen/__main__.py ${CMAKE_SOURCE_DIR}/simple.proto
        DEPENDS ${CMAKE_SOURCE_DIR}/simple.proto
)

add_custom_target(
        NAME ALL
        COMMAND ${PYTHON} ${ARROWGEN}/tests/data_generator.py --proto_file=${CMAKE_SOURCE_DIR}/simple.proto --output_dir=${CMAKE_BINARY_DIR}/data
        DEPENDS ${CMAKE_SOURCE_DIR}/simple.proto
)


INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR})
ADD_EXECUTABLE(test_exe test.cpp ${PROTO_SRC} simple.arrow.cc)
TARGET_LINK_LIBRARIES(test_exe PRIVATE ${Protobuf_LIBRARIES} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} arrow_shared)

ENABLE_TESTING()
ADD_TEST(ArrowGenTest test_exe)