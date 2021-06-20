SET(CMAKE_CXX_FLAGS "-g -std=c++11")
SET(Boost_USE_STATIC_LIBS ON)

#SET(Protobuf_DEBUG "1") # If you ever need to debug protobuf
SET(Protobuf_INCLUDE_DIR "/usr/local/include")
SET(Protobuf_PROTOC_EXECUTABLE "/usr/local/bin/protoc") # Some issue with snap protoc

execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE} --version
        OUTPUT_VARIABLE _PROTOBUF_PROTOC_EXECUTABLE_VERSION)

MESSAGE(${Protobuf_PROTOC_EXECUTABLE} --version)
MESSAGE("${_PROTOBUF_PROTOC_EXECUTABLE_VERSION} FOOOOOO")

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
FIND_PACKAGE(Arrow REQUIRED)

FIND_PACKAGE(Boost COMPONENTS system filesystem unit_test_framework REQUIRED)


FILE(GLOB ProtofFiles ${CMAKE_CURRENT_SOURCE_DIR}/*.proto)
FILE(GLOB PythonFiles ${CMAKE_SOURCE_DIR}/arrowgen/*.py)
FILE(GLOB TemplateFiles ${CMAKE_SOURCE_DIR}/arrowgen/templates/*)

PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER ${ProtofFiles})

SET(PYTHON PYTHONPATH=${CMAKE_SOURCE_DIR}/ ${CMAKE_SOURCE_DIR}/venv/bin/python)
SET(ARROWGEN ${CMAKE_SOURCE_DIR}/)

ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/simple.arrow.cc ${CMAKE_CURRENT_BINARY_DIR}/simple.arrow.h
        COMMAND ${PYTHON} ${ARROWGEN}/arrowgen/__main__.py ${CMAKE_CURRENT_SOURCE_DIR}/simple.proto
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/simple.proto ${PythonFiles} ${TemplateFiles}
)


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR})

ADD_EXECUTABLE(
        test_exe
        ${CMAKE_CURRENT_SOURCE_DIR}/test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/arrow_example_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/learning_test.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/arrow_example.cpp
        ${PROTO_SRC}
        ${CMAKE_CURRENT_BINARY_DIR}/simple.arrow.cc
        )
TARGET_LINK_LIBRARIES(test_exe PRIVATE ${Protobuf_LIBRARIES} ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY} arrow_shared)


ADD_CUSTOM_COMMAND(
        TARGET test_exe POST_BUILD
        COMMAND ${PYTHON} ${ARROWGEN}/tests/data_generator.py ${CMAKE_CURRENT_SOURCE_DIR}/simple.proto --output_dir=${CMAKE_CURRENT_BINARY_DIR}/data
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/simple.proto ${PythonFiles} ${TemplateFiles}
)


ENABLE_TESTING()
ADD_TEST(ArrowGenTest test_exe)