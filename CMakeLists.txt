cmake_minimum_required(VERSION 3.15)
project(ParkingViolationSystem CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---------------------------------------------------------------------------
# gRPC / Protobuf (same search order as sample repo)
# ---------------------------------------------------------------------------
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

if(EXISTS "/opt/homebrew/include/grpc")
    set(GRPC_INCLUDE_DIR "/opt/homebrew/include")
    set(GRPC_LIB_DIR     "/opt/homebrew/lib")
elseif(EXISTS "/opt/anaconda3/include/grpc")
    set(GRPC_INCLUDE_DIR "/opt/anaconda3/include")
    set(GRPC_LIB_DIR     "/opt/anaconda3/lib")
else()
    set(GRPC_INCLUDE_DIR "/usr/local/include")
    set(GRPC_LIB_DIR     "/usr/local/lib")
endif()

message(STATUS "Using gRPC from: ${GRPC_INCLUDE_DIR}")
include_directories(${GRPC_INCLUDE_DIR})
link_directories(${GRPC_LIB_DIR})

# ---------------------------------------------------------------------------
# Proto code generation
# ---------------------------------------------------------------------------
set(PROTO_SRC_DIR       ${CMAKE_CURRENT_SOURCE_DIR}/proto)
set(PROTO_GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GENERATED_DIR})

set(PROTO_FILES ${PROTO_SRC_DIR}/parking_violation_query.proto)

add_custom_command(
    OUTPUT
        ${PROTO_GENERATED_DIR}/parking_violation_query.pb.cc
        ${PROTO_GENERATED_DIR}/parking_violation_query.pb.h
        ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.cc
        ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.h
    COMMAND protoc
    ARGS --cpp_out=${PROTO_GENERATED_DIR}
         --grpc_out=${PROTO_GENERATED_DIR}
         --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`
         -I${PROTO_SRC_DIR}
         ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf and gRPC C++ files for parking violation service"
)

add_library(parking_violation_proto
    ${PROTO_GENERATED_DIR}/parking_violation_query.pb.cc
    ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.cc
)
target_link_libraries(parking_violation_proto ${Protobuf_LIBRARIES})
target_include_directories(parking_violation_proto PUBLIC ${PROTO_GENERATED_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${PROTO_GENERATED_DIR})

set(GRPC_LIBS
    parking_violation_proto
    grpc++
    grpc
    gpr
    absl_synchronization
    ${Protobuf_LIBRARIES}
)

# ---------------------------------------------------------------------------
# load_test  (standalone: no gRPC dependency, just the CSV loader)
# ---------------------------------------------------------------------------
add_executable(load_test
    src/tools/load_test.cpp
)
# load_test only needs the C++17 standard library
target_include_directories(load_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# ---------------------------------------------------------------------------
# Platform flags (same as sample repo)
# ---------------------------------------------------------------------------
if(APPLE)
    set_target_properties(load_test PROPERTIES
        LINK_FLAGS "-Wl,-undefined,dynamic_lookup"
    )
else()
    target_link_libraries(load_test rt pthread)
endif()

# ---------------------------------------------------------------------------
# Status summary
# ---------------------------------------------------------------------------
message(STATUS "========================================")
message(STATUS "Parking Violation System Build Configuration")
message(STATUS "========================================")
message(STATUS "C++ Standard  : ${CMAKE_CXX_STANDARD}")
message(STATUS "Protobuf      : ${Protobuf_VERSION}")
message(STATUS "Proto dir     : ${PROTO_SRC_DIR}")
message(STATUS "Generated dir : ${PROTO_GENERATED_DIR}")
message(STATUS "========================================")
