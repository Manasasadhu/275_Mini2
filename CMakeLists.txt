cmake_minimum_required(VERSION 3.15)
project(ParkingViolationSystem CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---------------------------------------------------------------------------
# gRPC + Protobuf via CMake config (brings in all transitive abseil deps)
# ---------------------------------------------------------------------------
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

message(STATUS "gRPC version  : ${gRPC_VERSION}")
message(STATUS "Protobuf ver  : ${Protobuf_VERSION}")

# ---------------------------------------------------------------------------
# Locate grpc_cpp_plugin from the gRPC CMake targets
# ---------------------------------------------------------------------------
get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION_RELEASE)
if(NOT GRPC_CPP_PLUGIN)
    get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin IMPORTED_LOCATION)
endif()
if(NOT GRPC_CPP_PLUGIN)
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
        HINTS /opt/homebrew/bin /usr/local/bin)
endif()
message(STATUS "grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")

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
    COMMAND protobuf::protoc
    ARGS --cpp_out=${PROTO_GENERATED_DIR}
         --grpc_out=${PROTO_GENERATED_DIR}
         --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
         -I${PROTO_SRC_DIR}
         ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf and gRPC C++ files"
)

add_library(parking_violation_proto
    ${PROTO_GENERATED_DIR}/parking_violation_query.pb.cc
    ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.cc
)
target_link_libraries(parking_violation_proto
    PUBLIC gRPC::grpc++ protobuf::libprotobuf
)
target_include_directories(parking_violation_proto PUBLIC ${PROTO_GENERATED_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

# Common link libraries — gRPC::grpc++ brings in ALL abseil transitive deps
set(GRPC_LIBS
    parking_violation_proto
    gRPC::grpc++
    protobuf::libprotobuf
)

# ---------------------------------------------------------------------------
# load_test  (standalone: no gRPC dependency)
# ---------------------------------------------------------------------------
add_executable(load_test
    src/tools/load_test.cpp
)
target_include_directories(load_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# ---------------------------------------------------------------------------
# leader_server  (Process A)
# ---------------------------------------------------------------------------
add_executable(leader_server
    src/servers/leader/leader_server.cpp
    src/common/metrics.cpp
)
target_include_directories(leader_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(leader_server ${GRPC_LIBS})

# ---------------------------------------------------------------------------
# team_leader_server  (Processes B, E)
# ---------------------------------------------------------------------------
add_executable(team_leader_server
    src/servers/team_leader/team_leader_server.cpp
    src/common/metrics.cpp
)
target_include_directories(team_leader_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(team_leader_server ${GRPC_LIBS})

# ---------------------------------------------------------------------------
# worker_server  (Processes C, D, F, G, H)
# ---------------------------------------------------------------------------
add_executable(worker_server
    src/servers/worker/worker_server.cpp
    src/common/metrics.cpp
)
target_include_directories(worker_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(worker_server ${GRPC_LIBS})

# ---------------------------------------------------------------------------
# client
# ---------------------------------------------------------------------------
add_executable(client
    src/client/client.cpp
    src/common/metrics.cpp
)
target_include_directories(client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(client ${GRPC_LIBS})

# ---------------------------------------------------------------------------
# Status summary
# ---------------------------------------------------------------------------
message(STATUS "========================================")
message(STATUS "Parking Violation System Build Configuration")
message(STATUS "========================================")
message(STATUS "C++ Standard  : ${CMAKE_CXX_STANDARD}")
message(STATUS "Proto dir     : ${PROTO_SRC_DIR}")
message(STATUS "Generated dir : ${PROTO_GENERATED_DIR}")
message(STATUS "Targets       : load_test leader_server team_leader_server worker_server client")
message(STATUS "========================================")