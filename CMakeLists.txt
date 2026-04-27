cmake_minimum_required(VERSION 3.15)
project(ParkingViolationSystem CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---------------------------------------------------------------------------
# gRPC / Protobuf
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

# Find grpc_cpp_plugin
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
    HINTS /opt/homebrew/bin /opt/anaconda3/bin /usr/local/bin)
if(NOT GRPC_CPP_PLUGIN)
    message(FATAL_ERROR "grpc_cpp_plugin not found. Install grpc or set path manually.")
endif()
message(STATUS "grpc_cpp_plugin: ${GRPC_CPP_PLUGIN}")

add_custom_command(
    OUTPUT
        ${PROTO_GENERATED_DIR}/parking_violation_query.pb.cc
        ${PROTO_GENERATED_DIR}/parking_violation_query.pb.h
        ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.cc
        ${PROTO_GENERATED_DIR}/parking_violation_query.grpc.pb.h
    COMMAND protoc
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
target_link_libraries(parking_violation_proto ${Protobuf_LIBRARIES})
target_include_directories(parking_violation_proto PUBLIC ${PROTO_GENERATED_DIR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${PROTO_GENERATED_DIR})

# Common gRPC link libraries
set(GRPC_LIBS
    parking_violation_proto
    grpc++
    grpc
    gpr
    absl_synchronization
    ${Protobuf_LIBRARIES}
)

# Helper to apply platform link flags
macro(apply_platform_flags TARGET)
    if(APPLE)
        set_target_properties(${TARGET} PROPERTIES
            LINK_FLAGS "-Wl,-undefined,dynamic_lookup"
        )
    else()
        target_link_libraries(${TARGET} rt pthread)
    endif()
endmacro()

# ---------------------------------------------------------------------------
# load_test  (standalone: no gRPC dependency)
# ---------------------------------------------------------------------------
add_executable(load_test
    src/tools/load_test.cpp
)
target_include_directories(load_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
apply_platform_flags(load_test)

# ---------------------------------------------------------------------------
# leader_server  (Process A)
# ---------------------------------------------------------------------------
add_executable(leader_server
    src/servers/leader/leader_server.cpp
)
target_include_directories(leader_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(leader_server ${GRPC_LIBS})
apply_platform_flags(leader_server)

# ---------------------------------------------------------------------------
# team_leader_server  (Processes B, E)
# ---------------------------------------------------------------------------
add_executable(team_leader_server
    src/servers/team_leader/team_leader_server.cpp
)
target_include_directories(team_leader_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(team_leader_server ${GRPC_LIBS})
apply_platform_flags(team_leader_server)

# ---------------------------------------------------------------------------
# worker_server  (Processes C, D, F, G, H)
# ---------------------------------------------------------------------------
add_executable(worker_server
    src/servers/worker/worker_server.cpp
)
target_include_directories(worker_server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(worker_server ${GRPC_LIBS})
apply_platform_flags(worker_server)

# ---------------------------------------------------------------------------
# client
# ---------------------------------------------------------------------------
add_executable(client
    src/client/client.cpp
)
target_include_directories(client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_GENERATED_DIR}
)
target_link_libraries(client ${GRPC_LIBS})
apply_platform_flags(client)

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
message(STATUS "Targets       : load_test leader_server team_leader_server worker_server client")
message(STATUS "========================================")