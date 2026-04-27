#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "parking_violation_query.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;
using parkingviolation::DateRange;

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    // Example: Query by plate_id
    void QueryByPlateId(const std::string& request_id, const std::string& plate_id, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        request.set_plate_id(plate_id);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;

        Status status = stub_->SubmitQuery(&context, request, &response);

        if (status.ok()) {
            for (const auto& chunk : response.chunks()) {
                std::cout << "Chunk for request " << chunk.request_id() << ":" << std::endl;
                for (const auto& record : chunk.records()) {
                    std::cout << "Plate: " << record.plate_id() << ", Date: " << record.issue_date() << std::endl;
                }
                if (chunk.is_last()) {
                    std::cout << "Last chunk." << std::endl;
                }
            }
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

    // Add methods for other query types: violation_code, issue_date_range

private:
    std::unique_ptr<ParkingViolationService::Stub> stub_;
};

#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <unistd.h>  // for getopt
#include "parking_violation_query.grpc.pb.h"
#include "config.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;
using parkingviolation::DateRange;

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    // Example: Query by plate_id
    void QueryByPlateId(const std::string& request_id, const std::string& plate_id, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        request.set_plate_id(plate_id);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;

        Status status = stub_->SubmitQuery(&context, request, &response);

        if (status.ok()) {
            for (const auto& chunk : response.chunks()) {
                std::cout << "Chunk for request " << chunk.request_id() << ":" << std::endl;
                for (const auto& record : chunk.records()) {
                    std::cout << "Plate: " << record.plate_id() << ", Date: " << record.issue_date() << std::endl;
                }
                if (chunk.is_last()) {
                    std::cout << "Last chunk." << std::endl;
                }
            }
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

    // Add methods for other query types: violation_code, issue_date_range

private:
    std::unique_ptr<ParkingViolationService::Stub> stub_;
};

int main(int argc, char** argv) {
    std::string config_path;
    int opt;
    while ((opt = getopt(argc, argv, "c:")) != -1) {
        if (opt == 'c') config_path = optarg;
        else { std::cerr << "Usage: " << argv[0] << " -c <config_path>" << std::endl; return 1; }
    }
    if (config_path.empty()) {
        std::cerr << "Missing -c" << std::endl; return 1;
    }
    try {
        NodeConfig a_config = load_node_config(config_path, "A");
        std::string target = a_config.host + ":" + std::to_string(a_config.port);
        ParkingClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
        // Example usage
        client.QueryByPlateId("req1", "ABC123", 100);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}