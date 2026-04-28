#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <unistd.h>
#include "parking_violation_query.grpc.pb.h"
#include "common/config.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    void QueryByPlateId(const std::string& request_id, const std::string& plate_id, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        request.set_plate_id(plate_id);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;

        Status status = stub_->SubmitQuery(&context, request, &response);

        if (status.ok()) {
            std::cout << "Query successful. Received " << response.chunks_size() << " chunks." << std::endl;
            int total_records = 0;
            for (const auto& chunk : response.chunks()) {
                std::cout << "Chunk: " << chunk.records_size() << " records (is_last=" << chunk.is_last() << ")" << std::endl;
                for (const auto& record : chunk.records()) {
                    std::cout << "  Plate: " << record.plate_id() << ", Date: " << record.issue_date() 
                              << ", Code: " << record.violation_code() << std::endl;
                    total_records++;
                }
            }
            std::cout << "Total records: " << total_records << std::endl;
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

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