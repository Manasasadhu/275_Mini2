#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <unistd.h>  // for getopt
#include "parking_violation_query.grpc.pb.h"
#include "config.hpp"

class ParkingServiceImpl final : public parkingviolation::ParkingViolationService::Service {
public:
    ParkingServiceImpl(const NodeConfig& config) : config_(config) {
        // Load data if has shard
        if (config_.shard) {
            // load_csv(config_.data_file);
        }
    }

private:
    NodeConfig config_;
    std::unordered_set<std::string> processed_requests_; // for dedup

    grpc::Status SubmitQuery(grpc::ServerContext* context,
                             const parkingviolation::QueryRequest* request,
                             parkingviolation::QueryResponse* response) override {
        if (!config_.client_facing) {
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not client-facing");
        }
        // Process as gateway: forward to neighbors, aggregate
        // Pseudocode: aggregate_chunks = forward_to_neighbors(request)
        // for chunk in aggregate_chunks: response.add_chunks()->CopyFrom(chunk)
        return grpc::Status::OK;
    }

    grpc::Status ForwardQuery(grpc::ServerContext* context,
                              const parkingviolation::ForwardRequest* request,
                              parkingviolation::ForwardResponse* response) override {
        std::string req_id = request->request_id();
        if (processed_requests_.count(req_id)) {
            // Dedup: already processed
            return grpc::Status::OK;
        }
        processed_requests_.insert(req_id);

        // Forward to neighbors except from_node
        std::vector<parkingviolation::Chunk> all_chunks;
        for (const auto& neighbor : config_.neighbors) {
            if (neighbor == request->from_node()) continue;
            // send ForwardRequest to neighbor, collect chunks
            // all_chunks.insert(all_chunks.end(), neighbor_chunks.begin(), neighbor_chunks.end());
        }

        // If has data, process own shard
        if (config_.shard) {
            // chunks = process_query_on_shard(request->query, config_.data_file, *config_.shard, request->chunk_size)
            // all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
        }

        // Send back
        for (const auto& chunk : all_chunks) {
            response->add_chunks()->CopyFrom(chunk);
        }
        return grpc::Status::OK;
    }

    grpc::Status CancelQuery(grpc::ServerContext* context,
                             const parkingviolation::CancelRequest* request,
                             parkingviolation::CancelResponse* response) override {
        // Implement cancel logic
        response->set_success(true);
        return grpc::Status::OK;
    }

    grpc::Status HealthCheck(grpc::ServerContext* context,
                             const parkingviolation::HealthRequest* request,
                             parkingviolation::HealthResponse* response) override {
        response->set_healthy(true);
        return grpc::Status::OK;
    }
};

void RunServer(const NodeConfig& config) {
    std::string server_address = config.host + ":" + std::to_string(config.port);
    ParkingServiceImpl service(config);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    std::string node_id, config_path;
    int opt;
    while ((opt = getopt(argc, argv, "n:c:")) != -1) {
        switch (opt) {
            case 'n': node_id = optarg; break;
            case 'c': config_path = optarg; break;
            default: std::cerr << "Usage: " << argv[0] << " -n <node_id> -c <config_path>" << std::endl; return 1;
        }
    }
    if (node_id.empty() || config_path.empty()) {
        std::cerr << "Missing -n or -c" << std::endl; return 1;
    }
    try {
        NodeConfig config = load_node_config(config_path, node_id);
        RunServer(config);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}