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
#include "common/config.hpp"
#include "common/sharding.hpp"

class ParkingServiceImpl final : public parkingviolation::ParkingViolationService::Service {
public:
    ParkingServiceImpl(const NodeConfig& config) : config_(config) {
        // Initialize gRPC stubs to all neighbors
        for (const auto& neighbor : config_.neighbors) {
            std::string neighbor_addr = "localhost:" + std::to_string(get_neighbor_port(neighbor));
            auto channel = grpc::CreateChannel(neighbor_addr, grpc::InsecureChannelCredentials());
            neighbor_stubs_[neighbor] = parkingviolation::ParkingViolationService::NewStub(channel);
            std::cout << "Initialized neighbor " << neighbor << " at " << neighbor_addr << std::endl;
        }
    }

private:
    NodeConfig config_;
    std::unordered_set<std::string> processed_requests_; // for dedup
    std::map<std::string, std::unique_ptr<parkingviolation::ParkingViolationService::Stub>> neighbor_stubs_;

    // Get port number for a neighbor node
    int get_neighbor_port(const std::string& neighbor_id) {
        // Hardcoded mapping (would be better to read from config, but keeping simple for now)
        std::map<std::string, int> port_map = {
            {"A", 50051}, {"B", 50052}, {"C", 50053}, {"D", 50054}, {"E", 50055},
            {"F", 50056}, {"G", 50057}, {"H", 50058}, {"I", 50059}
        };
        return port_map[neighbor_id];
    }

    grpc::Status SubmitQuery(grpc::ServerContext* context,
                             const parkingviolation::QueryRequest* request,
                             parkingviolation::QueryResponse* response) override {
        if (!config_.client_facing) {
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not client-facing");
        }
        
        std::string req_id = request->request_id();
        std::string plate_id = request->has_plate_id() ? request->plate_id() : "";
        
        std::cout << "[" << config_.node_id << "] Gateway received query: request_id=" << req_id 
                  << ", plate_id=" << plate_id << std::endl;
        
        // Convert QueryRequest to ForwardRequest
        parkingviolation::ForwardRequest forward_req;
        forward_req.set_request_id(req_id);
        forward_req.set_from_node(config_.node_id);
        if (request->has_plate_id()) {
            forward_req.set_plate_id(request->plate_id());
        } else if (request->has_violation_code()) {
            forward_req.set_violation_code(request->violation_code());
        } else if (request->has_issue_date()) {
            forward_req.mutable_issue_date()->set_start(request->issue_date().start());
            forward_req.mutable_issue_date()->set_end(request->issue_date().end());
        }
        forward_req.set_chunk_size(request->chunk_size());
        
        // Forward to all neighbors and aggregate results
        std::vector<parkingviolation::Chunk> all_chunks;
        for (const auto& neighbor : config_.neighbors) {
            std::cout << "[" << config_.node_id << "] Forwarding query to neighbor " << neighbor << std::endl;
            
            grpc::ClientContext ctx;
            parkingviolation::ForwardResponse fwd_response;
            grpc::Status status = neighbor_stubs_[neighbor]->ForwardQuery(&ctx, forward_req, &fwd_response);
            
            if (status.ok()) {
                std::cout << "[" << config_.node_id << "] Received " << fwd_response.chunks_size() 
                          << " chunks from " << neighbor << std::endl;
                for (const auto& chunk : fwd_response.chunks()) {
                    response->add_chunks()->CopyFrom(chunk);
                }
            } else {
                std::cerr << "[" << config_.node_id << "] Error forwarding to " << neighbor 
                          << ": " << status.error_message() << std::endl;
            }
        }
        
        std::cout << "[" << config_.node_id << "] Gateway response contains " << response->chunks_size() 
                  << " total chunks" << std::endl;
        return grpc::Status::OK;
    }

    grpc::Status ForwardQuery(grpc::ServerContext* context,
                              const parkingviolation::ForwardRequest* request,
                              parkingviolation::ForwardResponse* response) override {
        std::string req_id = request->request_id();
        std::string from_node = request->from_node();
        
        // Deduplication check
        if (processed_requests_.count(req_id)) {
            std::cout << "[" << config_.node_id << "] Query " << req_id << " already processed, skipping" << std::endl;
            return grpc::Status::OK;
        }
        processed_requests_.insert(req_id);
        
        std::cout << "[" << config_.node_id << "] ForwardQuery received: request_id=" << req_id 
                  << ", from_node=" << from_node << std::endl;

        // Get data file from config
        std::string data_file = config_.data_file.empty() ? config_.global_data_file : config_.data_file;
        
        // Step 1: Process own shard if this is a worker node
        std::vector<parkingviolation::Chunk> all_chunks;
        if (config_.shard && !data_file.empty()) {
            std::string plate_id = request->has_plate_id() ? request->plate_id() : "";
            std::cout << "[" << config_.node_id << "] Processing own shard (worker) for plate: " << plate_id << std::endl;
            
            auto chunks = process_query_on_shard(
                data_file,
                config_.shard->start,
                config_.shard->end,
                plate_id,
                config_.chunk_size,
                req_id
            );
            all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
            std::cout << "[" << config_.node_id << "] Own shard returned " << chunks.size() << " chunks" << std::endl;
        }
        
        // Step 2: Forward to downstream neighbors (relays: B and E)
        if (config_.role == "relay" || config_.role == "gateway") {
            std::cout << "[" << config_.node_id << "] Node is relay/gateway, forwarding to neighbors..." << std::endl;
            
            for (const auto& neighbor : config_.neighbors) {
                // Don't forward back to the node that sent us this query
                if (neighbor == from_node) {
                    std::cout << "[" << config_.node_id << "] Skipping " << neighbor << " (query came from here)" << std::endl;
                    continue;
                }
                
                std::cout << "[" << config_.node_id << "] Forwarding to neighbor " << neighbor << std::endl;
                
                grpc::ClientContext ctx;
                parkingviolation::ForwardResponse fwd_response;
                grpc::Status status = neighbor_stubs_[neighbor]->ForwardQuery(&ctx, *request, &fwd_response);
                
                if (status.ok()) {
                    for (const auto& chunk : fwd_response.chunks()) {
                        all_chunks.push_back(chunk);
                    }
                    std::cout << "[" << config_.node_id << "] Collected " << fwd_response.chunks_size() 
                              << " chunks from " << neighbor << std::endl;
                } else {
                    std::cerr << "[" << config_.node_id << "] Error forwarding to " << neighbor 
                              << ": " << status.error_message() << std::endl;
                }
            }
        }

        // Step 3: Return aggregated results
        for (const auto& chunk : all_chunks) {
            response->add_chunks()->CopyFrom(chunk);
        }
        std::cout << "[" << config_.node_id << "] Returning " << all_chunks.size() << " total chunks for request " << req_id << std::endl;
        
        return grpc::Status::OK;
    }

    grpc::Status CancelQuery(grpc::ServerContext* context,
                             const parkingviolation::CancelRequest* request,
                             parkingviolation::CancelResponse* response) override {
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