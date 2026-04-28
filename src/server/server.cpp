#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <unistd.h>
#include "parking_violation_query.grpc.pb.h"
#include "common/config.hpp"
#include "common/sharding.hpp"

class ParkingServiceImpl final : public parkingviolation::ParkingViolationService::Service {
public:
    ParkingServiceImpl(const NodeConfig& config) : config_(config) {
        for (const auto& neighbor : config_.neighbors) {
            std::string neighbor_addr = "localhost:" + std::to_string(get_neighbor_port(neighbor));
            auto channel = grpc::CreateChannel(neighbor_addr, grpc::InsecureChannelCredentials());
            neighbor_stubs_[neighbor] = parkingviolation::ParkingViolationService::NewStub(channel);
            std::cout << "Initialized neighbor " << neighbor << " at " << neighbor_addr << std::endl;
        }
    }

private:
    NodeConfig config_;
    std::unordered_set<std::string> processed_requests_;
    std::map<std::string, std::unique_ptr<parkingviolation::ParkingViolationService::Stub>> neighbor_stubs_;

    int get_neighbor_port(const std::string& neighbor_id) {
        std::map<std::string, int> port_map = {
            {"A", 50051}, {"B", 50052}, {"C", 50053}, {"D", 50054}, {"E", 50055},
            {"F", 50056}, {"G", 50057}, {"H", 50058}, {"I", 50059}
        };
        return port_map[neighbor_id];
    }

    static void copy_query(const parkingviolation::QueryRequest* src,
                           parkingviolation::ForwardRequest* dst) {
        if (src->has_plate_id()) {
            dst->set_plate_id(src->plate_id());
        } else if (src->has_violation_code()) {
            dst->set_violation_code(src->violation_code());
        } else if (src->has_issue_date()) {
            dst->mutable_issue_date()->CopyFrom(src->issue_date());
        } else if (src->has_plate_violation_history()) {
            dst->mutable_plate_violation_history()->CopyFrom(src->plate_violation_history());
        } else if (src->has_precinct_vehicle_analysis()) {
            dst->mutable_precinct_vehicle_analysis()->CopyFrom(src->precinct_vehicle_analysis());
        } else if (src->has_unregistered_vehicle_lookup()) {
            dst->mutable_unregistered_vehicle_lookup()->CopyFrom(src->unregistered_vehicle_lookup());
        }
    }

    static std::string describe_query(const parkingviolation::QueryRequest* req) {
        if (req->has_plate_id())
            return "plate_id=" + req->plate_id();
        if (req->has_violation_code())
            return "violation_code=" + std::to_string(req->violation_code());
        if (req->has_issue_date())
            return "issue_date=" + req->issue_date().start() + ".." + req->issue_date().end();
        if (req->has_plate_violation_history()) {
            const auto& q = req->plate_violation_history();
            return "plate_violation_history{plate=" + q.plate_id()
                + ",code=" + std::to_string(q.violation_code())
                + ",dates=" + q.date_range().start() + ".." + q.date_range().end() + "}";
        }
        if (req->has_precinct_vehicle_analysis()) {
            const auto& q = req->precinct_vehicle_analysis();
            return "precinct_vehicle_analysis{county=" + q.county()
                + ",precinct=" + std::to_string(q.precinct())
                + ",years=" + std::to_string(q.vehicle_year_min()) + "-" + std::to_string(q.vehicle_year_max())
                + ",body=" + q.body_type() + "}";
        }
        if (req->has_unregistered_vehicle_lookup()) {
            const auto& q = req->unregistered_vehicle_lookup();
            return "unregistered_vehicle_lookup{state=" + q.state()
                + ",feet_min=" + std::to_string(q.feet_from_curb_min()) + "}";
        }
        return "unknown";
    }

    grpc::Status SubmitQuery(grpc::ServerContext* context,
                             const parkingviolation::QueryRequest* request,
                             parkingviolation::QueryResponse* response) override {
        if (!config_.client_facing) {
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not client-facing");
        }

        std::string req_id = request->request_id();
        std::cout << "[" << config_.node_id << "] Gateway received query: request_id=" << req_id
                  << ", " << describe_query(request) << std::endl;

        parkingviolation::ForwardRequest forward_req;
        forward_req.set_request_id(req_id);
        forward_req.set_from_node(config_.node_id);
        copy_query(request, &forward_req);
        forward_req.set_chunk_size(request->chunk_size());

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

        if (processed_requests_.count(req_id)) {
            std::cout << "[" << config_.node_id << "] Query " << req_id << " already processed, skipping" << std::endl;
            return grpc::Status::OK;
        }
        processed_requests_.insert(req_id);

        std::cout << "[" << config_.node_id << "] ForwardQuery received: request_id=" << req_id
                  << ", from_node=" << from_node << std::endl;

        std::string data_file = config_.data_file.empty() ? config_.global_data_file : config_.data_file;

        std::vector<parkingviolation::Chunk> all_chunks;

        // Process own shard if this is a worker node
        if (config_.shard && !data_file.empty()) {
            std::cout << "[" << config_.node_id << "] Processing own shard (worker)" << std::endl;
            auto chunks = process_query_on_shard(
                data_file,
                config_.shard->start,
                config_.shard->end,
                *request,
                config_.chunk_size,
                req_id
            );
            all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
            std::cout << "[" << config_.node_id << "] Own shard returned " << chunks.size() << " chunks" << std::endl;
        }

        // Forward to downstream neighbors
        if (config_.role == "relay" || config_.role == "gateway") {
            for (const auto& neighbor : config_.neighbors) {
                if (neighbor == from_node) continue;

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

        for (const auto& chunk : all_chunks) {
            response->add_chunks()->CopyFrom(chunk);
        }
        std::cout << "[" << config_.node_id << "] Returning " << all_chunks.size()
                  << " total chunks for request " << req_id << std::endl;

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
