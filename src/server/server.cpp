#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <future>
#include <mutex>
#include <unordered_set>
#include <unordered_map>
#include <list>
#include <vector>
#include <iomanip>
#include <ctime>
#include <grpcpp/grpcpp.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <unistd.h>  // for getopt
#include "parking_violation_query.grpc.pb.h"
#include "common/config.hpp"
#include "common/sharding.hpp"

static const size_t MAX_CACHE_ENTRIES = 64;

static std::string build_cache_key(const parkingviolation::ForwardRequest* req) {
    std::string key;
    if (req->has_plate_id()) {
        key = "plate:" + req->plate_id();
    } else if (req->has_violation_code()) {
        key = "vc:" + std::to_string(req->violation_code());
    } else if (req->has_issue_date()) {
        key = "date:" + req->issue_date().start() + "~" + req->issue_date().end();
    } else if (req->has_plate_violation_history()) {
        const auto& q = req->plate_violation_history();
        key = "pvh:" + q.plate_id() + ":" + std::to_string(q.violation_code()) + ":" +
              q.date_range().start() + "~" + q.date_range().end() + ":" + q.registration_state();
    } else if (req->has_violation_code_date_range()) {
        const auto& q = req->violation_code_date_range();
        key = "vcdr:" + std::to_string(q.violation_code()) + ":" +
              q.date_range().start() + "~" + q.date_range().end();
    } else if (req->has_precinct_vehicle_analysis()) {
        const auto& q = req->precinct_vehicle_analysis();
        key = "pva:" + q.county() + ":" + std::to_string(q.precinct()) + ":" +
              std::to_string(q.vehicle_year_min()) + "~" + std::to_string(q.vehicle_year_max()) + ":" + q.body_type();
    } else if (req->has_unregistered_vehicle_lookup()) {
        const auto& q = req->unregistered_vehicle_lookup();
        key = "uvl:" + std::string(q.unregistered() ? "1" : "0") + ":" + q.state() + ":" +
              std::to_string(q.feet_from_curb_min());
    }
    key += "|cs:" + std::to_string(req->chunk_size());
    return key;
}

class ParkingServiceImpl final : public parkingviolation::ParkingViolationService::Service {
public:
    ParkingServiceImpl(const NodeConfig& config) : config_(config) {
        for (const auto& neighbor : config_.neighbors) {
            std::string neighbor_host = config_.neighbor_hosts.count(neighbor)
                ? config_.neighbor_hosts.at(neighbor) : "localhost";
            int neighbor_port = config_.neighbor_ports.count(neighbor)
                ? config_.neighbor_ports.at(neighbor) : 0;
            std::string neighbor_addr = neighbor_host + ":" + std::to_string(neighbor_port);
            grpc::ChannelArguments args;
            args.SetMaxReceiveMessageSize(-1);
            args.SetMaxSendMessageSize(-1);
            auto channel = grpc::CreateCustomChannel(neighbor_addr, grpc::InsecureChannelCredentials(), args);
            neighbor_stubs_[neighbor] = parkingviolation::ParkingViolationService::NewStub(channel);
            std::cout << "[" << timestamp() << "] Initialized neighbor " << neighbor << " at " << neighbor_addr << std::endl;
        }
    }

private:
    NodeConfig config_;
    std::mutex dedup_mutex_;
    std::unordered_set<std::string> processed_requests_;
    std::map<std::string, std::unique_ptr<parkingviolation::ParkingViolationService::Stub>> neighbor_stubs_;

    // LRU cache: key -> chunks
    std::mutex cache_mutex_;
    std::list<std::string> cache_order_;
    std::unordered_map<std::string, std::pair<std::vector<parkingviolation::Chunk>,
                       std::list<std::string>::iterator>> cache_;

    // Result store for client-side chunk retrieval (gateway only)
    std::mutex results_mutex_;
    std::unordered_map<std::string, std::vector<parkingviolation::Chunk>> result_store_;

    void cache_put(const std::string& key, const std::vector<parkingviolation::Chunk>& chunks) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            cache_order_.erase(it->second.second);
            cache_.erase(it);
        }
        if (cache_.size() >= MAX_CACHE_ENTRIES) {
            auto oldest = cache_order_.back();
            cache_order_.pop_back();
            cache_.erase(oldest);
        }
        cache_order_.push_front(key);
        cache_[key] = {chunks, cache_order_.begin()};
    }

    bool cache_get(const std::string& key, std::vector<parkingviolation::Chunk>& out) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = cache_.find(key);
        if (it == cache_.end()) return false;
        cache_order_.erase(it->second.second);
        cache_order_.push_front(key);
        it->second.second = cache_order_.begin();
        out = it->second.first;
        return true;
    }

    grpc::Status SubmitQuery(grpc::ServerContext* context,
                             const parkingviolation::QueryRequest* request,
                             parkingviolation::QueryResponse* response) override {
        if (!config_.client_facing) {
            return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Not client-facing");
        }

        std::string req_id = request->request_id();
        std::string query_type = "unknown";
        if (request->has_plate_id()) query_type = "plate_id";
        else if (request->has_violation_code()) query_type = "violation_code";
        else if (request->has_issue_date()) query_type = "issue_date";
        else if (request->has_plate_violation_history()) query_type = "plate_violation_history";
        else if (request->has_violation_code_date_range()) query_type = "violation_code_date_range";
        else if (request->has_precinct_vehicle_analysis()) query_type = "precinct_vehicle_analysis";
        else if (request->has_unregistered_vehicle_lookup()) query_type = "unregistered_vehicle_lookup";

        std::cout << "[" << timestamp() << "][" << config_.node_id << "] Gateway received query: request_id=" << req_id
                  << ", type=" << query_type << ", chunk_size=" << request->chunk_size() << std::endl;

        // Build a temporary ForwardRequest to compute cache key
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
        } else if (request->has_plate_violation_history()) {
            forward_req.mutable_plate_violation_history()->CopyFrom(request->plate_violation_history());
        } else if (request->has_violation_code_date_range()) {
            forward_req.mutable_violation_code_date_range()->CopyFrom(request->violation_code_date_range());
        } else if (request->has_precinct_vehicle_analysis()) {
            forward_req.mutable_precinct_vehicle_analysis()->CopyFrom(request->precinct_vehicle_analysis());
        } else if (request->has_unregistered_vehicle_lookup()) {
            forward_req.mutable_unregistered_vehicle_lookup()->CopyFrom(request->unregistered_vehicle_lookup());
        }
        forward_req.set_chunk_size(request->chunk_size());

        // Check gateway-level cache
        std::string cache_key = build_cache_key(&forward_req);
        std::vector<parkingviolation::Chunk> cached_chunks;
        if (cache_get(cache_key, cached_chunks)) {
            std::cout << "[" << timestamp() << "][" << config_.node_id << "] CACHE HIT at gateway for key=" << cache_key << std::endl;
            {
                std::lock_guard<std::mutex> lock(results_mutex_);
                result_store_[req_id] = cached_chunks;
            }
            response->set_total_chunks(cached_chunks.size());
            response->set_request_id(req_id);
            return grpc::Status::OK;
        }
        std::cout << "[" << timestamp() << "][" << config_.node_id << "] CACHE MISS at gateway, forwarding query" << std::endl;

        // Forward to all neighbors in parallel
        using ChunkVec = std::vector<parkingviolation::Chunk>;
        std::vector<std::future<ChunkVec>> futures;

        for (const auto& neighbor : config_.neighbors) {
            futures.push_back(std::async(std::launch::async, [this, neighbor, &forward_req]() {
                ChunkVec chunks;
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(600));
                parkingviolation::ForwardResponse fwd_response;
                grpc::Status status = neighbor_stubs_[neighbor]->ForwardQuery(&ctx, forward_req, &fwd_response);
                if (status.ok()) {
                    for (const auto& chunk : fwd_response.chunks()) chunks.push_back(chunk);
                    std::cout << "[" << timestamp() << "][" << config_.node_id << "] Received " << fwd_response.chunks_size()
                              << " chunks from " << neighbor << std::endl;
                } else {
                    std::cerr << "[" << timestamp() << "][" << config_.node_id << "] Error forwarding to " << neighbor
                              << ": " << status.error_message() << std::endl;
                }
                return chunks;
            }));
        }

        std::vector<parkingviolation::Chunk> all_result_chunks;
        for (auto& f : futures) {
            for (const auto& chunk : f.get()) all_result_chunks.push_back(chunk);
        }

        // Store in gateway cache and result store for client pull
        cache_put(cache_key, all_result_chunks);
        {
            std::lock_guard<std::mutex> lock(results_mutex_);
            result_store_[req_id] = all_result_chunks;
        }

        response->set_total_chunks(all_result_chunks.size());
        response->set_request_id(req_id);
        std::cout << "[" << timestamp() << "][" << config_.node_id << "] Gateway stored " << all_result_chunks.size()
                  << " chunks for client pull (request_id=" << req_id << ")" << std::endl;
        return grpc::Status::OK;
    }

    grpc::Status FetchChunks(grpc::ServerContext* context,
                             const parkingviolation::FetchChunksRequest* request,
                             parkingviolation::FetchChunksResponse* response) override {
        std::string req_id = request->request_id();
        int offset = request->offset();
        int limit = request->limit();

        std::lock_guard<std::mutex> lock(results_mutex_);
        auto it = result_store_.find(req_id);
        if (it == result_store_.end()) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "No results for request_id=" + req_id);
        }

        const auto& chunks = it->second;
        int total = chunks.size();
        response->set_total_chunks(total);

        int end = std::min(offset + limit, total);
        for (int i = offset; i < end; i++) {
            response->add_chunks()->CopyFrom(chunks[i]);
        }
        response->set_has_more(end < total);

        std::cout << "[" << timestamp() << "][" << config_.node_id << "] FetchChunks: request_id=" << req_id
                  << " offset=" << offset << " limit=" << limit << " returned=" << (end - offset)
                  << " has_more=" << (end < total) << std::endl;
        return grpc::Status::OK;
    }

    grpc::Status ForwardQuery(grpc::ServerContext* context,
                              const parkingviolation::ForwardRequest* request,
                              parkingviolation::ForwardResponse* response) override {
        std::string req_id = request->request_id();
        std::string from_node = request->from_node();
        std::string query_type = "unknown";
        if (request->has_plate_id()) query_type = "plate_id";
        else if (request->has_violation_code()) query_type = "violation_code";
        else if (request->has_issue_date()) query_type = "issue_date";
        else if (request->has_plate_violation_history()) query_type = "plate_violation_history";
        else if (request->has_violation_code_date_range()) query_type = "violation_code_date_range";
        else if (request->has_precinct_vehicle_analysis()) query_type = "precinct_vehicle_analysis";
        else if (request->has_unregistered_vehicle_lookup()) query_type = "unregistered_vehicle_lookup";

        {
            std::lock_guard<std::mutex> lock(dedup_mutex_);
            if (processed_requests_.count(req_id)) {
                std::cout << "[" << timestamp() << "][" << config_.node_id << "] Query " << req_id << " already processed, skipping" << std::endl;
                return grpc::Status::OK;
            }
            processed_requests_.insert(req_id);
        }

        std::cout << "[" << timestamp() << "][" << config_.node_id << "] ForwardQuery received: request_id=" << req_id
                  << ", from_node=" << from_node << ", type=" << query_type
                  << ", neighbors=" << config_.neighbors.size() << std::endl;

        // Check node-level cache
        std::string cache_key = build_cache_key(request);
        std::vector<parkingviolation::Chunk> cached_chunks;
        if (cache_get(cache_key, cached_chunks)) {
            std::cout << "[" << timestamp() << "][" << config_.node_id << "] CACHE HIT for key=" << cache_key << std::endl;
            for (const auto& chunk : cached_chunks) response->add_chunks()->CopyFrom(chunk);
            return grpc::Status::OK;
        }

        std::string data_file = config_.data_file.empty() ? config_.global_data_file : config_.data_file;

        // Step 1: Process own shard
        std::vector<parkingviolation::Chunk> all_chunks;
        if (config_.shard && !data_file.empty()) {
            std::cout << "[" << timestamp() << "][" << config_.node_id << "] Processing own shard (worker)" << std::endl;
            int chunk_size = request->chunk_size() > 0 ? request->chunk_size() : config_.chunk_size;
            auto chunks = process_query_on_shard(
                data_file, config_.shard->start, config_.shard->end, request, chunk_size);
            all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
            std::cout << "[" << timestamp() << "][" << config_.node_id << "] Own shard returned " << chunks.size() << " chunks" << std::endl;
        }

        // Step 2: Forward to neighbors in parallel (relay/gateway only)
        if (config_.role == "relay" || config_.role == "gateway") {
            using ChunkVec = std::vector<parkingviolation::Chunk>;
            std::vector<std::future<ChunkVec>> futures;

            for (const auto& neighbor : config_.neighbors) {
                if (neighbor == from_node) {
                    std::cout << "[" << timestamp() << "][" << config_.node_id << "] Skipping " << neighbor << " (query came from here)" << std::endl;
                    continue;
                }
                std::cout << "[" << timestamp() << "][" << config_.node_id << "] Forwarding to neighbor " << neighbor << std::endl;
                futures.push_back(std::async(std::launch::async, [this, neighbor, request]() {
                    ChunkVec chunks;
                    grpc::ClientContext ctx;
                    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(600));
                    parkingviolation::ForwardResponse fwd_response;
                    grpc::Status status = neighbor_stubs_[neighbor]->ForwardQuery(&ctx, *request, &fwd_response);
                    if (status.ok()) {
                        for (const auto& chunk : fwd_response.chunks()) chunks.push_back(chunk);
                        std::cout << "[" << timestamp() << "][" << config_.node_id << "] Collected " << fwd_response.chunks_size()
                                  << " chunks from " << neighbor << std::endl;
                    } else {
                        std::cerr << "[" << timestamp() << "][" << config_.node_id << "] Error forwarding to " << neighbor
                                  << ": " << status.error_message() << std::endl;
                    }
                    return chunks;
                }));
            }

            for (auto& f : futures) {
                auto chunks = f.get();
                all_chunks.insert(all_chunks.end(), chunks.begin(), chunks.end());
            }
        }

        // Store in cache
        cache_put(cache_key, all_chunks);
        std::cout << "[" << timestamp() << "][" << config_.node_id << "] CACHE STORE for key=" << cache_key << std::endl;

        // Step 3: Return aggregated results
        for (const auto& chunk : all_chunks) {
            response->add_chunks()->CopyFrom(chunk);
        }
        std::cout << "[" << timestamp() << "][" << config_.node_id << "] Returning " << all_chunks.size()
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
    std::string server_address = config.listen_host + ":" + std::to_string(config.port);
    ParkingServiceImpl service(config);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "[" << timestamp() << "] Server listening on " << server_address << std::endl;
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
