#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <chrono>
#include <mutex>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/create_channel.h>
#include <absl/log/initialize.h>

#include "parking_violation_query.grpc.pb.h"
#include "../../common/config.hpp"
#include "../../common/metrics.hpp"
#include "../../common/thread_safe_queue.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::ClientContext;
using grpc::Status;

using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;
using parkingviolation::DelegationRequest;
using parkingviolation::DelegationResponse;
using parkingviolation::HealthRequest;
using parkingviolation::HealthResponse;
using parkingviolation::CancelRequest;
using parkingviolation::CancelResponse;

// One downstream process connection
struct DownstreamReader {
    std::string process_id;
    std::unique_ptr<ClientContext> ctx;
    std::unique_ptr<grpc::ClientReader<DelegationResponse>> reader;
    ThreadSafeQueue<DelegationResponse> buffer{32};
    std::atomic<bool> finished{false};
    Status finish_status;

    DownstreamReader() = default;
    DownstreamReader(const DownstreamReader&) = delete;
    DownstreamReader& operator=(const DownstreamReader&) = delete;
};

class TeamLeaderServiceImpl final : public ParkingViolationService::Service {
public:
    TeamLeaderServiceImpl(const ProcessConfig& cfg) : config_(cfg), pending_(0) {
        std::cout << "[TeamLeader " << config_.process_id << "] starting"
                  << " (team=" << config_.team << ")\n";
        std::cout << "[TeamLeader " << config_.process_id << "] listening on "
                  << config_.listen_host << ":" << config_.listen_port << "\n";

        // Connect to all downstream edges
        for (const auto& edge : config_.edges) {
            std::string target = edge.host + ":" + std::to_string(edge.port);
            auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
            stubs_[edge.to] = ParkingViolationService::NewStub(channel);
            std::cout << "[TeamLeader " << config_.process_id
                      << "] connected to " << edge.to << " at " << target << "\n";
        }

        metrics::init(config_.process_id, config_.role);
    }

    // -----------------------------------------------------------------------
    // DelegateQuery — receive from parent, fan out to edges, multiplex back
    // -----------------------------------------------------------------------
    Status DelegateQuery(ServerContext* context,
                         const DelegationRequest* request,
                         ServerWriter<DelegationResponse>* writer) override {

        std::cout << "\n[TeamLeader " << config_.process_id
                  << "] delegation " << request->request_id()
                  << " from " << request->delegating_process() << "\n";

        metrics::log_event("RECV_DELEGATION", request->request_id(),
                           ++pending_, (int)stubs_.size(), -1, -1,
                           request->delegating_process());

        if (stubs_.empty()) {
            std::cout << "[TeamLeader " << config_.process_id
                      << "] no downstream edges — returning empty\n";
            --pending_;
            return Status::OK;
        }

        // Build delegation to forward downstream
        DelegationRequest fwd;
        fwd.set_request_id(request->request_id());
        fwd.set_original_query(request->original_query());
        fwd.set_delegating_process(config_.process_id);

        // Open a stream to each downstream process
        std::vector<std::unique_ptr<DownstreamReader>> readers;
        for (const auto& edge : config_.edges) {
            auto it = stubs_.find(edge.to);
            if (it == stubs_.end()) continue;

            auto dr = std::make_unique<DownstreamReader>();
            dr->process_id = edge.to;
            dr->ctx    = std::make_unique<ClientContext>();
            dr->reader = it->second->DelegateQuery(dr->ctx.get(), fwd);
            readers.push_back(std::move(dr));
        }

        std::atomic<bool> cancel{false};

        // One reader thread per downstream
        std::vector<std::thread> threads;
        for (size_t i = 0; i < readers.size(); ++i) {
            threads.emplace_back([&readers, i, &cancel]() {
                DownstreamReader& dr = *readers[i];
                DelegationResponse chunk;
                while (!cancel.load() && dr.reader->Read(&chunk)) {
                    if (!dr.buffer.push(std::move(chunk))) break;
                }
                dr.finished = true;
                dr.finish_status = dr.reader->Finish();
                dr.buffer.set_finished();
                if (!dr.finish_status.ok()) {
                    std::cerr << "[TeamLeader] downstream " << dr.process_id
                              << " error: " << dr.finish_status.error_message() << "\n";
                }
            });
        }

        // Multiplex: one chunk per downstream per scan, 2ms wait
        int relay_chunks = 0;
        bool all_done = false;
        while (!all_done) {
            if (context->IsCancelled()) {
                cancel.store(true);
                for (auto& dr : readers) if (dr->ctx) dr->ctx->TryCancel();
                break;
            }

            all_done = true;
            for (auto& dr_ptr : readers) {
                DownstreamReader& dr = *dr_ptr;
                if (dr.buffer.is_finished()) continue;
                all_done = false;

                DelegationResponse chunk;
                if (dr.buffer.wait_pop_for(chunk, std::chrono::milliseconds(2))) {
                    if (!writer->Write(chunk)) {
                        cancel.store(true);
                        for (auto& r : readers) if (r->ctx) r->ctx->TryCancel();
                        for (auto& t : threads) if (t.joinable()) t.join();
                        --pending_;
                        return Status::CANCELLED;
                    }
                    metrics::log_event("RELAY_CHUNK", request->request_id(),
                                       pending_, 1, relay_chunks,
                                       chunk.records_size(), dr.process_id);
                    ++relay_chunks;
                }
            }
        }

        for (auto& t : threads) if (t.joinable()) t.join();

        std::cout << "[TeamLeader " << config_.process_id
                  << "] delegation " << request->request_id()
                  << " done, relayed " << relay_chunks << " chunks\n";

        metrics::log_event("DONE", request->request_id(),
                           pending_, 1, relay_chunks, -1, "");
        --pending_;
        ++completed_;
        return Status::OK;
    }

    Status HealthCheck(ServerContext*,
                       const HealthRequest*,
                       HealthResponse* resp) override {
        resp->set_responding_process(config_.process_id);
        resp->set_is_healthy(true);
        resp->set_pending_requests(pending_);
        resp->set_active_workers((int)stubs_.size());
        return Status::OK;
    }

    Status QueryViolations(ServerContext*,
                           const QueryRequest*,
                           ServerWriter<QueryResponse>*) override {
        return Status(grpc::StatusCode::UNIMPLEMENTED,
                      "Team leaders do not accept direct client queries");
    }

    Status CancelQuery(ServerContext*,
                       const CancelRequest* req,
                       CancelResponse* resp) override {
        resp->set_request_id(req->request_id());
        resp->set_cancelled(true);
        resp->set_message("cancel acknowledged");
        return Status::OK;
    }

private:
    ProcessConfig config_;
    std::map<std::string, std::unique_ptr<ParkingViolationService::Stub>> stubs_;
    int pending_   = 0;
    int completed_ = 0;
};

void RunTeamLeader(const std::string& config_file) {
    ProcessConfig config = ConfigParser::loadConfig(config_file);
    TeamLeaderServiceImpl service(config);

    std::string address = config.listen_host + ":" + std::to_string(config.listen_port);
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "\n*** TeamLeader " << config.process_id
              << " listening on " << address << " ***\n\n";
    server->Wait();
}

int main(int argc, char** argv) {
    absl::InitializeLog();
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>\n";
        return 1;
    }
    try {
        RunTeamLeader(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        metrics::shutdown();
        return 1;
    }
    metrics::shutdown();
    return 0;
}