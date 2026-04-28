#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <memory>
#include <atomic>
#include <chrono>
#include <map>
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

// One downstream process stream
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

// Generate a simple request ID
static std::string makeRequestId(const std::string& process_id) {
    static std::atomic<int> counter{0};
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
    return process_id + "-" + std::to_string(ms) + "-" + std::to_string(++counter);
}

class LeaderServiceImpl final : public ParkingViolationService::Service {
public:
    LeaderServiceImpl(const ProcessConfig& cfg) : config_(cfg), pending_(0) {
        std::cout << "[Leader " << config_.process_id << "] starting\n";
        std::cout << "[Leader " << config_.process_id << "] listening on "
                  << config_.listen_host << ":" << config_.listen_port << "\n";

        // Connect to all downstream edges (B, H, I)
        for (const auto& edge : config_.edges) {
            std::string target = edge.host + ":" + std::to_string(edge.port);
            auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
            stubs_[edge.to] = ParkingViolationService::NewStub(channel);
            std::cout << "[Leader] connected to " << edge.to
                      << " (" << edge.relationship << ") at " << target << "\n";
        }

        metrics::init(config_.process_id, config_.role);
    }

    // -----------------------------------------------------------------------
    // QueryViolations — the only RPC exposed to external clients
    // -----------------------------------------------------------------------
    Status QueryViolations(ServerContext* context,
                           const QueryRequest* request,
                           ServerWriter<QueryResponse>* writer) override {

        // Assign request ID if client didn't provide one
        std::string req_id = request->request_id().empty()
                             ? makeRequestId(config_.process_id)
                             : request->request_id();

        std::cout << "\n[Leader] query " << req_id << "\n";
        std::cout << "  county="    << (request->violation_county().empty() ? "(all)" : request->violation_county()) << "\n";
        std::cout << "  code="      << request->violation_code() << "\n";
        std::cout << "  dates="     << request->issue_date_start() << " to " << request->issue_date_end() << "\n";
        std::cout << "  max="       << request->max_records() << "\n";

        metrics::log_event("RECV_QUERY", req_id, ++pending_, (int)stubs_.size(), -1, -1, "");

        // Serialize query to pass downstream
        DelegationRequest del_req;
        del_req.set_request_id(req_id);
        del_req.set_delegating_process(config_.process_id);
        std::string serialized;
        request->SerializeToString(&serialized);
        del_req.set_original_query(serialized);

        // Open streams to all downstream edges
        std::vector<std::unique_ptr<DownstreamReader>> readers;
        for (const auto& edge : config_.edges) {
            auto it = stubs_.find(edge.to);
            if (it == stubs_.end()) continue;

            auto dr = std::make_unique<DownstreamReader>();
            dr->process_id = edge.to;
            dr->ctx    = std::make_unique<ClientContext>();
            dr->reader = it->second->DelegateQuery(dr->ctx.get(), del_req);
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
                    std::cerr << "[Leader] downstream " << dr.process_id
                              << " error: " << dr.finish_status.error_message() << "\n";
                }
            });
        }

        // Multiplex: one chunk per downstream per scan
        int total_chunks  = 0;
        int total_records = 0;
        bool all_done = false;

        while (!all_done) {
            if (context->IsCancelled()) {
                cancel.store(true);
                for (auto& dr : readers) if (dr->ctx) dr->ctx->TryCancel();
                break;
            }

            all_done = true;
            bool got_data = false;

            for (auto& dr_ptr : readers) {
                DownstreamReader& dr = *dr_ptr;
                if (dr.buffer.is_finished()) continue;
                all_done = false;

                DelegationResponse del_resp;
                if (dr.buffer.wait_pop_for(del_resp, std::chrono::milliseconds(2))) {
                    got_data = true;

                    // Convert DelegationResponse -> QueryResponse for client
                    QueryResponse qresp;
                    qresp.set_request_id(req_id);
                    qresp.set_chunk_number(total_chunks);
                    qresp.set_total_chunks(-1);   // unknown until final
                    qresp.set_is_final(false);
                    qresp.set_source_process(del_resp.responding_process());

                    for (const auto& rec : del_resp.records())
                        qresp.add_records()->CopyFrom(rec);

                    total_records += qresp.records_size();

                    if (!writer->Write(qresp)) {
                        cancel.store(true);
                        for (auto& r : readers) if (r->ctx) r->ctx->TryCancel();
                        for (auto& t : threads) if (t.joinable()) t.join();
                        metrics::log_event("CLIENT_DISCONNECT", req_id, pending_, 1,
                                           total_chunks, total_records, "");
                        --pending_;
                        return Status::CANCELLED;
                    }

                    metrics::log_event("CHUNK_SENT", req_id, pending_, 1,
                                       total_chunks, qresp.records_size(),
                                       dr.process_id);

                    std::cout << "[Leader] sent chunk " << total_chunks
                              << " (" << qresp.records_size() << " records"
                              << " from " << dr.process_id << ")\n";
                    ++total_chunks;
                }
            }

            if (!got_data && !all_done)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        for (auto& t : threads) if (t.joinable()) t.join();

        // Send final chunk
        QueryResponse final_resp;
        final_resp.set_request_id(req_id);
        final_resp.set_chunk_number(total_chunks);
        final_resp.set_total_chunks(total_chunks + 1);
        final_resp.set_is_final(true);
        final_resp.set_total_records(total_records);
        final_resp.set_source_process(config_.process_id);
        writer->Write(final_resp);

        std::cout << "[Leader] query " << req_id << " complete — "
                  << total_chunks << " chunks, " << total_records << " records\n";

        metrics::log_event("QUERY_DONE", req_id, pending_, 1,
                           total_chunks, total_records, "");
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

    Status DelegateQuery(ServerContext*,
                         const DelegationRequest*,
                         ServerWriter<DelegationResponse>*) override {
        return Status(grpc::StatusCode::UNIMPLEMENTED,
                      "Leader does not accept delegations");
    }

    Status CancelQuery(ServerContext*,
                       const CancelRequest* req,
                       CancelResponse* resp) override {
        std::cout << "[Leader] cancel request for " << req->request_id() << "\n";
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

void RunLeader(const std::string& config_file) {
    ProcessConfig config = ConfigParser::loadConfig(config_file);
    LeaderServiceImpl service(config);

    std::string address = config.listen_host + ":" + std::to_string(config.listen_port);
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "\n*** Leader " << config.process_id
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
        RunLeader(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        metrics::shutdown();
        return 1;
    }
    metrics::shutdown();
    return 0;
}