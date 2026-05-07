#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <unistd.h>
#include <chrono>
#include "parking_violation_query.grpc.pb.h"
#include "common/config.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;
using parkingviolation::FetchChunksRequest;
using parkingviolation::FetchChunksResponse;

static void print_top_records(const std::vector<parkingviolation::Chunk>& chunks, const std::string& qtype, int top_n = 5) {
    int count = 0;
    for (const auto& chunk : chunks) {
        for (const auto& rec : chunk.records()) {
            if (count >= top_n) break;
            if (count == 0) std::cout << "  Top " << top_n << " results:" << std::endl;
            if (qtype == "plate_violation_history") {
                std::cout << "  [" << (count+1) << "] plate=" << rec.plate_id()
                          << " date=" << rec.issue_date()
                          << " code=" << rec.violation_code()
                          << " state=" << rec.registration_state()
                          << " county=" << rec.violation_county() << std::endl;
            } else if (qtype == "violation_code_date_range") {
                std::cout << "  [" << (count+1) << "] plate=" << rec.plate_id()
                          << " date=" << rec.issue_date()
                          << " county=" << rec.violation_county()
                          << " make=" << rec.vehicle_make()
                          << " fine=$" << rec.fine_amount() << std::endl;
            } else if (qtype == "precinct_vehicle_analysis") {
                std::cout << "  [" << (count+1) << "] plate=" << rec.plate_id()
                          << " date=" << rec.issue_date()
                          << " body=" << rec.vehicle_body_type()
                          << " year=" << rec.vehicle_year()
                          << " county=" << rec.violation_county() << std::endl;
            }
            count++;
        }
        if (count >= top_n) break;
    }
    if (count == 0) std::cout << "  (no records returned)" << std::endl;
}

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    void execute_and_print(int chunk_size, int fetch_batch_size) {
        std::cout << "\n=== Executing All Queries (chunk_size=" << chunk_size
                  << ", fetch_batch=" << fetch_batch_size << ") ===" << std::endl;
        auto total_t0 = std::chrono::high_resolution_clock::now();

        // Q1: Plate violation history
        {
            QueryRequest req;
            req.set_request_id("q1"); req.set_chunk_size(chunk_size);
            auto* q = req.mutable_plate_violation_history();
            q->set_plate_id("JER1863"); q->set_registration_state("NY");
            q->mutable_date_range()->set_start("2022-01-01");
            q->mutable_date_range()->set_end("2025-12-31");
            run_query_with_fetch(req, "plate_violation_history", fetch_batch_size);
        }

        // Q2: Precinct vehicle analysis
        {
            QueryRequest req;
            req.set_request_id("q2"); req.set_chunk_size(chunk_size);
            auto* q = req.mutable_precinct_vehicle_analysis();
            q->set_county("NY"); q->set_vehicle_year_min(2022); q->set_vehicle_year_max(2023);
            run_query_with_fetch(req, "precinct_vehicle_analysis", fetch_batch_size);
        }

        // Q3: Cross-shard boundary
        {
            QueryRequest req;
            req.set_request_id("q3"); req.set_chunk_size(chunk_size);
            auto* q = req.mutable_violation_code_date_range();
            q->set_violation_code(36);
            q->mutable_date_range()->set_start("2022-06-25");
            q->mutable_date_range()->set_end("2022-07-10");
            run_query_with_fetch(req, "violation_code_date_range", fetch_batch_size);
        }

        // Q4: Single-node heavy
        {
            QueryRequest req;
            req.set_request_id("q4"); req.set_chunk_size(chunk_size);
            auto* q = req.mutable_violation_code_date_range();
            q->set_violation_code(36);
            q->mutable_date_range()->set_start("2022-07-01");
            q->mutable_date_range()->set_end("2022-07-03");
            run_query_with_fetch(req, "violation_code_date_range", fetch_batch_size);
        }

        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - total_t0).count();
        std::cout << "\n=== Summary: Total time " << total_ms << " ms ===\n" << std::endl;
    }

private:
    std::unique_ptr<ParkingViolationService::Stub> stub_;

    void run_query_with_fetch(QueryRequest& request, const std::string& qtype, int fetch_batch) {
        auto t0 = std::chrono::high_resolution_clock::now();

        // Step 1: Submit query — gateway processes and stores results
        QueryResponse submit_response;
        {
            ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));
            Status status = stub_->SubmitQuery(&context, request, &submit_response);
            if (!status.ok()) {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::high_resolution_clock::now() - t0).count();
                std::cout << "  [" << request.request_id() << "] RPC failed (" << ms << " ms): "
                          << status.error_message() << std::endl;
                return;
            }
        }

        auto submit_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - t0).count();

        int total_chunks = submit_response.total_chunks();
        std::string req_id = submit_response.request_id();

        std::cout << "[" << request.request_id() << "] SubmitQuery complete: " << total_chunks
                  << " chunks available (" << submit_ms << " ms)" << std::endl;

        // Step 2: Pull chunks on demand in batches
        std::vector<parkingviolation::Chunk> all_chunks;
        int offset = 0;
        int fetch_calls = 0;

        while (offset < total_chunks) {
            FetchChunksRequest fetch_req;
            fetch_req.set_request_id(req_id);
            fetch_req.set_offset(offset);
            fetch_req.set_limit(fetch_batch);

            FetchChunksResponse fetch_response;
            ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
            Status status = stub_->FetchChunks(&ctx, fetch_req, &fetch_response);

            if (!status.ok()) {
                std::cerr << "  FetchChunks failed at offset=" << offset << ": "
                          << status.error_message() << std::endl;
                break;
            }

            for (const auto& chunk : fetch_response.chunks()) {
                all_chunks.push_back(chunk);
            }
            offset += fetch_response.chunks_size();
            fetch_calls++;

            if (!fetch_response.has_more()) break;
        }

        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - t0).count();

        int total_records = 0;
        for (const auto& c : all_chunks) total_records += c.records_size();

        std::cout << "[" << request.request_id() << "] Fetched " << all_chunks.size()
                  << " chunks (" << total_records << " records) in " << fetch_calls
                  << " fetch calls, total " << total_ms << " ms" << std::endl;
        print_top_records(all_chunks, qtype);
    }
};

int main(int argc, char** argv) {
    std::string config_path;
    int fetch_batch = 5;
    int opt;
    while ((opt = getopt(argc, argv, "c:f:")) != -1) {
        switch (opt) {
            case 'c': config_path = optarg; break;
            case 'f': fetch_batch = std::stoi(optarg); break;
            default:
                std::cerr << "Usage: " << argv[0] << " -c <config_path> [-f <fetch_batch_size>]" << std::endl;
                return 1;
        }
    }
    if (config_path.empty()) { std::cerr << "Missing -c" << std::endl; return 1; }
    try {
        NodeConfig a_config = load_node_config(config_path, "A");
        std::string target = a_config.host + ":" + std::to_string(a_config.port);
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(-1);
        args.SetMaxSendMessageSize(-1);
        ParkingClient client(grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args));
        client.execute_and_print(a_config.chunk_size, fetch_batch);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl; return 1;
    }
    return 0;
}
