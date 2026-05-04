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

static void print_top_records(const QueryResponse& response, const std::string& qtype, int top_n = 5) {
    int count = 0;
    for (const auto& chunk : response.chunks()) {
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

static QueryResponse run_query(ParkingViolationService::Stub* stub,
                               QueryRequest& request, long long& ms_out) {
    QueryResponse response;
    ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));
    auto t0 = std::chrono::high_resolution_clock::now();
    Status status = stub->SubmitQuery(&context, request, &response);
    ms_out = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - t0).count();
    if (!status.ok()) {
        std::cout << "  RPC failed (" << ms_out << " ms): " << status.error_message() << std::endl;
        return {};
    }
    return response;
}

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    void execute_and_print(int chunk_size) {
        std::cout << "\n=== Executing All Queries (chunk_size=" << chunk_size << ") ===" << std::endl;
        auto total_t0 = std::chrono::high_resolution_clock::now();
        long long ms = 0;
        int total = 0;
        QueryRequest req;
        QueryResponse res;

        // Q1: Plate violation history — narrow plate lookup spanning all shards
        req.Clear(); req.set_request_id("q1"); req.set_chunk_size(chunk_size);
        { auto* q = req.mutable_plate_violation_history();
          q->set_plate_id("JER1863"); q->set_registration_state("NY");
          q->mutable_date_range()->set_start("2022-01-01");
          q->mutable_date_range()->set_end("2025-12-31"); }
        res = run_query(stub_.get(), req, ms);
        total = 0; for (const auto& c : res.chunks()) total += c.records_size();
        std::cout << "[q1] PlateViolationHistory(plate=JER1863, state=NY): "
                  << total << " records in " << ms << " ms" << std::endl;
        print_top_records(res, "plate_violation_history");

        // Q2: Precinct vehicle analysis — multi-field cross-shard query (2022–2023)
        req.Clear(); req.set_request_id("q2"); req.set_chunk_size(chunk_size);
        { auto* q = req.mutable_precinct_vehicle_analysis();
          q->set_county("NY"); q->set_vehicle_year_min(2022); q->set_vehicle_year_max(2023); }
        res = run_query(stub_.get(), req, ms);
        total = 0; for (const auto& c : res.chunks()) total += c.records_size();
        std::cout << "[q2] PrecinctVehicleAnalysis(county=NY, year=2022-2023): "
                  << total << " records in " << ms << " ms" << std::endl;
        print_top_records(res, "precinct_vehicle_analysis");

        // Q3: Cross-shard boundary — straddles C (Jun) and D (Jul) shards
        req.Clear(); req.set_request_id("q3"); req.set_chunk_size(chunk_size);
        { auto* q = req.mutable_violation_code_date_range();
          q->set_violation_code(36);
          q->mutable_date_range()->set_start("2022-06-25");
          q->mutable_date_range()->set_end("2022-07-10"); }
        res = run_query(stub_.get(), req, ms);
        total = 0; for (const auto& c : res.chunks()) total += c.records_size();
        std::cout << "[q3] ViolationCodeDateRange(code=36, 2022-06-25 to 2022-07-10, cross-shard C+D): "
                  << total << " records in " << ms << " ms" << std::endl;
        print_top_records(res, "violation_code_date_range");

        // Q4: Single-node heavy — falls entirely in D's shard, all others return 0
        req.Clear(); req.set_request_id("q4"); req.set_chunk_size(chunk_size);
        { auto* q = req.mutable_violation_code_date_range();
          q->set_violation_code(36);
          q->mutable_date_range()->set_start("2022-07-01");
          q->mutable_date_range()->set_end("2022-07-03"); }
        res = run_query(stub_.get(), req, ms);
        total = 0; for (const auto& c : res.chunks()) total += c.records_size();
        std::cout << "[q4] ViolationCodeDateRange(code=36, 2022-07-01 to 2022-07-03, D-only): "
                  << total << " records in " << ms << " ms" << std::endl;
        print_top_records(res, "violation_code_date_range");

        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - total_t0).count();
        std::cout << "\n=== Summary: Total time " << total_ms << " ms ===\n" << std::endl;
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
    if (config_path.empty()) { std::cerr << "Missing -c" << std::endl; return 1; }
    try {
        NodeConfig a_config = load_node_config(config_path, "A");
        std::string target = a_config.host + ":" + std::to_string(a_config.port);
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(-1);
        args.SetMaxSendMessageSize(-1);
        ParkingClient client(grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args));
        client.execute_and_print(a_config.chunk_size);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl; return 1;
    }
    return 0;
}
