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
using parkingviolation::PlateViolationHistoryQuery;
using parkingviolation::ViolationCodeDateRangeQuery;
using parkingviolation::DateRange;

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

    void QueryPlateViolationHistory(const std::string& request_id, const std::string& plate_id,
                                    int violation_code, const std::string& start_date,
                                    const std::string& end_date, const std::string& registration_state,
                                    int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* q = request.mutable_plate_violation_history();
        q->set_plate_id(plate_id);
        q->set_violation_code(violation_code);
        q->set_registration_state(registration_state);
        q->mutable_date_range()->set_start(start_date);
        q->mutable_date_range()->set_end(end_date);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));

        auto t0 = std::chrono::high_resolution_clock::now();
        Status status = stub_->SubmitQuery(&context, request, &response);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - t0).count();

        if (status.ok()) {
            int total = 0;
            for (const auto& chunk : response.chunks()) total += chunk.records_size();
            std::cout << "[" << request_id << "] PlateViolationHistory(plate=" << plate_id
                      << "): " << total << " records in " << ms << " ms" << std::endl;
            print_top_records(response, "plate_violation_history");
        } else {
            std::cout << "[" << request_id << "] RPC failed (" << ms << " ms): "
                      << status.error_message() << std::endl;
        }
    }

    void QueryViolationCodeDateRange(const std::string& request_id, int violation_code,
                                     const std::string& start_date, const std::string& end_date,
                                     int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* q = request.mutable_violation_code_date_range();
        q->set_violation_code(violation_code);
        q->mutable_date_range()->set_start(start_date);
        q->mutable_date_range()->set_end(end_date);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1800));

        auto t0 = std::chrono::high_resolution_clock::now();
        Status status = stub_->SubmitQuery(&context, request, &response);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - t0).count();

        if (status.ok()) {
            int total = 0;
            for (const auto& chunk : response.chunks()) total += chunk.records_size();
            std::cout << "[" << request_id << "] ViolationCodeDateRange(code=" << violation_code
                      << ", " << start_date << " to " << end_date
                      << "): " << total << " records in " << ms << " ms" << std::endl;
            print_top_records(response, "violation_code_date_range");
        } else {
            std::cout << "[" << request_id << "] RPC failed (" << ms << " ms): "
                      << status.error_message() << std::endl;
        }
    }

    void execute_and_print() {
        std::cout << "\n=== Executing All Queries ===" << std::endl;

        auto total_t0 = std::chrono::high_resolution_clock::now();

        // Query 1: Narrow correctness query using plate_id and registration_state
        QueryPlateViolationHistory("q1", "JER1863", 0, "2022-01-01", "2025-12-31", "NY", 100);

        // Query 2: Single shard - 1 month in C's range (2022 H1)
        QueryViolationCodeDateRange("q2", 36, "2022-01-01", "2022-01-31", 500);

        // Query 3: Cross-shard - spans C/D boundary (Jun-Jul 2022)
        QueryViolationCodeDateRange("q3", 36, "2022-06-25", "2022-07-10", 500);

        // Query 4: Single shard - 1 week in F's range (Python node)
        QueryViolationCodeDateRange("q4", 36, "2023-07-01", "2023-07-07", 500);

        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now() - total_t0).count();

        std::cout << "\n=== Summary ===" << std::endl;
        std::cout << "Total time: " << total_ms << " ms" << std::endl;
        std::cout << "===============\n" << std::endl;
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
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(-1);
        args.SetMaxSendMessageSize(-1);
        ParkingClient client(grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args));
        client.execute_and_print();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
