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
using parkingviolation::PrecinctVehicleAnalysisQuery;
using parkingviolation::UnregisteredVehicleLookupQuery;
using parkingviolation::ViolationCodeDateRangeQuery;
using parkingviolation::DateRange;

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    void QueryPlateViolationHistory(const std::string& request_id, const std::string& plate_id, int violation_code, const std::string& start_date, const std::string& end_date, const std::string& registration_state, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        
        auto* query = request.mutable_plate_violation_history();
        query->set_plate_id(plate_id);
        query->set_violation_code(violation_code);
        query->set_registration_state(registration_state);
        auto* date_range = query->mutable_date_range();
        date_range->set_start(start_date);
        date_range->set_end(end_date);
        
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        Status status = stub_->SubmitQuery(&context, request, &response);
        
        if (status.ok()) {
            int total_records = 0;
            for (const auto& chunk : response.chunks()) {
                total_records += chunk.records_size();
            }
            std::cout << "[Query 1] PlateViolationHistory: " << total_records << " records found" << std::endl;
        } else {
            std::cout << "[Query 1] RPC failed: " << status.error_message() << std::endl;
        }
    }

    void QueryPrecinctVehicleAnalysis(const std::string& request_id, const std::string& county, int precinct, int year_min, int year_max, const std::string& body_type, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        
        auto* query = request.mutable_precinct_vehicle_analysis();
        query->set_county(county);
        query->set_precinct(precinct);
        query->set_vehicle_year_min(year_min);
        query->set_vehicle_year_max(year_max);
        query->set_body_type(body_type);
        
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        Status status = stub_->SubmitQuery(&context, request, &response);
        
        if (status.ok()) {
            int total_records = 0;
            for (const auto& chunk : response.chunks()) {
                total_records += chunk.records_size();
            }
            std::cout << "[Query 2] PrecinctVehicleAnalysis: " << total_records << " records found" << std::endl;
        } else {
            std::cout << "[Query 2] RPC failed: " << status.error_message() << std::endl;
        }
    }

    void QueryUnregisteredVehicleLookup(const std::string& request_id, bool unregistered, const std::string& state, int feet_min, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        
        auto* query = request.mutable_unregistered_vehicle_lookup();
        query->set_unregistered(unregistered);
        query->set_state(state);
        query->set_feet_from_curb_min(feet_min);
        
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        Status status = stub_->SubmitQuery(&context, request, &response);
        
        if (status.ok()) {
            int total_records = 0;
            for (const auto& chunk : response.chunks()) {
                total_records += chunk.records_size();
            }
            std::cout << "[Query 3] UnregisteredVehicleLookup: " << total_records << " records found" << std::endl;
        } else {
            std::cout << "[Query 3] RPC failed: " << status.error_message() << std::endl;
        }
    }

    void QueryViolationCodeDateRange(const std::string& request_id, int violation_code, const std::string& start_date, const std::string& end_date, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* query = request.mutable_violation_code_date_range();
        query->set_violation_code(violation_code);
        query->mutable_date_range()->set_start(start_date);
        query->mutable_date_range()->set_end(end_date);
        request.set_chunk_size(chunk_size);

        QueryResponse response;
        ClientContext context;
        Status status = stub_->SubmitQuery(&context, request, &response);
        if (status.ok()) {
            int total_records = 0;
            for (const auto& chunk : response.chunks()) total_records += chunk.records_size();
            std::cout << "[" << request_id << "] ViolationCodeDateRange: " << total_records << " records found" << std::endl;
        } else {
            std::cout << "[" << request_id << "] RPC failed: " << status.error_message() << std::endl;
        }
    }

    void execute_and_print() {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        std::cout << "\n=== Executing All Queries ===\n" << std::endl;
        
        // Query 1: Narrow correctness query using plate_id and registration_state
        QueryPlateViolationHistory("q1", "JER1863", 0, "2022-01-01", "2025-12-31", "NY", 100);
        
        // Query 2: Broad heavy query by common violation code only
        QueryViolationCodeDateRange("q2", 36, "2018-07-01", "2026-06-30", 1000);
        
        // Query 3: Cross-shard distributed query over multiple worker shards
        QueryViolationCodeDateRange("q3", 21, "2023-01-01", "2025-12-31", 1000);
        
        // Query 4: Single-shard selective range aligned to one worker shard
        QueryViolationCodeDateRange("q4", 36, "2023-07-01", "2024-06-30", 1000);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        
        std::cout << "\n=== Execution Summary ===" << std::endl;
        std::cout << "Total execution time: " << duration.count() << " ms" << std::endl;
        std::cout << "==========================\n" << std::endl;
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
        client.execute_and_print();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}