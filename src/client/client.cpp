#include <iostream>
#include <memory>
#include <string>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <unistd.h>
#include "parking_violation_query.grpc.pb.h"
#include "common/config.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;

class ParkingClient {
public:
    ParkingClient(std::shared_ptr<Channel> channel)
        : stub_(ParkingViolationService::NewStub(channel)) {}

    void QueryByPlateId(const std::string& request_id, const std::string& plate_id, int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        request.set_plate_id(plate_id);
        request.set_chunk_size(chunk_size);

        std::cout << "\n========== Query: plate_id=" << plate_id << " ==========" << std::endl;
        execute_and_print(request);
    }

    void QueryPlateViolationHistory(const std::string& request_id,
                                    const std::string& plate_id,
                                    int32_t violation_code,
                                    const std::string& date_start,
                                    const std::string& date_end,
                                    int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* q = request.mutable_plate_violation_history();
        q->set_plate_id(plate_id);
        q->set_violation_code(violation_code);
        q->mutable_date_range()->set_start(date_start);
        q->mutable_date_range()->set_end(date_end);
        request.set_chunk_size(chunk_size);

        std::cout << "\n========== Query 1: PlateViolationHistory ==========" << std::endl;
        std::cout << "  plate_id=" << plate_id
                  << ", violation_code=" << violation_code
                  << ", dates=" << date_start << ".." << date_end << std::endl;
        execute_and_print(request);
    }

    void QueryPrecinctVehicleAnalysis(const std::string& request_id,
                                      const std::string& county,
                                      int32_t precinct,
                                      int32_t year_min,
                                      int32_t year_max,
                                      const std::string& body_type,
                                      int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* q = request.mutable_precinct_vehicle_analysis();
        q->set_county(county);
        q->set_precinct(precinct);
        q->set_vehicle_year_min(year_min);
        q->set_vehicle_year_max(year_max);
        q->set_body_type(body_type);
        request.set_chunk_size(chunk_size);

        std::cout << "\n========== Query 2: PrecinctVehicleAnalysis ==========" << std::endl;
        std::cout << "  county=" << county
                  << ", precinct=" << precinct
                  << ", years=" << year_min << "-" << year_max
                  << ", body_type=" << body_type << std::endl;
        execute_and_print(request);
    }

    void QueryUnregisteredVehicleLookup(const std::string& request_id,
                                        const std::string& state,
                                        int32_t feet_threshold,
                                        int chunk_size) {
        QueryRequest request;
        request.set_request_id(request_id);
        auto* q = request.mutable_unregistered_vehicle_lookup();
        q->set_unregistered(true);
        q->set_state(state);
        q->set_feet_from_curb_min(feet_threshold);
        request.set_chunk_size(chunk_size);

        std::cout << "\n========== Query 3: UnregisteredVehicleLookup ==========" << std::endl;
        std::cout << "  unregistered=true, state=" << state
                  << ", feet_from_curb>" << feet_threshold << std::endl;
        execute_and_print(request);
    }

private:
    std::unique_ptr<ParkingViolationService::Stub> stub_;

    void execute_and_print(const QueryRequest& request) {
        QueryResponse response;
        ClientContext context;

        auto t_start = std::chrono::high_resolution_clock::now();
        Status status = stub_->SubmitQuery(&context, request, &response);
        auto t_end = std::chrono::high_resolution_clock::now();
        long long elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();

        if (!status.ok()) {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
            return;
        }

        int total_records = 0;
        for (const auto& chunk : response.chunks()) {
            total_records += chunk.records_size();
        }

        std::cout << "  Received " << response.chunks_size() << " chunks, "
                  << total_records << " records in " << elapsed_ms << " ms" << std::endl;

        int shown = 0;
        for (const auto& chunk : response.chunks()) {
            for (const auto& r : chunk.records()) {
                if (shown < 5) {
                    std::cout << "    [" << shown << "] summons=" << r.summons_number()
                              << " plate=" << r.plate_id()
                              << " state=" << r.registration_state()
                              << " date=" << r.issue_date()
                              << " code=" << r.violation_code()
                              << " make=" << r.vehicle_make()
                              << " body=" << r.vehicle_body_type()
                              << " county=" << r.violation_county()
                              << " precinct=" << r.violation_precinct()
                              << " year=" << r.vehicle_year()
                              << " unreg=" << (r.unregistered_vehicle() ? "Y" : "N")
                              << " curb=" << r.feet_from_curb()
                              << " street=" << r.street_name()
                              << std::endl;
                }
                ++shown;
            }
        }
        if (total_records > 5) {
            std::cout << "    ... and " << (total_records - 5) << " more records" << std::endl;
        }
    }
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
        std::cout << "Connecting to gateway at " << target << std::endl;

        ParkingClient client(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));

        // Query 1: PlateViolationHistory
        // plate_id (string) + violation_code (int32) + DateRange
        client.QueryPlateViolationHistory(
            "q1", "JER1863", 67,
            "2022-06-01", "2023-12-31",
            100
        );

        // Query 2: PrecinctVehicleAnalysis
        // county (string) + precinct (int32) + year range (int32,int32) + body_type (string)
        client.QueryPrecinctVehicleAnalysis(
            "q2", "NY", 10,
            2015, 2020,
            "SDN",
            100
        );

        // Query 3: UnregisteredVehicleLookup
        // unregistered (bool) + state (string) + feet_from_curb threshold (int32)
        // results include summons_number (int64)
        client.QueryUnregisteredVehicleLookup(
            "q3", "NJ", 0,
            100
        );

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
