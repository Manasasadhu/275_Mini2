#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#include "parking_violation_query.grpc.pb.h"
#include "../../common/config.hpp"
#include "../../common/parking_violations_loader.hpp"
#include "../../common/metrics.hpp"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
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
using parkingviolation::ViolationRecord;

// Convert internal record to protobuf message
static void toProto(const ParkingViolationRecord& src, ViolationRecord* dst) {
    dst->set_summons_number(src.summons_number);
    dst->set_plate_id(src.plate_id);
    dst->set_registration_state(src.registration_state);
    dst->set_plate_type(src.plate_type);
    dst->set_issue_date(src.issue_date);
    dst->set_violation_code(src.violation_code);
    dst->set_vehicle_body_type(src.vehicle_body_type);
    dst->set_vehicle_make(src.vehicle_make);
    dst->set_issuing_agency(src.issuing_agency);
    dst->set_street_code1(src.street_code1);
    dst->set_street_code2(src.street_code2);
    dst->set_street_code3(src.street_code3);
    dst->set_vehicle_expiration_date(src.vehicle_expiration_date);
    dst->set_violation_location(src.violation_location);
    dst->set_violation_precinct(src.violation_precinct);
    dst->set_issuer_precinct(src.issuer_precinct);
    dst->set_issuer_code(src.issuer_code);
    dst->set_issuer_command(src.issuer_command);
    dst->set_issuer_squad(src.issuer_squad);
    dst->set_violation_time(src.violation_time);
    dst->set_time_first_observed(src.time_first_observed);
    dst->set_violation_county(src.violation_county);
    dst->set_violation_front_opposite(src.violation_front_opposite);
    dst->set_house_number(src.house_number);
    dst->set_street_name(src.street_name);
    dst->set_intersecting_street(src.intersecting_street);
    dst->set_date_first_observed(src.date_first_observed);
    dst->set_law_section(src.law_section);
    dst->set_sub_division(src.sub_division);
    dst->set_violation_legal_code(src.violation_legal_code);
    dst->set_days_parking_in_effect(src.days_parking_in_effect);
    dst->set_from_hours_in_effect(src.from_hours_in_effect);
    dst->set_to_hours_in_effect(src.to_hours_in_effect);
    dst->set_vehicle_color(src.vehicle_color);
    dst->set_unregistered_vehicle(src.unregistered_vehicle);
    dst->set_vehicle_year(src.vehicle_year);
    dst->set_meter_number(src.meter_number);
    dst->set_feet_from_curb(src.feet_from_curb);
    dst->set_violation_post_code(src.violation_post_code);
    dst->set_violation_description(src.violation_description);
    dst->set_no_standing_violation(src.no_standing_violation);
    dst->set_hydrant_violation(src.hydrant_violation);
    dst->set_double_parking_violation(src.double_parking_violation);
}

class WorkerServiceImpl final : public ParkingViolationService::Service {
public:
    WorkerServiceImpl(const ProcessConfig& cfg)
        : config_(cfg), loader_(cfg.data_path) {

        std::cout << "[Worker " << config_.process_id << "] starting"
                  << " (team=" << config_.team << ")\n";
        std::cout << "[Worker " << config_.process_id << "] listening on "
                  << config_.listen_host << ":" << config_.listen_port << "\n";
        std::cout << "[Worker " << config_.process_id << "] owned counties: ";
        for (const auto& c : config_.data_partitioning.owned_counties)
            std::cout << c << " ";
        std::cout << "\n";

        metrics::init(config_.process_id, config_.role);
    }

    // -----------------------------------------------------------------------
    // DelegateQuery — main entry point for workers
    // -----------------------------------------------------------------------
    Status DelegateQuery(ServerContext* context,
                         const DelegationRequest* request,
                         ServerWriter<DelegationResponse>* writer) override {

        std::cout << "\n[Worker " << config_.process_id
                  << "] delegation " << request->request_id()
                  << " from " << request->delegating_process() << "\n";

        metrics::log_event("RECV_DELEGATION", request->request_id(),
                           ++pending_, 1, -1, -1, request->delegating_process());

        // Deserialize original query
        QueryRequest query;
        if (!query.ParseFromString(request->original_query())) {
            --pending_;
            return Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to parse query");
        }

        // Determine counties this worker should process
        std::vector<std::string> counties = selectCounties(query);
        if (counties.empty()) {
            std::cout << "[Worker " << config_.process_id
                      << "] no matching counties — returning empty\n";
            --pending_;
            return Status::OK;
        }

        // Load matching records
        auto t0 = std::chrono::high_resolution_clock::now();
        auto records = loader_.loadData(
            counties,
            query.violation_code(),
            query.issue_date_start(),
            query.issue_date_end(),
            query.max_records()
        );
        auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::high_resolution_clock::now() - t0).count();

        std::cout << "[Worker " << config_.process_id << "] loaded "
                  << records.size() << " records in " << load_ms << "ms\n";

        metrics::log_event("LOADED", request->request_id(),
                           pending_, 1, -1, (int)records.size(), "");

        // Stream in chunks
        int chunk_size = config_.chunk_config.default_chunk_size;
        int chunk_num  = 0;

        for (size_t i = 0; i < records.size(); i += chunk_size) {
            if (context->IsCancelled()) {
                metrics::log_event("CANCELLED", request->request_id(),
                                   pending_, 1, chunk_num, -1, "");
                --pending_;
                return Status::CANCELLED;
            }

            DelegationResponse resp;
            resp.set_request_id(request->request_id());
            resp.set_chunk_number(chunk_num);
            resp.set_is_final(false);
            resp.set_responding_process(config_.process_id);

            size_t end = std::min(i + (size_t)chunk_size, records.size());
            for (size_t j = i; j < end; ++j)
                toProto(records[j], resp.add_records());

            if (!writer->Write(resp)) {
                metrics::log_event("WRITE_FAIL", request->request_id(),
                                   pending_, 1, chunk_num, (int)resp.records_size(), "");
                --pending_;
                return Status::CANCELLED;
            }

            metrics::log_event("CHUNK_SENT", request->request_id(),
                               pending_, 1, chunk_num, (int)resp.records_size(),
                               config_.process_id);

            std::cout << "[Worker " << config_.process_id << "] sent chunk "
                      << chunk_num << " (" << resp.records_size() << " records)\n";
            ++chunk_num;
        }

        metrics::log_event("DONE", request->request_id(),
                           pending_, 1, chunk_num, (int)records.size(), "");
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
        resp->set_active_workers(1);
        return Status::OK;
    }

    Status QueryViolations(ServerContext*,
                           const QueryRequest*,
                           ServerWriter<QueryResponse>*) override {
        return Status(grpc::StatusCode::UNIMPLEMENTED,
                      "Workers do not accept direct queries");
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
    ProcessConfig          config_;
    ParkingViolationsLoader loader_;
    int pending_   = 0;
    int completed_ = 0;

    // Returns the subset of owned_counties that match the query's county filter.
    // If the query has no county filter, returns all owned_counties.
    std::vector<std::string> selectCounties(const QueryRequest& query) {
        const auto& owned = config_.data_partitioning.owned_counties;
        if (owned.empty()) return {};  // this process owns no data

        const std::string& qc = query.violation_county();
        if (qc.empty()) return owned;  // no filter — serve all owned

        // Check if any owned county matches the requested county
        for (const auto& c : owned) {
            if (c == qc) return {qc};
        }
        return {};  // query county not in this process's partition
    }
};

void RunWorker(const std::string& config_file) {
    ProcessConfig config = ConfigParser::loadConfig(config_file);
    WorkerServiceImpl service(config);

    std::string address = config.listen_host + ":" + std::to_string(config.listen_port);
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    std::cout << "\n*** Worker " << config.process_id
              << " listening on " << address << " ***\n\n";
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>\n";
        return 1;
    }
    try {
        RunWorker(argv[1]);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        metrics::shutdown();
        return 1;
    }
    metrics::shutdown();
    return 0;
}