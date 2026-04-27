#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <stdexcept>
#include <cstdlib>

#include <grpcpp/grpcpp.h>
#include <grpcpp/create_channel.h>

#include "parking_violation_query.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::ClientReader;

using parkingviolation::ParkingViolationService;
using parkingviolation::QueryRequest;
using parkingviolation::QueryResponse;
using parkingviolation::HealthRequest;
using parkingviolation::HealthResponse;
using parkingviolation::ViolationRecord;

// -------------------------------------------------------------------------
// Helper: print one ViolationRecord in a readable format
// -------------------------------------------------------------------------
static void printRecord(const ViolationRecord& r, int idx) {
    std::cout << "  [" << std::setw(5) << idx << "] "
              << "summons=" << r.summons_number()
              << "  plate="  << r.plate_id()
              << "  county=" << r.violation_county()
              << "  code="   << r.violation_code()
              << "  date="   << r.issue_date()
              << "  make="   << r.vehicle_make()
              << "\n";
}

// -------------------------------------------------------------------------
// ParkingViolationClient
// -------------------------------------------------------------------------
class ParkingViolationClient {
public:
    explicit ParkingViolationClient(const std::string& target)
        : stub_(ParkingViolationService::NewStub(
              grpc::CreateChannel(target, grpc::InsecureChannelCredentials()))) {
        target_ = target;
    }

    // ---- HealthCheck ----
    bool healthCheck() {
        ClientContext ctx;
        HealthRequest  req;
        HealthResponse resp;
        Status status = stub_->HealthCheck(&ctx, req, &resp);
        if (!status.ok()) {
            std::cerr << "[Client] HealthCheck failed: " << status.error_message() << "\n";
            return false;
        }
        std::cout << "[Client] HealthCheck OK — process=" << resp.responding_process()
                  << " healthy=" << (resp.is_healthy() ? "true" : "false")
                  << " pending=" << resp.pending_requests()
                  << " workers=" << resp.active_workers() << "\n";
        return resp.is_healthy();
    }

    // ---- QueryViolations ----
    void query(const std::string& county,
               int                violation_code,
               const std::string& date_start,
               const std::string& date_end,
               int                max_records,
               bool               verbose) {

        QueryRequest req;
        req.set_violation_county(county);
        if (violation_code > 0) req.set_violation_code(violation_code);
        req.set_issue_date_start(date_start);
        req.set_issue_date_end(date_end);
        req.set_max_records(max_records);

        std::cout << "\n[Client] sending query to " << target_ << "\n";
        std::cout << "  county="  << (county.empty() ? "(all)" : county) << "\n";
        std::cout << "  code="    << violation_code << "\n";
        std::cout << "  dates="   << (date_start.empty() ? "(any)" : date_start)
                  << " to " << (date_end.empty() ? "(any)" : date_end) << "\n";
        std::cout << "  max="     << max_records << "\n\n";

        auto t0 = std::chrono::high_resolution_clock::now();

        ClientContext ctx;
        auto reader = stub_->QueryViolations(&ctx, req);

        int total_records = 0;
        int total_chunks  = 0;
        bool got_final    = false;

        QueryResponse resp;
        while (reader->Read(&resp)) {
            if (resp.is_final()) {
                got_final = true;
                total_records = resp.total_records();
                total_chunks  = resp.total_chunks();
                break;
            }

            std::cout << "[Client] chunk " << resp.chunk_number()
                      << " from " << resp.source_process()
                      << " — " << resp.records_size() << " records\n";

            if (verbose) {
                for (int i = 0; i < resp.records_size(); ++i)
                    printRecord(resp.records(i), total_records + i);
            }

            total_records += resp.records_size();
            ++total_chunks;
        }

        Status status = reader->Finish();
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::high_resolution_clock::now() - t0).count();

        if (!status.ok()) {
            std::cerr << "[Client] query failed: " << status.error_message() << "\n";
            return;
        }

        if (got_final) {
            std::cout << "\n[Client] query complete (from final chunk)\n"
                      << "  total_records = " << total_records << "\n"
                      << "  total_chunks  = " << total_chunks  << "\n"
                      << "  elapsed_ms    = " << elapsed_ms     << "\n\n";
        } else {
            std::cout << "\n[Client] stream ended\n"
                      << "  records received = " << total_records << "\n"
                      << "  chunks received  = " << total_chunks  << "\n"
                      << "  elapsed_ms       = " << elapsed_ms     << "\n\n";
        }
    }

private:
    std::string target_;
    std::unique_ptr<ParkingViolationService::Stub> stub_;
};

// -------------------------------------------------------------------------
// CLI argument helpers
// -------------------------------------------------------------------------
static std::string getArg(int argc, char** argv, const std::string& flag,
                           const std::string& defaultVal = "") {
    for (int i = 1; i + 1 < argc; ++i)
        if (std::string(argv[i]) == flag)
            return argv[i + 1];
    return defaultVal;
}

static bool hasFlag(int argc, char** argv, const std::string& flag) {
    for (int i = 1; i < argc; ++i)
        if (std::string(argv[i]) == flag)
            return true;
    return false;
}

// -------------------------------------------------------------------------
// main
// -------------------------------------------------------------------------
int main(int argc, char** argv) {
    if (hasFlag(argc, argv, "--help") || hasFlag(argc, argv, "-h")) {
        std::cout <<
            "Usage: client [options]\n"
            "\n"
            "Options:\n"
            "  --host   HOST       Leader host (default: localhost)\n"
            "  --port   PORT       Leader port (default: 50051)\n"
            "  --county COUNTY     Filter by violation_county (default: all)\n"
            "  --code   CODE       Filter by violation_code (default: 0 = all)\n"
            "  --start  MM/DD/YYYY Issue date range start (default: none)\n"
            "  --end    MM/DD/YYYY Issue date range end   (default: none)\n"
            "  --max    N          Max records to return  (default: 1000)\n"
            "  --health            Only run a health check\n"
            "  --verbose           Print every record\n"
            "\nExamples:\n"
            "  client --county NY --max 500\n"
            "  client --code 21 --start 01/01/2025 --end 12/31/2025\n"
            "  client --health\n";
        return 0;
    }

    std::string host    = getArg(argc, argv, "--host",   "localhost");
    std::string portStr = getArg(argc, argv, "--port",   "50051");
    std::string county  = getArg(argc, argv, "--county", "");
    std::string codeStr = getArg(argc, argv, "--code",   "0");
    std::string start   = getArg(argc, argv, "--start",  "");
    std::string end     = getArg(argc, argv, "--end",    "");
    std::string maxStr  = getArg(argc, argv, "--max",    "1000");
    bool health_only    = hasFlag(argc, argv, "--health");
    bool verbose        = hasFlag(argc, argv, "--verbose");

    int port       = std::stoi(portStr);
    int code       = std::stoi(codeStr);
    int max_rec    = std::stoi(maxStr);

    std::string target = host + ":" + portStr;

    ParkingViolationClient client(target);

    if (health_only) {
        client.healthCheck();
        return 0;
    }

    // Optionally run a health check first
    if (!client.healthCheck()) {
        std::cerr << "[Client] Leader is not healthy. Aborting.\n";
        return 1;
    }

    client.query(county, code, start, end, max_rec, verbose);
    return 0;
}