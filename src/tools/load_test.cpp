// load_test.cpp
// Quick smoke-test for DobDataLoader.
// Usage:
//   ./load_test <path-to-csv> [borough] [permit_type] [max_records]
//
// Examples:
//   ./load_test "/Users/Asha/Downloads/dob_permits copy.csv"
//   ./load_test "/Users/Asha/Downloads/dob_permits copy.csv" MANHATTAN EQ 200

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <chrono>
#include <map>

#include "../common/dob_data_loader.hpp"

static void printRecord(const DobPermitRecord& r) {
    std::cout
        << "  borough="       << r.borough
        << "  bin="           << r.bin_number
        << "  job#="          << r.job_number
        << "  type="          << r.permit_type
        << "  job_type="      << r.job_type
        << "  status="        << r.permit_status
        << "  filed="         << r.filing_date
        << "  lat="           << std::fixed << std::setprecision(6) << r.latitude
        << "  lon="           << r.longitude
        << "  nta="           << r.nta_name
        << "\n";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <csv_path> [borough] [permit_type] [max_records]\n";
        return 1;
    }

    std::string csv_path     = argv[1];
    std::string borough      = (argc >= 3) ? argv[2] : "";
    std::string permit_type  = (argc >= 4) ? argv[3] : "";
    int         max_records  = (argc >= 5) ? std::stoi(argv[4]) : 0;

    std::vector<std::string> borough_filter;
    if (!borough.empty()) borough_filter.push_back(borough);

    std::cout << "=== DOB Permit Issuance Loader ===\n";
    std::cout << "CSV path    : " << csv_path    << "\n";
    std::cout << "Borough     : " << (borough.empty()     ? "(all)" : borough)     << "\n";
    std::cout << "Permit type : " << (permit_type.empty() ? "(all)" : permit_type) << "\n";
    std::cout << "Max records : " << (max_records == 0 ? "(no limit)" : std::to_string(max_records)) << "\n\n";

    DobDataLoader loader(csv_path);

    // --- Count total rows first ---
    std::cout << "Counting total records in CSV...\n";
    auto t0 = std::chrono::high_resolution_clock::now();
    int total_rows = loader.countRecords();
    auto t1 = std::chrono::high_resolution_clock::now();
    long long count_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    std::cout << "Total data rows (excl. header): " << total_rows
              << "  (" << count_ms << " ms)\n\n";

    // --- Load with filters ---
    std::cout << "Loading records...\n";
    auto t2 = std::chrono::high_resolution_clock::now();

    std::vector<DobPermitRecord> records = loader.loadData(
        borough_filter,
        permit_type,
        "",  // filing_date_start (no filter)
        "",  // filing_date_end   (no filter)
        max_records
    );

    auto t3 = std::chrono::high_resolution_clock::now();
    long long load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();

    std::cout << "Loaded " << records.size() << " records in " << load_ms << " ms\n\n";

    // --- Print first 5 records ---
    int preview = std::min(static_cast<int>(records.size()), 5);
    if (preview > 0) {
        std::cout << "--- First " << preview << " records ---\n";
        for (int i = 0; i < preview; ++i) printRecord(records[i]);
        std::cout << "\n";
    }

    // --- Borough summary ---
    std::cout << "--- Borough distribution ---\n";
    std::map<std::string, int> borough_counts;
    for (const auto& r : records) borough_counts[r.borough]++;
    for (const auto& kv : borough_counts)
        std::cout << "  " << std::setw(14) << std::left << kv.first << " : " << kv.second << "\n";
    std::cout << "\n";

    // --- Permit type summary ---
    std::cout << "--- Permit type distribution ---\n";
    std::map<std::string, int> type_counts;
    for (const auto& r : records) type_counts[r.permit_type]++;
    for (const auto& kv : type_counts)
        std::cout << "  " << std::setw(6) << std::left << kv.first << " : " << kv.second << "\n";
    std::cout << "\n";

    // --- Chunking preview ---
    int chunk_size = 500;
    int num_chunks = static_cast<int>((records.size() + chunk_size - 1) / chunk_size);
    std::cout << "--- Chunk split preview (chunk_size=" << chunk_size << ") ---\n";
    std::cout << "  Total records : " << records.size() << "\n";
    std::cout << "  Num chunks    : " << num_chunks     << "\n";
    for (int c = 0; c < std::min(num_chunks, 4); ++c) {
        int start = c * chunk_size;
        int end   = std::min(start + chunk_size, static_cast<int>(records.size()));
        std::cout << "  Chunk " << c << " : rows " << start << " - " << (end - 1)
                  << " (" << (end - start) << " records)\n";
    }
    if (num_chunks > 4) std::cout << "  ... (" << (num_chunks - 4) << " more chunks)\n";

    return 0;
}
