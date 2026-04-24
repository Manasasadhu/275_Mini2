// load_test.cpp
// Quick smoke-test for ParkingViolationsLoader.
// Usage:
//   ./load_test <path-to-csv> [county] [violation_code] [max_records]
//
// Examples:
//   ./load_test "/path/to/parking_violations_2025.csv"
//   ./load_test "/path/to/parking_violations_2025.csv" NY
//   ./load_test "/path/to/parking_violations_2025.csv" BK 38 1000

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <chrono>
#include <map>

#include "../common/parking_violations_loader.hpp"

static const char* kDefaultCsvPath = "/Users/aravindreddy/Downloads/SJSU ClassWork/275 EAD/275_Mini2_Dataset/parking_violations_combined.csv";

static void printRecord(const ParkingViolationRecord& r) {
    std::cout
        << "  summons="     << r.summons_number
        << "  county="      << r.violation_county
        << "  precinct="    << r.violation_precinct
        << "  code="        << r.violation_code
        << "  plate="       << r.plate_id
        << "  state="       << r.registration_state
        << "  make="        << r.vehicle_make
        << "  issued="      << r.issue_date
        << "  agency="      << r.issuing_agency
        << "  street="      << r.street_name
        << "  desc="        << r.violation_description
        << "\n";
}

int main(int argc, char** argv) {
    std::string csv_path        = (argc >= 2) ? argv[1] : kDefaultCsvPath;
    std::string county          = (argc >= 3) ? argv[2] : "";
    int32_t     violation_code  = (argc >= 4) ? std::stoi(argv[3]) : 0;
    int         max_records     = (argc >= 5) ? std::stoi(argv[4]) : 0;

    std::vector<std::string> county_filter;
    if (!county.empty()) county_filter.push_back(county);

    std::cout << "=== NYC Parking Violations Loader ===\n";
    std::cout << "CSV path       : " << csv_path << "\n";
    std::cout << "County         : " << (county.empty() ? "(all)" : county) << "\n";
    std::cout << "Violation code : " << (violation_code == 0 ? "(all)" : std::to_string(violation_code)) << "\n";
    std::cout << "Max records    : " << (max_records == 0 ? "(no limit)" : std::to_string(max_records)) << "\n\n";

    ParkingViolationsLoader loader(csv_path);

    // --- Count total rows ---
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

    std::vector<ParkingViolationRecord> records = loader.loadData(
        county_filter,
        violation_code,
        "",   // issue_date_start (no filter)
        "",   // issue_date_end   (no filter)
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

    // --- County distribution ---
    std::cout << "--- County distribution ---\n";
    std::map<std::string, int> county_counts;
    for (const auto& r : records) county_counts[r.violation_county]++;
    for (const auto& kv : county_counts)
        std::cout << "  " << std::setw(8) << std::left << kv.first << " : " << kv.second << "\n";
    std::cout << "\n";

    // --- Top 10 violation codes ---
    std::cout << "--- Top violation codes ---\n";
    std::map<int32_t, int> code_counts;
    for (const auto& r : records) code_counts[r.violation_code]++;
    std::vector<std::pair<int,int32_t>> sorted_codes;
    for (const auto& kv : code_counts)
        sorted_codes.push_back({kv.second, kv.first});
    std::sort(sorted_codes.rbegin(), sorted_codes.rend());
    int shown = 0;
    for (const auto& kv : sorted_codes) {
        std::cout << "  code " << std::setw(4) << kv.second << " : " << kv.first << "\n";
        if (++shown >= 10) break;
    }
    std::cout << "\n";

    // --- Issuing agency distribution ---
    std::cout << "--- Issuing agency distribution ---\n";
    std::map<std::string, int> agency_counts;
    for (const auto& r : records) agency_counts[r.issuing_agency]++;
    for (const auto& kv : agency_counts)
        std::cout << "  " << std::setw(4) << std::left << kv.first << " : " << kv.second << "\n";
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