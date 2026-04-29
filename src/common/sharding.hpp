#ifndef SHARDING_HPP
#define SHARDING_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <ctime>
#include "parking_violation_query.pb.h"

using namespace parkingviolation;

// Parse MM/DD/YYYY to compare with YYYY-MM-DD
bool date_in_range(const std::string& issue_date, const std::string& shard_start, const std::string& shard_end) {
    if (issue_date.length() < 10) return false;
    // Convert MM/DD/YYYY to YYYYMMDD for comparison
    int m = std::stoi(issue_date.substr(0, 2));
    int d = std::stoi(issue_date.substr(3, 2));
    int y = std::stoi(issue_date.substr(6, 4));
    std::string normalized = std::to_string(y) + (m < 10 ? "0" : "") + std::to_string(m) + (d < 10 ? "0" : "") + std::to_string(d);
    
    std::string start_norm = shard_start.substr(0, 4) + shard_start.substr(5, 2) + shard_start.substr(8, 2);
    std::string end_norm = shard_end.substr(0, 4) + shard_end.substr(5, 2) + shard_end.substr(8, 2);
    
    return normalized >= start_norm && normalized <= end_norm;
}

// Build ViolationRecord from CSV line
ViolationRecord parse_csv_line(const std::string& line) {
    ViolationRecord record;
    std::stringstream ss(line);
    std::string field;
    int col = 0;
    
    while (std::getline(ss, field, ',')) {
        try {
            switch (col) {
                case 0: record.set_summons_number(std::stoll(field)); break;
                case 1: record.set_plate_id(field); break;
                case 2: record.set_registration_state(field); break;
                case 3: record.set_plate_type(field); break;
                case 4: record.set_issue_date(field); break;
                case 5: record.set_violation_code(std::stoi(field)); break;
                case 6: record.set_vehicle_body_type(field); break;
                case 7: record.set_vehicle_make(field); break;
                case 8: record.set_issuing_agency(field); break;
                case 9: record.set_street_code1(std::stoi(field)); break;
                case 10: record.set_street_code2(std::stoi(field)); break;
                case 11: record.set_street_code3(std::stoi(field)); break;
                case 12: record.set_vehicle_expiration_date(field); break;
                case 13: record.set_violation_location(field); break;
                case 14: record.set_violation_precinct(std::stoi(field)); break;
                case 15: record.set_issuer_precinct(std::stoi(field)); break;
                case 16: record.set_issuer_code(std::stoi(field)); break;
                case 21: record.set_violation_county(field); break;
                case 24: record.set_street_name(field); break;
                case 25: record.set_county(field); break;  // Intersecting Street, but using as county
                case 26: record.set_issuing_agency_name(field); break;
                case 27: record.set_violation_status(field); break;
                case 33: record.set_vehicle_color(field); break;
                case 34: record.set_unregistered_vehicle(field == "1"); break;
                case 35: record.set_vehicle_year(std::stoi(field)); break;
                case 37: record.set_feet_from_curb(std::stoi(field)); break;
                case 39: record.set_violation_description(field); break;
            }
        } catch (const std::exception& e) {
            // Skip invalid fields
        }
        col++;
    }
    return record;
}

#include "parking_violation_query.grpc.pb.h"

// Query shard: scan CSV, filter by shard+query, chunk results
std::vector<Chunk> process_query_on_shard(
    const std::string& csv_path,
    const std::string& shard_start,
    const std::string& shard_end,
    const parkingviolation::ForwardRequest* request,
    int chunk_size
) {
    std::vector<Chunk> chunks;
    std::ifstream file(csv_path);
    if (!file.is_open()) return chunks;
    
    std::string line;
    std::getline(file, line);  // Skip header
    
    Chunk current_chunk;
    current_chunk.set_request_id(request->request_id());
    current_chunk.set_is_last(false);
    
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        
        // Quick date check (4th field is Issue Date)
        size_t pos = 0, field_num = 0;
        std::string issue_date;
        for (int i = 0; i < 5; i++) {
            pos = line.find(',', pos);
            if (i == 4) {
                size_t start = line.rfind(',', pos - 1) + 1;
                issue_date = line.substr(start, pos - start);
                break;
            }
            pos++;
        }
        
        if (!date_in_range(issue_date, shard_start, shard_end)) continue;
        
        ViolationRecord record = parse_csv_line(line);
        
        // Query filter based on type
        bool matches = false;
        if (request->has_plate_id()) {
            matches = (record.plate_id() == request->plate_id());
        } else if (request->has_violation_code()) {
            matches = (record.violation_code() == request->violation_code());
        } else if (request->has_issue_date()) {
            matches = date_in_range(issue_date, request->issue_date().start(), request->issue_date().end());
        } else if (request->has_plate_violation_history()) {
            const auto& q = request->plate_violation_history();
            matches = (record.plate_id() == q.plate_id()) &&
                     (q.violation_code() == 0 || record.violation_code() == q.violation_code()) &&
                     date_in_range(issue_date, q.date_range().start(), q.date_range().end());
        } else if (request->has_precinct_vehicle_analysis()) {
            const auto& q = request->precinct_vehicle_analysis();
            matches = (record.violation_county() == q.county()) &&
                     (q.precinct() == 0 || record.violation_precinct() == q.precinct()) &&
                     (q.vehicle_year_min() == 0 && q.vehicle_year_max() == 9999 || 
                      (record.vehicle_year() >= q.vehicle_year_min() && record.vehicle_year() <= q.vehicle_year_max())) &&
                     (q.body_type().empty() || record.vehicle_body_type() == q.body_type());
        } else if (request->has_unregistered_vehicle_lookup()) {
            const auto& q = request->unregistered_vehicle_lookup();
            matches = (record.unregistered_vehicle() == q.unregistered()) &&
                     (record.registration_state() == q.state()) &&
                     (record.feet_from_curb() >= q.feet_from_curb_min());
        } else {
            matches = true; // No filter
        }
        
        if (!matches) continue;
        
        current_chunk.add_records()->CopyFrom(record);
        if (current_chunk.records_size() >= chunk_size) {
            chunks.push_back(current_chunk);
            current_chunk.Clear();
            current_chunk.set_request_id(request->request_id());
            current_chunk.set_is_last(false);
        }
    }
    
    if (current_chunk.records_size() > 0) {
        current_chunk.set_is_last(true);
        chunks.push_back(current_chunk);
    } else if (chunks.empty()) {
        current_chunk.set_is_last(true);
        chunks.push_back(current_chunk);
    } else {
        chunks.back().set_is_last(true);
    }
    
    return chunks;
}

#endif // SHARDING_HPP
