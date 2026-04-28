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
    
    // Header: Summons Number,Plate ID,Registration State,Plate Type,Issue Date,Violation Code,Vehicle Body Type,Vehicle Make,Issuing Agency,...
    while (std::getline(ss, field, ',') && col < 30) {
        if (col == 0) record.set_summons_number(std::stoll(field));
        else if (col == 1) record.set_plate_id(field);
        else if (col == 2) record.set_registration_state(field);
        else if (col == 3) record.set_plate_type(field);
        else if (col == 4) record.set_issue_date(field);
        else if (col == 5) record.set_violation_code(std::stoi(field));
        else if (col == 6) record.set_vehicle_body_type(field);
        else if (col == 7) record.set_vehicle_make(field);
        else if (col == 8) record.set_issuing_agency(field);
        col++;
    }
    return record;
}

// Query shard: scan CSV, filter by shard+query, chunk results
std::vector<Chunk> process_query_on_shard(
    const std::string& csv_path,
    const std::string& shard_start,
    const std::string& shard_end,
    const std::string& plate_id,  // Empty if not searching by plate
    int chunk_size,
    const std::string& request_id
) {
    std::vector<Chunk> chunks;
    std::ifstream file(csv_path);
    if (!file.is_open()) return chunks;
    
    std::string line;
    std::getline(file, line);  // Skip header
    
    Chunk current_chunk;
    current_chunk.set_request_id(request_id);
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
        
        // Query filter: plate_id
        if (!plate_id.empty() && record.plate_id() != plate_id) continue;
        
        current_chunk.add_records()->CopyFrom(record);
        if (current_chunk.records_size() >= chunk_size) {
            chunks.push_back(current_chunk);
            current_chunk.Clear();
            current_chunk.set_request_id(request_id);
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
