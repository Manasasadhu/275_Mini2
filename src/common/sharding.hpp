#ifndef SHARDING_HPP
#define SHARDING_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include "parking_violation_query.pb.h"

using namespace parkingviolation;

static inline bool date_in_range(const std::string& issue_date,
                                 const std::string& shard_start,
                                 const std::string& shard_end) {
    if (issue_date.length() < 10) return false;
    int m = std::stoi(issue_date.substr(0, 2));
    int d = std::stoi(issue_date.substr(3, 2));
    int y = std::stoi(issue_date.substr(6, 4));
    std::string normalized = std::to_string(y)
        + (m < 10 ? "0" : "") + std::to_string(m)
        + (d < 10 ? "0" : "") + std::to_string(d);

    std::string start_norm = shard_start.substr(0, 4) + shard_start.substr(5, 2) + shard_start.substr(8, 2);
    std::string end_norm   = shard_end.substr(0, 4)   + shard_end.substr(5, 2)   + shard_end.substr(8, 2);

    return normalized >= start_norm && normalized <= end_norm;
}

static inline std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    bool in_quotes = false;
    std::string field;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                field += '"';
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == ',' && !in_quotes) {
            fields.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    fields.push_back(field);
    return fields;
}

static inline int32_t safe_stoi(const std::string& s) {
    if (s.empty()) return 0;
    try { return static_cast<int32_t>(std::stoi(s)); }
    catch (...) { return 0; }
}

static inline int64_t safe_stoll(const std::string& s) {
    if (s.empty()) return 0;
    try { return std::stoll(s); }
    catch (...) { return 0; }
}

static inline ViolationRecord parse_csv_line(const std::string& line) {
    ViolationRecord record;
    auto f = split_csv_line(line);

    auto get = [&](size_t idx) -> const std::string& {
        static const std::string empty;
        return idx < f.size() ? f[idx] : empty;
    };

    record.set_summons_number(safe_stoll(get(0)));
    record.set_plate_id(get(1));
    record.set_registration_state(get(2));
    record.set_plate_type(get(3));
    record.set_issue_date(get(4));
    record.set_violation_code(safe_stoi(get(5)));
    record.set_vehicle_body_type(get(6));
    record.set_vehicle_make(get(7));
    record.set_issuing_agency(get(8));
    record.set_street_code1(safe_stoi(get(9)));
    record.set_street_code2(safe_stoi(get(10)));
    record.set_street_code3(safe_stoi(get(11)));
    record.set_vehicle_expiration_date(get(12));
    record.set_violation_location(get(13));
    record.set_violation_precinct(safe_stoi(get(14)));
    record.set_issuer_precinct(safe_stoi(get(15)));
    record.set_issuer_code(safe_stoi(get(16)));
    record.set_violation_county(get(21));
    record.set_street_name(get(24));
    record.set_unregistered_vehicle(get(34) == "Y" || get(34) == "y");
    record.set_vehicle_year(safe_stoi(get(35)));
    record.set_feet_from_curb(safe_stoi(get(37)));
    record.set_violation_description(get(39));
    record.set_vehicle_color(get(33));

    return record;
}

static inline bool matches_query(const ViolationRecord& record,
                                 const ForwardRequest& request) {
    if (request.has_plate_id()) {
        return record.plate_id() == request.plate_id();
    }
    if (request.has_violation_code()) {
        return record.violation_code() == request.violation_code();
    }
    if (request.has_issue_date()) {
        return true;  // shard date filter already applied
    }
    if (request.has_plate_violation_history()) {
        const auto& q = request.plate_violation_history();
        if (record.plate_id() != q.plate_id()) return false;
        if (record.violation_code() != q.violation_code()) return false;
        if (!q.date_range().start().empty() || !q.date_range().end().empty()) {
            if (!date_in_range(record.issue_date(),
                               q.date_range().start(),
                               q.date_range().end()))
                return false;
        }
        return true;
    }
    if (request.has_precinct_vehicle_analysis()) {
        const auto& q = request.precinct_vehicle_analysis();
        if (record.violation_county() != q.county()) return false;
        if (record.violation_precinct() != q.precinct()) return false;
        if (record.vehicle_year() < q.vehicle_year_min()) return false;
        if (record.vehicle_year() > q.vehicle_year_max()) return false;
        if (record.vehicle_body_type() != q.body_type()) return false;
        return true;
    }
    if (request.has_unregistered_vehicle_lookup()) {
        const auto& q = request.unregistered_vehicle_lookup();
        if (q.unregistered() && !record.unregistered_vehicle()) return false;
        if (record.registration_state() != q.state()) return false;
        if (record.feet_from_curb() <= q.feet_from_curb_min()) return false;
        return true;
    }
    return true;
}

std::vector<Chunk> process_query_on_shard(
    const std::string& csv_path,
    const std::string& shard_start,
    const std::string& shard_end,
    const ForwardRequest& request,
    int chunk_size,
    const std::string& request_id
) {
    std::vector<Chunk> chunks;
    std::ifstream file(csv_path);
    if (!file.is_open()) return chunks;

    std::string line;
    std::getline(file, line);  // skip header

    Chunk current_chunk;
    current_chunk.set_request_id(request_id);
    current_chunk.set_is_last(false);

    while (std::getline(file, line)) {
        if (line.empty()) continue;

        // Quick date extraction from column 4 (Issue Date) for shard filter
        size_t pos = 0;
        std::string issue_date;
        int commas = 0;
        for (size_t i = 0; i < line.size() && commas < 5; ++i) {
            if (line[i] == ',' && !(i > 0 && line[i-1] == '"')) {
                ++commas;
                if (commas == 4) pos = i + 1;
                if (commas == 5) {
                    issue_date = line.substr(pos, i - pos);
                    break;
                }
            }
        }

        if (!date_in_range(issue_date, shard_start, shard_end)) continue;

        ViolationRecord record = parse_csv_line(line);

        if (!matches_query(record, request)) continue;

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
