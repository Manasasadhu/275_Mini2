#ifndef PARKING_VIOLATIONS_LOADER_HPP
#define PARKING_VIOLATIONS_LOADER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <stdexcept>

// ---------------------------------------------------------------------------
// NYC Parking Violations record
// Field types match the dataset: int64 for summons/IDs, int32 for codes,
// bool for Y/N flags, strings for everything else.
//
// CSV column order (0-indexed):
//  0  Summons Number             -> int64
//  1  Plate ID                   -> string
//  2  Registration State         -> string
//  3  Plate Type                 -> string
//  4  Issue Date                 -> string  (MM/DD/YYYY)
//  5  Violation Code             -> int32
//  6  Vehicle Body Type          -> string
//  7  Vehicle Make               -> string
//  8  Issuing Agency             -> string
//  9  Street Code1               -> int32
// 10  Street Code2               -> int32
// 11  Street Code3               -> int32
// 12  Vehicle Expiration Date    -> string  (YYYYMMDD)
// 13  Violation Location         -> string
// 14  Violation Precinct         -> int32
// 15  Issuer Precinct            -> int32
// 16  Issuer Code                -> int32
// 17  Issuer Command             -> string
// 18  Issuer Squad               -> string
// 19  Violation Time             -> string  (e.g. "0209P")
// 20  Time First Observed        -> string
// 21  Violation County           -> string  (partition key: NY/MN, BK/K/Kings, QN/Q, BX, ST)
// 22  Violation In Front Of Or Opposite -> string
// 23  House Number               -> string
// 24  Street Name                -> string
// 25  Intersecting Street        -> string
// 26  Date First Observed        -> string
// 27  Law Section                -> int32
// 28  Sub Division               -> string
// 29  Violation Legal Code       -> string
// 30  Days Parking In Effect     -> string  (e.g. "YYYYYY")
// 31  From Hours In Effect       -> string  (e.g. "0730A")
// 32  To Hours In Effect         -> string
// 33  Vehicle Color              -> string
// 34  Unregistered Vehicle?      -> bool    (Y / empty)
// 35  Vehicle Year               -> int32
// 36  Meter Number               -> string
// 37  Feet From Curb             -> int32
// 38  Violation Post Code        -> string
// 39  Violation Description      -> string
// 40  No Standing or Stopping Violation -> string
// 41  Hydrant Violation          -> string
// 42  Double Parking Violation   -> string
// ---------------------------------------------------------------------------

struct ParkingViolationRecord {
    int64_t     summons_number      = 0;
    std::string plate_id;
    std::string registration_state;
    std::string plate_type;
    std::string issue_date;          // MM/DD/YYYY
    int32_t     violation_code      = 0;
    std::string vehicle_body_type;
    std::string vehicle_make;
    std::string issuing_agency;
    int32_t     street_code1        = 0;
    int32_t     street_code2        = 0;
    int32_t     street_code3        = 0;
    std::string vehicle_expiration_date; // YYYYMMDD
    std::string violation_location;
    int32_t     violation_precinct  = 0;
    int32_t     issuer_precinct     = 0;
    int32_t     issuer_code         = 0;
    std::string issuer_command;
    std::string issuer_squad;
    std::string violation_time;      // e.g. "0209P"
    std::string time_first_observed;
    std::string violation_county;    // partition key: NY/MN, BK/K/Kings, QN/Q, BX, ST
    std::string violation_front_opposite;
    std::string house_number;
    std::string street_name;
    std::string intersecting_street;
    std::string date_first_observed;
    int32_t     law_section         = 0;
    std::string sub_division;
    std::string violation_legal_code;
    std::string days_parking_in_effect;
    std::string from_hours_in_effect;
    std::string to_hours_in_effect;
    std::string vehicle_color;
    bool        unregistered_vehicle = false;
    int32_t     vehicle_year        = 0;
    std::string meter_number;
    int32_t     feet_from_curb      = 0;
    std::string violation_post_code;
    std::string violation_description;
    std::string no_standing_violation;
    std::string hydrant_violation;
    std::string double_parking_violation;
};

// ---------------------------------------------------------------------------
// ParkingViolationsLoader
// Loads the flat CSV into a vector<ParkingViolationRecord>.
// Partition key is violation_county — each process owns a list of county codes.
// Note: NYC county codes have variants (e.g. Brooklyn = BK, K, Kings).
// Include all variants in owned_counties in the process config.
// ---------------------------------------------------------------------------
class ParkingViolationsLoader {
public:
    explicit ParkingViolationsLoader(const std::string& csv_path)
        : csv_path_(csv_path) {}

    // Load records from the CSV.
    //   county_filter     : keep only these county codes (empty = keep all)
    //   violation_code_filter : 0 = no filter
    //   issue_date_start/end  : MM/DD/YYYY range, inclusive (empty = no limit)
    //   max_records       : 0 = no limit
    std::vector<ParkingViolationRecord> loadData(
        const std::vector<std::string>& county_filter    = {},
        int32_t violation_code_filter                    = 0,
        const std::string& issue_date_start              = "",
        const std::string& issue_date_end                = "",
        int max_records                                  = 0)
    {
        std::ifstream file(csv_path_);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open CSV: " + csv_path_);
        }

        std::vector<ParkingViolationRecord> results;
        std::string line;

        // Skip header row
        if (!std::getline(file, line)) {
            std::cerr << "Warning: CSV is empty\n";
            return results;
        }

        int line_num = 1;
        while (std::getline(file, line)) {
            ++line_num;
            if (line.empty()) continue;

            if (max_records > 0 && static_cast<int>(results.size()) >= max_records) break;

            ParkingViolationRecord rec = parseCSVLine(line, line_num);

            // --- filter: county ---
            if (!county_filter.empty()) {
                bool match = false;
                for (const auto& c : county_filter) {
                    if (rec.violation_county == c) { match = true; break; }
                }
                if (!match) continue;
            }

            // --- filter: violation code ---
            if (violation_code_filter != 0 && rec.violation_code != violation_code_filter)
                continue;

            // --- filter: date range ---
            if (!issue_date_start.empty() || !issue_date_end.empty()) {
                std::string d = normalizeDate(rec.issue_date);
                if (!issue_date_start.empty() && d < normalizeDate(issue_date_start))
                    continue;
                if (!issue_date_end.empty()   && d > normalizeDate(issue_date_end))
                    continue;
            }

            results.push_back(std::move(rec));
        }

        return results;
    }

    // Return total data line count (excluding header).
    int countRecords() {
        std::ifstream file(csv_path_);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open CSV: " + csv_path_);
        }
        int count = -1;
        std::string line;
        while (std::getline(file, line)) ++count;
        return count < 0 ? 0 : count;
    }

private:
    std::string csv_path_;

    // Reorder MM/DD/YYYY -> YYYY/MM/DD for lexicographic date comparison
    static std::string normalizeDate(const std::string& mdy) {
        if (mdy.size() < 10) return mdy;
        if (mdy[2] != '/' || mdy[5] != '/') return mdy;
        return mdy.substr(6, 4) + "/" + mdy.substr(0, 2) + "/" + mdy.substr(3, 2);
    }

    // CSV parser: handles quoted fields
    static std::vector<std::string> splitCSV(const std::string& line) {
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

    static int32_t parseInt32(const std::string& s) {
        if (s.empty()) return 0;
        try { return static_cast<int32_t>(std::stoi(s)); }
        catch (...) { return 0; }
    }

    static int64_t parseInt64(const std::string& s) {
        if (s.empty()) return 0;
        try { return std::stoll(s); }
        catch (...) { return 0; }
    }

    static bool parseBoolY(const std::string& s) {
        return s == "Y" || s == "y";
    }

    ParkingViolationRecord parseCSVLine(const std::string& line, int /*line_num*/) {
        ParkingViolationRecord rec;
        std::vector<std::string> f = splitCSV(line);

        auto get = [&](size_t idx) -> const std::string& {
            static const std::string empty;
            return idx < f.size() ? f[idx] : empty;
        };

        rec.summons_number          = parseInt64(get(0));
        rec.plate_id                = get(1);
        rec.registration_state      = get(2);
        rec.plate_type              = get(3);
        rec.issue_date              = get(4);
        rec.violation_code          = parseInt32(get(5));
        rec.vehicle_body_type       = get(6);
        rec.vehicle_make            = get(7);
        rec.issuing_agency          = get(8);
        rec.street_code1            = parseInt32(get(9));
        rec.street_code2            = parseInt32(get(10));
        rec.street_code3            = parseInt32(get(11));
        rec.vehicle_expiration_date = get(12);
        rec.violation_location      = get(13);
        rec.violation_precinct      = parseInt32(get(14));
        rec.issuer_precinct         = parseInt32(get(15));
        rec.issuer_code             = parseInt32(get(16));
        rec.issuer_command          = get(17);
        rec.issuer_squad            = get(18);
        rec.violation_time          = get(19);
        rec.time_first_observed     = get(20);
        rec.violation_county        = get(21);
        rec.violation_front_opposite = get(22);
        rec.house_number            = get(23);
        rec.street_name             = get(24);
        rec.intersecting_street     = get(25);
        rec.date_first_observed     = get(26);
        rec.law_section             = parseInt32(get(27));
        rec.sub_division            = get(28);
        rec.violation_legal_code    = get(29);
        rec.days_parking_in_effect  = get(30);
        rec.from_hours_in_effect    = get(31);
        rec.to_hours_in_effect      = get(32);
        rec.vehicle_color           = get(33);
        rec.unregistered_vehicle    = parseBoolY(get(34));
        rec.vehicle_year            = parseInt32(get(35));
        rec.meter_number            = get(36);
        rec.feet_from_curb          = parseInt32(get(37));
        rec.violation_post_code     = get(38);
        rec.violation_description   = get(39);
        rec.no_standing_violation   = get(40);
        rec.hydrant_violation       = get(41);
        rec.double_parking_violation = get(42);

        return rec;
    }
};

#endif // PARKING_VIOLATIONS_LOADER_HPP
