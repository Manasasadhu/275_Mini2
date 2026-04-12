#ifndef DOB_DATA_LOADER_HPP
#define DOB_DATA_LOADER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <stdexcept>

// ---------------------------------------------------------------------------
// DOB Permit Issuance record
// Field types match the dataset: ints for numeric IDs, doubles for lat/lon,
// bools for Y/N flags, strings for everything else.
//
// CSV column order (0-indexed):
//  0  BOROUGH               -> string
//  1  Bin #                 -> int32
//  2  House #               -> string
//  3  Street Name           -> string
//  4  Job #                 -> int64
//  5  Job doc. #            -> int32
//  6  Job Type              -> string
//  7  Self_Cert             -> bool  (Y / empty)
//  8  Block                 -> string
//  9  Lot                   -> string
// 10  Community Board       -> int32
// 11  Zip Code              -> string
// 12  Bldg Type             -> int32
// 13  Residential           -> bool  (YES / empty)
// 14  Special District 1    -> string
// 15  Special District 2    -> string
// 16  Work Type             -> string
// 17  Permit Status         -> string
// 18  Filing Status         -> string
// 19  Permit Type           -> string
// 20  Permit Sequence #     -> int32
// 21  Permit Subtype        -> string
// 22  Oil Gas               -> string
// 23  Site Fill             -> string
// 24  Filing Date           -> string (MM/DD/YYYY)
// 25  Issuance Date         -> string
// 26  Expiration Date       -> string
// 27  Job Start Date        -> string
// 28  Permittee First Name  -> string
// 29  Permittee Last Name   -> string
// 30  Permittee Business    -> string
// 31  Permittee Phone       -> string
// 32  Permittee License Type-> string
// 33  Permittee License #   -> string
// 34  Act as Superintendent -> bool  (Y / empty)
// 35  Permittee Other Title -> string
// 36  HIC License           -> string
// 37  Site Safety Mgr First -> string
// 38  Site Safety Mgr Last  -> string
// 39  Site Safety Mgr Biz   -> string
// 40  Super First & Last    -> string
// 41  Super Business Name   -> string
// 42  Owner Business Type   -> string
// 43  Non-Profit            -> bool  (Y / empty)
// 44  Owner Business Name   -> string
// 45  Owner First Name      -> string
// 46  Owner Last Name       -> string
// 47  Owner House #         -> string
// 48  Owner House Street    -> string
// 49  Owner House City      -> string
// 50  Owner House State     -> string
// 51  Owner House Zip       -> string
// 52  Owner Phone           -> string
// 53  DOBRunDate            -> string
// 54  PERMIT_SI_NO          -> int64
// 55  LATITUDE              -> double
// 56  LONGITUDE             -> double
// 57  COUNCIL_DISTRICT      -> int32
// 58  CENSUS_TRACT          -> string
// 59  NTA_NAME              -> string
// ---------------------------------------------------------------------------

struct DobPermitRecord {
    // Location / identification
    std::string borough;
    int32_t     bin_number       = 0;
    std::string house_number;
    std::string street_name;
    int64_t     job_number       = 0;
    int32_t     job_doc_number   = 0;
    std::string job_type;       // A1, A2, A3, NB, DM
    bool        self_cert        = false;
    std::string block;
    std::string lot;
    int32_t     community_board  = 0;
    std::string zip_code;
    int32_t     bldg_type        = 0;
    bool        residential      = false;

    // Work / permit info
    std::string work_type;
    std::string permit_status;
    std::string filing_status;
    std::string permit_type;    // EQ, EW, MH, NB, etc.
    int32_t     permit_sequence = 0;
    std::string permit_subtype;

    // Dates
    std::string filing_date;
    std::string issuance_date;
    std::string expiration_date;
    std::string job_start_date;

    // Permittee
    std::string permittee_first_name;
    std::string permittee_last_name;
    std::string permittee_business;
    std::string permittee_phone;
    std::string permittee_license_type;
    std::string permittee_license_num;
    bool        act_as_super     = false;

    // Owner
    std::string owner_business_name;
    std::string owner_first_name;
    std::string owner_last_name;
    bool        non_profit       = false;

    // Geo / admin
    std::string dob_run_date;
    int64_t     permit_si_no     = 0;
    double      latitude         = 0.0;
    double      longitude        = 0.0;
    int32_t     council_district = 0;
    std::string census_tract;
    std::string nta_name;
};

// ---------------------------------------------------------------------------
// DobDataLoader
// Loads the flat CSV into a vector<DobPermitRecord>.
// Supports optional filtering so each process only retains its partition.
// ---------------------------------------------------------------------------
class DobDataLoader {
public:
    explicit DobDataLoader(const std::string& csv_path) : csv_path_(csv_path) {}

    // Load records from the CSV.
    //   borough_filter : keep only these boroughs (empty = keep all)
    //   permit_type_filter : keep only this permit type (empty = keep all)
    //   filing_date_start/end : MM/DD/YYYY range, inclusive (empty = no limit)
    //   max_records : 0 = no limit
    std::vector<DobPermitRecord> loadData(
        const std::vector<std::string>& borough_filter    = {},
        const std::string& permit_type_filter             = "",
        const std::string& filing_date_start              = "",
        const std::string& filing_date_end                = "",
        int max_records                                   = 0)
    {
        std::ifstream file(csv_path_);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open CSV: " + csv_path_);
        }

        std::vector<DobPermitRecord> results;
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

            DobPermitRecord rec = parseCSVLine(line, line_num);

            // --- filter: borough ---
            if (!borough_filter.empty()) {
                bool match = false;
                for (const auto& b : borough_filter) {
                    if (rec.borough == b) { match = true; break; }
                }
                if (!match) continue;
            }

            // --- filter: permit type ---
            if (!permit_type_filter.empty() && rec.permit_type != permit_type_filter)
                continue;

            // --- filter: date range (lexicographic on MM/DD/YYYY is not sortable,
            //     so we convert to YYYY/MM/DD for comparison) ---
            if (!filing_date_start.empty() || !filing_date_end.empty()) {
                std::string fd = normalizeDate(rec.filing_date);
                if (!filing_date_start.empty() && fd < normalizeDate(filing_date_start))
                    continue;
                if (!filing_date_end.empty()   && fd > normalizeDate(filing_date_end))
                    continue;
            }

            results.push_back(std::move(rec));
        }

        return results;
    }

    // Return total line count (excluding header) without storing records.
    // Useful for sizing / planning chunk splits.
    int countRecords() {
        std::ifstream file(csv_path_);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open CSV: " + csv_path_);
        }

        int count = -1; // -1 accounts for header
        std::string line;
        while (std::getline(file, line)) ++count;
        return count < 0 ? 0 : count;
    }

private:
    std::string csv_path_;

    // Reorder MM/DD/YYYY -> YYYY/MM/DD for lexicographic date comparison
    static std::string normalizeDate(const std::string& mdy) {
        // Expected format: MM/DD/YYYY or MM/DD/YYYY HH:MM:SS
        if (mdy.size() < 10) return mdy;
        // positions: 0-1 MM, 3-4 DD, 6-9 YYYY
        if (mdy[2] != '/' || mdy[5] != '/') return mdy;
        return mdy.substr(6, 4) + "/" + mdy.substr(0, 2) + "/" + mdy.substr(3, 2);
    }

    // Minimal CSV parser: handles quoted fields (fields may contain commas if quoted)
    static std::vector<std::string> splitCSV(const std::string& line) {
        std::vector<std::string> fields;
        bool in_quotes = false;
        std::string field;

        for (size_t i = 0; i < line.size(); ++i) {
            char c = line[i];
            if (c == '"') {
                if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                    // Escaped quote inside a quoted field
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
        fields.push_back(field); // last field
        return fields;
    }

    static bool parseBoolYN(const std::string& s) {
        return s == "Y" || s == "y";
    }

    static bool parseBoolYES(const std::string& s) {
        return s == "YES" || s == "Yes" || s == "yes";
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

    static double parseDouble(const std::string& s) {
        if (s.empty()) return 0.0;
        try { return std::stod(s); }
        catch (...) { return 0.0; }
    }

    DobPermitRecord parseCSVLine(const std::string& line, int line_num) {
        DobPermitRecord rec;
        std::vector<std::string> f = splitCSV(line);

        // The CSV has 60 columns. If a row is shorter, missing fields default.
        auto get = [&](size_t idx) -> const std::string& {
            static const std::string empty;
            return idx < f.size() ? f[idx] : empty;
        };

        rec.borough              = get(0);
        rec.bin_number           = parseInt32(get(1));
        rec.house_number         = get(2);
        rec.street_name          = get(3);
        rec.job_number           = parseInt64(get(4));
        rec.job_doc_number       = parseInt32(get(5));
        rec.job_type             = get(6);
        rec.self_cert            = parseBoolYN(get(7));
        rec.block                = get(8);
        rec.lot                  = get(9);
        rec.community_board      = parseInt32(get(10));
        rec.zip_code             = get(11);
        rec.bldg_type            = parseInt32(get(12));
        rec.residential          = parseBoolYES(get(13));
        // 14, 15: Special Districts (not stored in struct — not critical)
        rec.work_type            = get(16);
        rec.permit_status        = get(17);
        rec.filing_status        = get(18);
        rec.permit_type          = get(19);
        rec.permit_sequence      = parseInt32(get(20));
        rec.permit_subtype       = get(21);
        // 22: Oil Gas, 23: Site Fill (omitted)
        rec.filing_date          = get(24);
        rec.issuance_date        = get(25);
        rec.expiration_date      = get(26);
        rec.job_start_date       = get(27);
        rec.permittee_first_name = get(28);
        rec.permittee_last_name  = get(29);
        rec.permittee_business   = get(30);
        rec.permittee_phone      = get(31);
        rec.permittee_license_type = get(32);
        rec.permittee_license_num  = get(33);
        rec.act_as_super         = parseBoolYN(get(34));
        // 35: Other Title, 36: HIC License, 37-41: Site Safety / Super (omitted)
        rec.owner_business_name  = get(44);
        rec.owner_first_name     = get(45);
        rec.owner_last_name      = get(46);
        rec.non_profit           = parseBoolYN(get(43));
        // 47-52: Owner address / phone (omitted)
        rec.dob_run_date         = get(53);
        rec.permit_si_no         = parseInt64(get(54));
        rec.latitude             = parseDouble(get(55));
        rec.longitude            = parseDouble(get(56));
        rec.council_district     = parseInt32(get(57));
        rec.census_tract         = get(58);
        rec.nta_name             = get(59);

        return rec;
    }
};

#endif // DOB_DATA_LOADER_HPP
