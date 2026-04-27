#include "metrics.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <mutex>
#include <filesystem>

namespace metrics {

static std::string  process_id_;
static std::string  role_;
static std::ofstream log_file_;
static std::mutex   log_mutex_;

static std::string timestamp() {
    auto now = std::chrono::system_clock::now();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                   now.time_since_epoch()).count() % 1000;
    std::time_t t  = std::chrono::system_clock::to_time_t(now);
    std::tm     tm = *std::localtime(&t);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &tm);
    std::ostringstream oss;
    oss << buf << "." << std::setw(3) << std::setfill('0') << ms;
    return oss.str();
}

void init(const std::string& process_id, const std::string& role) {
    process_id_ = process_id;
    role_       = role;
    namespace fs = std::filesystem;
    fs::create_directories("logs");
    log_file_.open("logs/" + process_id + "_" + role + ".log", std::ios::app);
    log_event("INIT", "", 0, 0, -1, -1, "process started");
}

void log_event(const std::string& event,
               const std::string& request_id,
               int pending, int /*workers*/,
               int chunk_num, int record_count,
               const std::string& extra) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    std::ostringstream line;
    line << timestamp()
         << " [" << process_id_ << "] "
         << std::left << std::setw(22) << event
         << " req="     << (request_id.empty() ? "-" : request_id)
         << " pending=" << pending
         << " chunk="   << chunk_num
         << " records=" << record_count;
    if (!extra.empty()) line << " " << extra;

    std::cout << line.str() << "\n";
    if (log_file_.is_open()) log_file_ << line.str() << "\n";
}

void shutdown() {
    log_event("SHUTDOWN", "", 0, 0, -1, -1, "process stopping");
    if (log_file_.is_open()) log_file_.close();
}

} // namespace metrics