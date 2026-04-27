#ifndef METRICS_HPP
#define METRICS_HPP

#include <string>

namespace metrics {
    void init(const std::string& process_id, const std::string& role);
    void log_event(const std::string& event,
                   const std::string& request_id,
                   int pending, int workers,
                   int chunk_num, int record_count,
                   const std::string& extra = "");
    void shutdown();
}

#endif // METRICS_HPP