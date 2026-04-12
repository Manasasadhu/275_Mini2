#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iostream>
#include <cstdlib>

struct EdgeConfig {
    std::string to;
    std::string host;
    int port;
    std::string relationship;
    std::string team;
};

struct ChunkConfig {
    int default_chunk_size;
    int max_chunk_size;
    int min_chunk_size;
};

struct DataPartitioning {
    std::string strategy;
    std::vector<std::string> owned_boroughs;  // e.g. ["MANHATTAN", "BROOKLYN"]
};

struct ProcessConfig {
    std::string process_id;
    std::string role;
    std::string listen_host;
    int listen_port;
    std::string data_path;   // Path to dob_permits CSV file
    std::string team;
    bool is_team_leader;
    std::vector<EdgeConfig> edges;
    DataPartitioning data_partitioning;
    ChunkConfig chunk_config;
};

class ConfigParser {
public:
    static ProcessConfig loadConfig(const std::string& config_file) {
        std::ifstream file(config_file);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open config file: " + config_file);
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string content = buffer.str();

        ProcessConfig config;

        config.process_id   = extractString(content, "process_id");
        config.role         = extractString(content, "role");
        config.listen_host  = extractString(content, "listen_host");
        config.listen_port  = extractInt(content, "listen_port");

        // DOB_DATA_PATH env var overrides config file
        const char* env_data_path = std::getenv("DOB_DATA_PATH");
        if (env_data_path && std::string(env_data_path).length() > 0) {
            config.data_path = std::string(env_data_path);
            std::cout << "Using data path from DOB_DATA_PATH env: " << config.data_path << std::endl;
        } else {
            config.data_path = extractString(content, "data_path");
        }

        config.team           = extractString(content, "\"team\"");
        config.is_team_leader = extractBool(content, "is_team_leader");
        config.edges          = extractEdges(content);

        config.data_partitioning.strategy        = extractString(content, "strategy");
        config.data_partitioning.owned_boroughs  = extractStringArray(content, "owned_boroughs");

        config.chunk_config.default_chunk_size = extractInt(content, "default_chunk_size");
        config.chunk_config.max_chunk_size     = extractInt(content, "max_chunk_size");
        config.chunk_config.min_chunk_size     = extractInt(content, "min_chunk_size");

        return config;
    }

private:
    static std::string extractString(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\":";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return "";

        pos += search.length();
        while (pos < json.length() && (json[pos] == ' ' || json[pos] == '\t')) pos++;

        if (json[pos] == 'n') return "";   // null
        if (json[pos] != '"') return "";

        pos++;
        size_t end = json.find('"', pos);
        return json.substr(pos, end - pos);
    }

    static int extractInt(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\":";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return 0;

        pos += search.length();
        while (pos < json.length() && (json[pos] == ' ' || json[pos] == '\t')) pos++;

        size_t end = pos;
        while (end < json.length() && (isdigit(json[end]) || json[end] == '-')) end++;

        return std::stoi(json.substr(pos, end - pos));
    }

    static bool extractBool(const std::string& json, const std::string& key) {
        std::string search = "\"" + key + "\":";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return false;

        pos += search.length();
        while (pos < json.length() && (json[pos] == ' ' || json[pos] == '\t')) pos++;

        return (json.substr(pos, 4) == "true");
    }

    static std::vector<std::string> extractStringArray(const std::string& json, const std::string& key) {
        std::vector<std::string> result;
        std::string search = "\"" + key + "\":";
        size_t pos = json.find(search);
        if (pos == std::string::npos) return result;

        pos = json.find('[', pos);
        if (pos == std::string::npos) return result;

        size_t end = json.find(']', pos);
        std::string array_content = json.substr(pos + 1, end - pos - 1);

        size_t current = 0;
        while (current < array_content.length()) {
            size_t q1 = array_content.find('"', current);
            if (q1 == std::string::npos) break;
            size_t q2 = array_content.find('"', q1 + 1);
            if (q2 == std::string::npos) break;
            result.push_back(array_content.substr(q1 + 1, q2 - q1 - 1));
            current = q2 + 1;
        }

        return result;
    }

    static std::vector<EdgeConfig> extractEdges(const std::string& json) {
        std::vector<EdgeConfig> edges;

        size_t edges_start = json.find("\"edges\":");
        if (edges_start == std::string::npos) return edges;

        size_t array_start = json.find('[', edges_start);
        if (array_start == std::string::npos) return edges;

        size_t array_end = json.find(']', array_start);
        std::string edges_content = json.substr(array_start + 1, array_end - array_start - 1);

        size_t pos = 0;
        while (pos < edges_content.length()) {
            size_t obj_start = edges_content.find('{', pos);
            if (obj_start == std::string::npos) break;
            size_t obj_end = edges_content.find('}', obj_start);
            if (obj_end == std::string::npos) break;

            std::string edge_json = edges_content.substr(obj_start, obj_end - obj_start + 1);

            EdgeConfig edge;
            edge.to           = extractString(edge_json, "to");
            edge.host         = extractString(edge_json, "host");
            edge.port         = extractInt(edge_json, "port");
            edge.relationship = extractString(edge_json, "relationship");
            edge.team         = extractString(edge_json, "team");

            edges.push_back(edge);
            pos = obj_end + 1;
        }

        return edges;
    }
};

#endif // CONFIG_HPP
