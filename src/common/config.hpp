#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>

struct Shard {
    std::string type;
    std::string start;
    std::string end;
    std::vector<std::string> counties;
};

struct NodeConfig {
    std::string node_id;
    std::string host;         // address other nodes use to reach this node
    std::string listen_host;  // address this node binds to (0.0.0.0 for multi-host)
    int port;
    std::vector<std::string> neighbors;
    std::string language;
    std::string role;
    bool client_facing;
    int chunk_size;
    std::string data_file;
    std::string global_data_file;
    std::string date_field;
    Shard* shard;  // nullptr if null
    std::map<std::string, std::string> neighbor_hosts;
    std::map<std::string, int> neighbor_ports;
};

static std::string resolve_path(const std::string& path, const std::string& base_dir) {
    if (path.empty() || path[0] == '/') return path;
    return (std::filesystem::path(base_dir) / path).string();
}

NodeConfig load_node_config(const std::string& config_path, const std::string& node_id) {
    std::ifstream f(config_path);
    if (!f.is_open()) {
        throw std::runtime_error("Failed to open config file: " + config_path);
    }
    std::string base_dir = std::filesystem::path(config_path).parent_path().parent_path().string();
    nlohmann::json j;
    f >> j;
    if (!j.contains("nodes") || !j["nodes"].contains(node_id)) {
        throw std::runtime_error("Node ID not found in config: " + node_id);
    }
    auto global = j["global"];
    auto node = j["nodes"][node_id];
    NodeConfig config;
    config.node_id = node["node_id"];
    config.host = node["host"];
    config.listen_host = node.value("listen_host", "0.0.0.0");
    config.port = node["port"];
    config.neighbors = node["neighbors"].get<std::vector<std::string>>();
    config.language = node["language"];
    config.role = node["role"];
    config.client_facing = node["client_facing"];
    config.chunk_size = node.value("chunk_size", global["default_chunk_size"].get<int>());
    config.global_data_file = resolve_path(global["shared_data_file"], base_dir);
    config.date_field = global["date_field"];
    if (node["data_file"].is_null()) {
        config.data_file = config.global_data_file;
    } else {
        config.data_file = resolve_path(node["data_file"], base_dir);
    }
    if (node["shard"].is_object()) {
        auto s = node["shard"];
        Shard* shard = new Shard();
        shard->type = s["type"];
        if (shard->type == "county") {
            shard->counties = s["counties"].get<std::vector<std::string>>();
        } else {
            shard->start = s["start"];
            shard->end = s["end"];
        }
        config.shard = shard;
    } else {
        config.shard = nullptr;
    }

    for (const auto& [nid, ndata] : j["nodes"].items()) {
        config.neighbor_hosts[nid] = ndata["host"].get<std::string>();
        config.neighbor_ports[nid] = ndata["port"].get<int>();
    }

    return config;
}

#endif // CONFIG_HPP
