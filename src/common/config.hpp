#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <nlohmann/json.hpp>

struct Shard {
    std::string type;
    std::string start;
    std::string end;
};

struct NodeConfig {
    std::string node_id;
    std::string host;
    int port;
    std::vector<std::string> neighbors;
    std::string language;
    std::string role;
    bool client_facing;
    int chunk_size;
    std::string data_file;
    std::string global_data_file;
    std::string date_field;
    Shard* shard;
    std::map<std::string, int> neighbor_ports;
};

NodeConfig load_node_config(const std::string& config_path, const std::string& node_id) {
    std::ifstream f(config_path);
    if (!f.is_open()) {
        throw std::runtime_error("Failed to open config file: " + config_path);
    }
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
    config.port = node["port"];
    config.neighbors = node["neighbors"].get<std::vector<std::string>>();
    config.language = node["language"];
    config.role = node["role"];
    config.client_facing = node["client_facing"];
    config.chunk_size = node.value("chunk_size", global["default_chunk_size"].get<int>());
    config.global_data_file = global["shared_data_file"];
    config.date_field = global["date_field"];
    if (node["data_file"].is_null()) {
        config.data_file = config.global_data_file;
    } else {
        config.data_file = node["data_file"];
    }
    if (node["shard"].is_object()) {
        auto s = node["shard"];
        config.shard = new Shard{s["type"], s["start"], s["end"]};
    } else {
        config.shard = nullptr;
    }

    for (const auto& [nid, ndata] : j["nodes"].items()) {
        config.neighbor_ports[nid] = ndata["port"].get<int>();
    }

    return config;
}

#endif // CONFIG_HPP
