#include "dlslime/csrc/topology.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace dlslime::topology {

namespace {

namespace fs = std::filesystem;

std::string trim(std::string value)
{
    auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [&](char ch) { return !is_space(ch); }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [&](char ch) { return !is_space(ch); }).base(), value.end());
    return value;
}

std::string toUpper(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

std::string readText(const fs::path& path)
{
    std::ifstream in(path);
    if (!in.is_open()) {
        throw std::runtime_error("cannot open " + path.string());
    }
    std::string value;
    std::getline(in, value);
    return trim(value);
}

std::optional<std::string> tryReadText(const fs::path& path)
{
    try {
        return readText(path);
    }
    catch (...) {
        return std::nullopt;
    }
}

std::vector<std::string> visibleDevices()
{
    const char* env = std::getenv("SLIME_VISIBLE_DEVICES");
    if (env == nullptr || std::string(env).empty()) {
        return {};
    }

    std::vector<std::string> devices;
    std::stringstream        stream(env);
    std::string              item;
    while (std::getline(stream, item, ',')) {
        item = trim(item);
        if (!item.empty()) {
            devices.push_back(item);
        }
    }
    return devices;
}

fs::path infinibandRoot(const std::string& sysfs_root)
{
    return fs::path(sysfs_root) / "class" / "infiniband";
}

fs::path deviceRoot(const std::string& sysfs_root, const std::string& device)
{
    return infinibandRoot(sysfs_root) / device;
}

std::string normalizePortState(const std::string& value)
{
    std::string state = value;
    auto        pos   = state.find(':');
    if (pos != std::string::npos) {
        state = state.substr(pos + 1);
    }
    state = toUpper(trim(state));
    return state.empty() ? "UNKNOWN" : state;
}

int64_t nowSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

json localHost()
{
    std::array<char, 256> hostname_buf{};
    std::string           hostname = "localhost";
    if (gethostname(hostname_buf.data(), hostname_buf.size()) == 0) {
        hostname_buf.back() = '\0';
        hostname            = hostname_buf.data();
    }

    std::string address = hostname;
    addrinfo    hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    if (getaddrinfo(hostname.c_str(), nullptr, &hints, &result) == 0) {
        std::string first_address;
        for (addrinfo* item = result; item != nullptr; item = item->ai_next) {
            auto* addr = reinterpret_cast<sockaddr_in*>(item->ai_addr);
            char  ip[INET_ADDRSTRLEN]{};
            if (inet_ntop(AF_INET, &addr->sin_addr, ip, sizeof(ip)) == nullptr) {
                continue;
            }
            std::string candidate = ip;
            if (first_address.empty()) {
                first_address = candidate;
            }
            if (candidate.rfind("127.", 0) != 0) {
                address = candidate;
                break;
            }
        }
        if (address == hostname && !first_address.empty()) {
            address = first_address;
        }
        freeaddrinfo(result);
    }

    return {{"hostname", hostname}, {"address", address}};
}

}  // namespace

std::string normalizeLinkType(const std::string& link_type)
{
    std::string value = trim(link_type);
    std::string lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });

    if (lower == "ethernet" || lower == "roce") {
        return "RoCE";
    }
    if (lower == "infiniband" || lower == "ib") {
        return "IB";
    }
    if (value.empty()) {
        return "UNKNOWN";
    }
    return value;
}

std::vector<std::string> listRdmaDevices(const std::string& sysfs_root)
{
    std::vector<std::string> devices;
    fs::path                 root = infinibandRoot(sysfs_root);
    std::error_code          ec;
    if (!fs::exists(root, ec)) {
        return devices;
    }

    for (const auto& entry : fs::directory_iterator(root, ec)) {
        if (ec) {
            break;
        }
        devices.push_back(entry.path().filename().string());
    }

    std::vector<std::string> visible = visibleDevices();
    if (!visible.empty()) {
        devices.erase(std::remove_if(devices.begin(),
                                     devices.end(),
                                     [&](const std::string& device) {
                                         return std::find(visible.begin(), visible.end(), device) == visible.end();
                                     }),
                      devices.end());
    }

    std::sort(devices.begin(), devices.end());
    return devices;
}

json readSysfsPort(const std::string& device, int ib_port, const std::string& sysfs_root)
{
    fs::path port_root = deviceRoot(sysfs_root, device) / "ports" / std::to_string(ib_port);

    json port = {
        {"port", ib_port},
        {"state", "UNKNOWN"},
        {"link_type", "UNKNOWN"},
    };

    if (auto value = tryReadText(port_root / "state")) {
        port["state"] = normalizePortState(*value);
    }

    if (auto value = tryReadText(port_root / "link_layer")) {
        port["link_type"] = normalizeLinkType(*value);
    }

    if (auto value = tryReadText(port_root / "active_mtu")) {
        try {
            port["active_mtu"] = std::stoi(*value);
        }
        catch (...) {
        }
    }

    return port;
}

json discoverTopology(const std::optional<std::string>&              preferred_device,
                      int                                            ib_port,
                      const std::optional<std::string>&              preferred_link_type,
                      const std::string&                             sysfs_root,
                      const std::optional<std::vector<std::string>>& devices_override)
{
    std::vector<std::string> devices = devices_override.value_or(listRdmaDevices(sysfs_root));

    if (preferred_device && !preferred_device->empty()) {
        std::vector<std::string> ordered;
        if (std::find(devices.begin(), devices.end(), *preferred_device) != devices.end()) {
            ordered.push_back(*preferred_device);
        }
        for (const auto& device : devices) {
            if (device != *preferred_device) {
                ordered.push_back(device);
            }
        }
        devices = std::move(ordered);
    }

    if (devices.empty()) {
        throw std::runtime_error("No RDMA devices available");
    }

    json nics = json::array();
    for (const auto& device : devices) {
        json port = readSysfsPort(device, ib_port, sysfs_root);
        if (port.value("link_type", "UNKNOWN") == "UNKNOWN") {
            port["link_type"] = preferred_link_type ? normalizeLinkType(*preferred_link_type) : "UNKNOWN";
        }
        if (port.value("state", "UNKNOWN") == "UNKNOWN") {
            port["state"] = "ACTIVE";
        }

        int numa_node = -1;
        try {
            std::string value = readText(fs::weakly_canonical(deviceRoot(sysfs_root, device) / "device") / "numa_node");
            numa_node         = std::stoi(value);
        }
        catch (...) {
        }

        std::string pci_bus_id;
        try {
            pci_bus_id = fs::weakly_canonical(deviceRoot(sysfs_root, device) / "device").filename().string();
        }
        catch (...) {
        }

        nics.push_back({
            {"name", device},
            {"health", port.value("state", "ACTIVE") == "ACTIVE" ? "AVAILABLE" : "DEGRADED"},
            {"numa_node", numa_node},
            {"pci_bus_id", pci_bus_id},
            {"ports", json::array({port})},
        });
    }

    return {
        {"schema_version", 1},
        {"host", localHost()},
        {"nics", nics},
        {"accelerators", json::array()},
        {"memory_keys", json::array()},
        {"topology_epoch", nowSeconds()},
    };
}

}  // namespace dlslime::topology
