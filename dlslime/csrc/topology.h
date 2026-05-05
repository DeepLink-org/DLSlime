#pragma once

#include <optional>
#include <string>
#include <vector>

#include "dlslime/csrc/common/json.hpp"

namespace dlslime::topology {

using json = nlohmann::json;

std::string normalizeLinkType(const std::string& link_type);

std::vector<std::string> listRdmaDevices(const std::string& sysfs_root = "/sys");

json readSysfsPort(const std::string& device, int ib_port, const std::string& sysfs_root = "/sys");

json discoverTopology(const std::optional<std::string>&              preferred_device    = std::nullopt,
                      int                                            ib_port             = 1,
                      const std::optional<std::string>&              preferred_link_type = std::nullopt,
                      const std::string&                             sysfs_root          = "/sys",
                      const std::optional<std::vector<std::string>>& devices_override    = std::nullopt);

}  // namespace dlslime::topology
