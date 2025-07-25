#pragma once

#include <string>
#include <vector>

namespace slime {

std::vector<std::string> available_nic();

int get_gid_index(std::string dev_name);

}  // namespace slime
