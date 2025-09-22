#pragma once

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <numa.h>

namespace slime {

std::vector<std::string> available_nic();

int get_gid_index(std::string dev_name);

}  // namespace slime
