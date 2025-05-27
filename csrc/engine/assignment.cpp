#include <cstdint>
#include <functional>
#include <vector>

#include "utils/logging.h"

#include "assignment.h"

namespace slime {
json Assignment::dump() const
{
    return json{
        "Assignment",
        {{"mr_key", mr_key}, {"target_offset", target_offset}, {"source_offset", source_offset}, {"length", length}}};
}

std::ostream& operator<<(std::ostream& os, const Assignment& assignment) {
    os << assignment.dump().dump(2);
    return os;
}

}  // namespace slime
