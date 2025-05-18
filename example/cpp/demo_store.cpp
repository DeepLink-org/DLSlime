#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/socket.h>

#include "gloo/rendezvous/context.h"
#include "gloo/rendezvous/file_store.h"
#include "gloo/rendezvous/prefix_store.h"

int main(int argc, char** argv)
{
    // Unrelated to the example: perform some sanity checks.
    if (getenv("PREFIX") == nullptr || getenv("SIZE") == nullptr || getenv("RANK") == nullptr) {
        std::cerr << "Please set environment variables PREFIX, SIZE, and RANK." << std::endl;
        return 1;
    }

    std::shared_ptr<gloo::rendezvous::FileStore> fileStore =
        std::make_shared<gloo::rendezvous::FileStore>("/tmp");

    std::string prefix      = getenv("PREFIX");
    auto        prefixStore = std::make_shared<gloo::rendezvous::PrefixStore>(prefix, fileStore);

    const int rank = atoi(getenv("RANK"));
    const int size = atoi(getenv("SIZE"));
    auto context = std::make_shared<gloo::rendezvous::Context>(rank, size);
    // context->connectFullMesh(prefixStore, dev);


}
