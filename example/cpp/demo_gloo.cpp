#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/socket.h>

#include "gloo/allreduce_ring.h"
#include "gloo/rendezvous/context.h"
#include "gloo/rendezvous/file_store.h"
#include "gloo/rendezvous/prefix_store.h"

#include "gloo/transport/ibverbs/device.h"

int main(int argc, char** argv)
{
    // Unrelated to the example: perform some sanity checks.
    if (getenv("PREFIX") == nullptr || getenv("SIZE") == nullptr || getenv("RANK") == nullptr) {
        std::cerr << "Please set environment variables PREFIX, SIZE, and RANK." << std::endl;
        return 1;
    }

    std::shared_ptr<gloo::rendezvous::FileStore> fileStore = std::make_shared<gloo::rendezvous::FileStore>("/tmp");

    // context->connectFullMesh(prefixStore, dev);

    gloo::transport::ibverbs::attr attr;
    attr.name  = "mlx5_bond_0";
    attr.port  = 1;
    attr.index = 3;

    auto dev = gloo::transport::ibverbs::CreateDevice(attr);

    std::string prefix      = getenv("PREFIX");
    auto        prefixStore = std::make_shared<gloo::rendezvous::PrefixStore>(prefix, fileStore);

    const int rank    = atoi(getenv("RANK"));
    const int size    = atoi(getenv("SIZE"));
    auto      context = std::make_shared<gloo::rendezvous::Context>(rank, size);
    context->connectFullMesh(prefixStore, dev);

    // All connections are now established. We can now initialize some
    // test data, instantiate the collective algorithm, and run it.
    std::array<int, 4> data;
    std::cout << "Input: " << std::endl;
    for (int i = 0; i < data.size(); i++) {
        data[i] = i;
        std::cout << "data[" << i << "] = " << data[i] << std::endl;
    }

    // Allreduce operates on memory that is already managed elsewhere.
    // Every instance can take multiple pointers and perform reduction
    // across local buffers as well. If you have a single buffer only,
    // you must pass a std::vector with a single pointer.
    std::vector<int*> ptrs;
    ptrs.push_back(&data[0]);

    // The number of elements at the specified pointer.
    int count = data.size();

    // Instantiate the collective algorithm.
    auto allreduce = std::make_shared<gloo::AllreduceRing<int>>(context, ptrs, count);

    // Run the algorithm.
    allreduce->run();

    // Print the result.
    std::cout << "Output: " << std::endl;
    for (int i = 0; i < data.size(); i++) {
        std::cout << "data[" << i << "] = " << data[i] << std::endl;
    }
}
