#include <iostream>
#include <cassert>
#include <cstring> // for memset, strncpy, strcmp
#include <cstdlib> // for posix_memalign, free
#include <cstdio>  // for snprintf
#include "dlslime/jring.h"

// --------------------------------------------------------------------------
// Definition of a dummy communication packet.
//
// Design considerations:
// 1. Total size is 32 bytes (4 + 4 + 24).
// 2. Must be a multiple of 4 bytes as required by jring element size.
// 3. Uses a fixed-size array for payload to support direct memcpy logic.
// --------------------------------------------------------------------------
struct CommPacket {
    uint32_t seq_num;     // Sequence number to detect ordering issues
    uint32_t msg_type;    // Message type ID (e.g., 0x01: Data, 0x02: Heartbeat)
    char     payload[24]; // Fixed-size payload buffer
};

int main() {
    // ----------------------------------------------------------------------
    // 1. Configuration and Memory Allocation
    // ----------------------------------------------------------------------

    // Ring depth must be a power of 2
    const uint32_t RING_COUNT = 1024;
    // Element size of our packet structure
    const uint32_t ELEM_SIZE = sizeof(CommPacket);

    // Calculate the total memory required for the ring structure + data
    size_t mem_size = jring_get_buf_ring_size(ELEM_SIZE, RING_COUNT);
    assert(mem_size != (size_t)-1 && "Ring size calculation failed (alignment issue?)");

    void* mem_ptr = nullptr;
    // Allocate memory aligned to 64 bytes (CACHE_LINE_SIZE) for performance
    int alloc_ret = posix_memalign(&mem_ptr, 64, mem_size);
    if (alloc_ret != 0) {
        std::cerr << "Memory allocation failed." << std::endl;
        return -1;
    }

    struct jring* r = static_cast<struct jring*>(mem_ptr);

    // Initialize the ring.
    // Parameters: ring_ptr, count, element_size, single_producer(0), single_consumer(0)
    if (jring_init(r, RING_COUNT, ELEM_SIZE, 0, 0) < 0) {
        std::cerr << "jring_init failed." << std::endl;
        free(mem_ptr);
        return -1;
    }

    std::cout << "[Setup] Ring initialized. Capacity: " << r->capacity
              << ", Element Size: " << ELEM_SIZE << " bytes." << std::endl;

    // ----------------------------------------------------------------------
    // 2. Prepare Test Data (Producer Side)
    // ----------------------------------------------------------------------
    const int BATCH_SIZE = 16;
    CommPacket tx_buffer[BATCH_SIZE];

    for (int i = 0; i < BATCH_SIZE; ++i) {
        tx_buffer[i].seq_num = 1000 + i;
        tx_buffer[i].msg_type = (i % 2 == 0) ? 0xA0 : 0xB0;

        // Clear payload and write a dummy string
        memset(tx_buffer[i].payload, 0, sizeof(tx_buffer[i].payload));
        snprintf(tx_buffer[i].payload, sizeof(tx_buffer[i].payload), "PACKET_ID_%d", i);
    }

    // ----------------------------------------------------------------------
    // 3. Enqueue Operation (Single Producer)
    // ----------------------------------------------------------------------

    // jring copies data from tx_buffer into the internal ring memory.
    // 'bulk' means it writes all items or returns 0 if space is insufficient.
    unsigned int enq_count = jring_sp_enqueue_bulk(r, tx_buffer, BATCH_SIZE, nullptr);

    if (enq_count != BATCH_SIZE) {
        std::cerr << "[Error] Failed to enqueue packets." << std::endl;
        free(mem_ptr);
        return -1;
    }
    std::cout << "[Producer] Successfully enqueued " << enq_count << " packets." << std::endl;

    // Verify ring status
    assert(!jring_empty(r));
    assert(jring_count(r) == BATCH_SIZE);

    // ----------------------------------------------------------------------
    // 4. Dequeue Operation (Single Consumer)
    // ----------------------------------------------------------------------
    CommPacket rx_buffer[BATCH_SIZE];

    // Dequeue data from ring into rx_buffer
    unsigned int deq_count = jring_sc_dequeue_bulk(r, rx_buffer, BATCH_SIZE, nullptr);

    assert(deq_count == BATCH_SIZE);
    std::cout << "[Consumer] Successfully dequeued " << deq_count << " packets." << std::endl;

    // ----------------------------------------------------------------------
    // 5. Data Integrity Verification
    // ----------------------------------------------------------------------
    for (int i = 0; i < BATCH_SIZE; ++i) {
        // Verify sequence number
        if (rx_buffer[i].seq_num != 1000 + i) {
            std::cerr << "[Error] SeqNum mismatch at index " << i << std::endl;
            exit(1);
        }

        // Verify message type
        uint32_t expected_type = (i % 2 == 0) ? 0xA0 : 0xB0;
        if (rx_buffer[i].msg_type != expected_type) {
            std::cerr << "[Error] Type mismatch at index " << i << std::endl;
            exit(1);
        }

        // Verify payload content
        char expected_str[24];
        snprintf(expected_str, sizeof(expected_str), "PACKET_ID_%d", i);
        if (strncmp(rx_buffer[i].payload, expected_str, sizeof(expected_str)) != 0) {
            std::cerr << "[Error] Payload corruption at index " << i << std::endl;
            std::cerr << "   Expected: " << expected_str << std::endl;
            std::cerr << "   Actual:   " << rx_buffer[i].payload << std::endl;
            exit(1);
        }
    }

    std::cout << "[Pass] Data integrity check passed. All tests finished." << std::endl;

    // ----------------------------------------------------------------------
    // 6. Cleanup
    // ----------------------------------------------------------------------
    free(mem_ptr);
    return 0;
}
