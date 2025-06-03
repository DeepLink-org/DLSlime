#pragma once

#include "engine/assignment.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_context.h"

#include <array>
#include <cstdint>
#include <infiniband/verbs.h>
#include <memory>
#include <string>

namespace slime {

class RDMAEndpoint {
public:
    int register_memory_region(std::string mr_key, uintptr_t data_ptr, size_t length)
    {
        data_ctx_->register_memory_region(mr_key, data_ptr, length);
        mr_ctx_->register_memory_region(mr_key, (uintptr_t)(data_ctx_->memory_pool_.get_mr(mr_key)), sizeof(ibv_mr));
        return 0;
    }

    std::shared_ptr<RDMABuffer> post_recv_batch(AssignmentBatch& batch)
    {
        int32_t send_mr_slot = next_slot();

        // Step 1: send mr by mr_context;
        std::shared_ptr<RDMABuffer> buffer = std::make_shared<RDMABuffer>(batch);
        AssignmentBatch             mr_assign;
        for (int i = 0; i < batch.size(); ++i) {
            mr_assign.push_back(Assignment(batch[i].mr_key, 0, 0, sizeof(ibv_mr)));
        }

        std::function<void(int)> recv_data_callback = [this, &batch, &buffer](int code) {
            // Step 2: recv done signal
            data_ctx_->submit(OpCode::RECV, batch, [&buffer](int) {
                buffer->finished_.fetch_add(0);
                buffer->finished_cv_.notify_all();
            });
        };
        send_mr_callback[send_mr_slot] = recv_data_callback;

        mr_ctx_->submit_with_imm_data(OpCode::SEND_WITH_IMM, mr_assign, send_mr_slot, nullptr, [this](int imm_data) {
            send_mr_callback[imm_data](imm_data);
        });
        return buffer;
    }

    std::shared_ptr<RDMABuffer> post_send_batch(AssignmentBatch& batch)
    {
        int recv_mr_slot = next_slot();

        std::shared_ptr<RDMABuffer> buffer = std::make_shared<RDMABuffer>(batch);

        AssignmentBatch mr_assign_batch;
        AssignmentBatch write_assign_batch;
        for (int i = 0; i < batch.size(); ++i) {
            std::string remote_mr_key =
                batch[i].mr_key + "@slot_" + std::to_string(recv_mr_slot) + "#" + std::to_string(i);
            mr_ctx_->register_memory_region(remote_mr_key, (uintptr_t)(buffer->mr_list + i), sizeof(ibv_mr));
            mr_assign_batch.push_back(Assignment(remote_mr_key, 0, 0, sizeof(ibv_mr)));
            write_assign_batch.push_back(
                Assignment(remote_mr_key, batch[i].target_offset, batch[i].source_offset, batch[i].length));
        }

        std::function<void(int)> write_data_callback = [this, &batch, &buffer](int imm_data) {
            data_ctx_->submit_with_imm_data(OpCode::WRITE_WITH_IMM, batch, imm_data, nullptr, [&buffer](int) {
                buffer->finished_.fetch_add(0);
                buffer->finished_cv_.notify_all();
            });
        };

        recv_mr_callback[recv_mr_slot] = write_data_callback;
        mr_ctx_->submit_with_imm_data(OpCode::RECV, mr_assign_batch, 0, nullptr, [this, recv_mr_slot](int imm_data) {
            recv_mr_callback[recv_mr_slot](imm_data);
        });

        return buffer;
    }

    int next_slot(int num_to_skip = 1)
    {
        auto temp = slot_;
        slot_ += num_to_skip;
        return temp;
    }

private:
    int32_t slot_{0};

    std::map<int, std::function<void(int)>> send_mr_callback;
    std::map<int, std::function<void(int)>> recv_mr_callback;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> mr_ctx_;
};
}  // namespace slime
