// #pragma once
// #include "engine/rdma/rdma_context.h"
// #include <memory>
// #include <mutex>
// #include <vector>
// #include <atomic>
// #include <infiniband/verbs.h>

// namespace slime {

// typedef struct MetaData {
//     uint64_t addr;   // 内存地址
//     uint32_t rkey;   // 远程键
//     uint32_t length; // 数据长度
//     uint32_t index;  // 批次索引
// } meta_data_t;

// class RDMAEndpoint : public std::enable_shared_from_this<RDMAEndpoint> {
// private:
//     struct RegisteredMemory {
//         std::shared_ptr<RDMAContext> ctx;
//         ibv_mr* mr = nullptr;
//         std::shared_ptr<char[]> buffer;
//         std::string key;

//         ~RegisteredMemory() {
//             if (mr && ctx) {
//                 ctx->memory_pool_->unregister_memory_region(key);
//                 // MR将由memory_pool统一管理释放
//             }
//         }
//     };

//     std::mutex mem_mutex_;
//     std::vector<std::shared_ptr<RegisteredMemory>> active_regions_;
//     std::atomic<bool> connected_{false};

//     std::shared_ptr<RegisteredMemory> register_memory(
//         std::shared_ptr<RDMAContext> ctx,
//         const std::string& key,
//         size_t size) 
//     {
//         auto mem = std::make_shared<RegisteredMemory>();
//         mem->ctx = ctx;
//         mem->key = key;
//         mem->buffer.reset(new char[size], [](char* p) { delete[] p; });
//         memset(mem->buffer.get(), 0, size);

//         // 注册内存区域
//         if (ctx->register_memory_region(
//             key, 
//             reinterpret_cast<uintptr_t>(mem->buffer.get()), 
//             size) != 0) 
//         {
//             SLIME_LOG_ERROR("Failed to register memory region: " << key);
//             return nullptr;
//         }

//         // 获取MR指针（假设memory_pool已存储）
//         mem->mr = ctx->memory_pool_->get_mr(key);
//         if (!mem->mr) {
//             SLIME_LOG_ERROR("Failed to get MR after registration: " << key);
//             return nullptr;
//         }

//         std::lock_guard<std::mutex> lock(mem_mutex_);
//         active_regions_.push_back(mem);
//         return mem;
//     }

// public:
//     std::shared_ptr<RDMAContext> data_ctx;
//     std::shared_ptr<RDMAContext> mem_region_ctx;

//     RDMAEndpoint() : 
//         data_ctx(std::make_shared<RDMAContext>()),
//         mem_region_ctx(std::make_shared<RDMAContext>()) {}
    
//     ~RDMAEndpoint() {
//         disconnect();
//     }

//     void init(const std::string& dev_name, uint8_t ib_port, const std::string& link_type) {
//         if (data_ctx->init(dev_name, ib_port, link_type) != 0 ||
//             mem_region_ctx->init(dev_name, ib_port, link_type) != 0) 
//         {
//             SLIME_ABORT("RDMA context initialization failed");
//         }
//     }

//     void connect(const json& EP_DATA, const json& EP_MR) {
//         if (connected_) return;

//         if (data_ctx->connect(EP_DATA) != 0 ||
//             mem_region_ctx->connect(EP_MR) != 0) 
//         {
//             SLIME_ABORT("RDMA connection failed");
//         }

//         data_ctx->launch_future();
//         mem_region_ctx->launch_future();
//         connected_.store(true);
//     }

//     void disconnect() {
//         if (!connected_) return;

//         {
//             std::lock_guard<std::mutex> lock(mem_mutex_);
//             active_regions_.clear(); // 触发所有RegisteredMemory的析构
//         }

//         data_ctx->stop_future();
//         mem_region_ctx->stop_future();
//         connected_.store(false);
//     }

//     void send(AssignmentBatch& send_batch) {
//         if (!connected_) {
//             SLIME_LOG_ERROR("Endpoint not connected");
//             return;
//         }

//         auto self = shared_from_this();
//         size_t batch_size = send_batch.size();
//         size_t meta_size = batch_size * sizeof(meta_data_t);

//         // 注册元数据内存区域
//         auto mem = register_memory(mem_region_ctx, "SEND_META_" + std::to_string(reinterpret_cast<uintptr_t>(this)), meta_size);
//         if (!mem) return;

//         // 准备接收元数据的批次
//         AssignmentBatch meta_recv_batch;
//         for (size_t i = 0; i < batch_size; i++) {
//             meta_recv_batch.push_back(Assignment(
//                 mem->key,
//                 i * sizeof(meta_data_t),
//                 0,
//                 sizeof(meta_data_t)
//             ));
//         }

//         // 提交接收操作
//         mem_region_ctx->submit(
//             OpCode::RECV,
//             meta_recv_batch,
//             [this, self, mem, batch_size, &send_batch](int code) {
//                 if (code != 0) {
//                     SLIME_LOG_ERROR("Meta data receive failed");
//                     return;
//                 }

//                 // 处理元数据
//                 meta_data_t* meta_data = reinterpret_cast<meta_data_t*>(mem->buffer.get());
//                 for (size_t i = 0; i < batch_size; i++) {
//                     send_batch[i].remote_addr = meta_data[i].addr;
//                     send_batch[i].remote_rkey = meta_data[i].rkey;
//                     send_batch[i].length = meta_data[i].length;
//                 }

//                 // 提交写操作
//                 auto atx = data_ctx->submit(OpCode::WRITE_WITH_IMM, send_batch, nullptr);
//                 atx->wait();

//                 // 自动释放mem（通过shared_ptr引用计数）
//             }
//         );
//     }

//     void recv(AssignmentBatch& recv_batch) {
//         if (!connected_) {
//             SLIME_LOG_ERROR("Endpoint not connected");
//             return;
//         }

//         auto self = shared_from_this();
//         size_t batch_size = recv_batch.size();
//         size_t meta_size = batch_size * sizeof(meta_data_t);

//         // 注册元数据内存区域
//         auto mem = register_memory(mem_region_ctx, "RECV_META_" + std::to_string(reinterpret_cast<uintptr_t>(this)), meta_size);
//         if (!mem) return;

//         // 准备元数据内容
//         std::vector<meta_data_t> meta_data(batch_size);
//         for (size_t i = 0; i < batch_size; ++i) {
//             auto mr = data_ctx->memory_pool_->get_mr(recv_batch[i].mr_key);
//             meta_data[i] = {
//                 .addr   = reinterpret_cast<uint64_t>(mr->addr),
//                 .rkey   = mr->rkey,
//                 .length = static_cast<uint32_t>(mr->length),
//                 .index  = static_cast<uint32_t>(i)
//             };
//         }
//         memcpy(mem->buffer.get(), meta_data.data(), meta_size);

//         // 准备发送批次
//         AssignmentBatch meta_send_batch;
//         for (size_t i = 0; i < batch_size; ++i) {
//             meta_send_batch.push_back(Assignment(
//                 mem->key,
//                 0,
//                 i * sizeof(meta_data_t),
//                 sizeof(meta_data_t)
//             ));
//         }

//         // 提交发送操作
//         mem_region_ctx->submit(
//             OpCode::SEND,
//             meta_send_batch,
//             [this, self, mem, &recv_batch](int code) {
//                 if (code != 0) {
//                     SLIME_LOG_ERROR("Meta data send failed");
//                     return;
//                 }

//                 auto atx = data_ctx->submit(OpCode::RECV, recv_batch, nullptr);
//                 atx->wait();

//                 // 自动释放mem
//             }
//         );
//     }

//     bool is_connected() const { return connected_.load(); }
// };

// } // namespace slime



#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_buffer.h"


#include <condition_variable>
#include <mutex>
#include <vector>
#include <map>
#include <infiniband/verbs.h>

namespace slime {


typedef struct meta_data{

    uint64_t addr;   
    uint32_t rkey;   
    uint32_t len; 
    uint32_t idx;  
    uint8_t padding[4]; // 添加必要的填充字节以确保对齐（如果需要）

} __attribute__((packed)) meta_data_t;  


// TODO:
// 0. mem_region_ctx和data_ctx的异步
// 1. check mr的正确性，在非连续内存的情况下
// 2. meta_data pool：事先构造出一个固定大小的meta_data queue，然后进行出队和入队操作来管理meta_data的发送和接收
// 3. 多线程情况下如何进行调优
// 4. buffer的作用


// 

class RDMAEndpoint{

    public:

         RDMAEndpoint() : data_ctx(std::make_shared<RDMAContext>()), mem_region_ctx(std::make_shared<RDMAContext>()) {}
        ~RDMAEndpoint() = default;

         void init(const std::string &dev_name, uint8_t ib_port, const std::string &link_type)
         {
            data_ctx->init(dev_name, ib_port, link_type);
            mem_region_ctx->init(dev_name, ib_port, link_type);
         }


         void connect(const json& EP_DATA, const json& EP_MR)
         {
            data_ctx->connect(EP_DATA); 
            mem_region_ctx->connect(EP_MR); 

            data_ctx->launch_future();
            mem_region_ctx->launch_future();
         }

  
        void recv(AssignmentBatch& recv_batch)
        {   
            size_t batch_size  = recv_batch.size();
            
            // 注意这里需要每一次发送都需要动态申请
            // 更高性能的做法是直接申请一个固定大小的meta_data_buf, 然后进行出队和入队操作来管理meta_data的发送(meta_data pool)
            // 另外对于batch内的内存是否是连续的?
            auto recv_meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
            for(size_t i = 0; i < batch_size; ++i)
            {
                recv_meta_data_buf[i].addr = reinterpret_cast<uint64_t>(data_ctx->memory_pool_->get_mr(recv_batch[i].mr_key)->addr); //转换为decimal
                recv_meta_data_buf[i].rkey = data_ctx->memory_pool_->get_mr(recv_batch[i].mr_key)->rkey;
                recv_meta_data_buf[i].len  = recv_batch[i].length; //data_ctx->memory_pool_->get_mr(recv_batch[i].mr_key)->length;
                recv_meta_data_buf[i].idx  = i;
            }

            
            mem_region_ctx->register_memory_region("RECV_Meta_DATA_KEY", 
                                                    reinterpret_cast<uintptr_t>(recv_meta_data_buf.get()), //整个batch的首地址
                                                    batch_size * sizeof(meta_data_t));


            // debug: check the mr of mem_region_ctx 
                
            AssignmentBatch recv_mem_region_assign(batch_size); 
            for(size_t i = 0; i < batch_size; ++i)
                recv_mem_region_assign[i] = Assignment(
                    "RECV_Meta_DATA_KEY",
                    0,
                    i * sizeof(meta_data_t),
                    sizeof(meta_data_t)
                );
         
            // 先测试mr的发送
            auto mr_atx = mem_region_ctx->submit(OpCode::SEND, recv_mem_region_assign);
            mr_atx->wait();

            std::cout<< "mr send" << std::endl;
            auto data_atx = data_ctx->submit(OpCode::RECV, recv_batch);
            data_atx->wait();
 
        }


        void send(AssignmentBatch& send_batch) {

            size_t batch_size = send_batch.size();
            auto send_meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
            mem_region_ctx->register_memory_region("SEND_Meta_DATA_KEY", 
                                                    reinterpret_cast<uintptr_t>(send_meta_data_buf.get()), 
                                                    batch_size * sizeof(meta_data_t));
            

                                                    
            AssignmentBatch send_mem_region_assign(batch_size); 
            for(size_t i = 0; i < batch_size; ++i)
                send_mem_region_assign[i] = Assignment(
                    "SEND_Meta_DATA_KEY",
                    0,
                    reinterpret_cast<uint64_t>(i * sizeof(meta_data_t)),
                    sizeof(meta_data_t)
                );
            

            auto mr_atx = mem_region_ctx->submit(OpCode::RECV, send_mem_region_assign);
            mr_atx->wait();
            
            for(size_t i = 0; i < batch_size; ++i)
            {
                send_batch[i].remote_addr = send_meta_data_buf[i].addr;
                send_batch[i].remote_rkey = send_meta_data_buf[i].rkey;
                send_batch[i].length = send_meta_data_buf[i].len;
                send_batch[i].target_offset = i * send_meta_data_buf[i].len;
            }
            
            std::cout<< "mr recv" << std::endl;
            auto data_atx = data_ctx->submit(OpCode::WRITE_WITH_IMM, send_batch);
            data_atx->wait();

        }

        int next_slot(int num_to_skip = 1)
        {
            auto temp = slot;
            slot += num_to_skip;
            return temp;
        }
        
        int32_t slot{0};

        std::map<int, std::function<void(int)>> send_callbacks;  //send端需要收到mr后再执行回调进行发送
        std::map<int, std::function<void(int)>> recv_callbacks;  //recv端需要发送mr后再执行回调进行接收

        std::shared_ptr<RDMAContext> data_ctx;
        std::shared_ptr<RDMAContext> mem_region_ctx;



};

}







            // // 注册内存区域
            // mem_region_ctx->register_memory_region(
            //     "SEND_Meta_DATA_KEY", 
            //     reinterpret_cast<uintptr_t>(meta_data_buf.get()),
            //     meta_data_buf_size
            // );

            // AssignmentBatch meta_recv_batch;
            // for(size_t i = 0; i < batch_size; i++) {
            //     meta_recv_batch.push_back(Assignment(
            //         "SEND_Meta_DATA_KEY",
            //         i * sizeof(meta_data_t),
            //         0,
            //         sizeof(meta_data_t)
            //     ));
            // }

            // mem_region_ctx->submit(
            //     OpCode::RECV,
            //     meta_recv_batch,
            //     [this, buf = meta_data_buf, batch_size, &send_batch](int code) {
            //         // 检查回调触发
            //         std::cout << "JJJJJJJJJJJJJJJ" << std::endl;
                    
            //         // 安全转换
            //         meta_data_t *meta_data = reinterpret_cast<meta_data_t*>(buf.get());
            //         if (!meta_data) {
            //             SLIME_LOG_ERROR("Meta data conversion failed");
            //             return;
            //         }

            //         std::cout << "K" << std::endl;
            //         for(size_t i = 0; i < batch_size; i++) {
            //             send_batch[i].remote_addr = meta_data[i].addr;
            //             send_batch[i].remote_rkey = meta_data[i].rkey;
            //             send_batch[i].length = meta_data[i].length;
            //         }

            //         // 提交RDMA写操作
            //         auto RDMA_atx = data_ctx->submit(OpCode::WRITE_WITH_IMM, send_batch, nullptr);
            //         RDMA_atx->wait();
            //     }
            // );

// mem_region_ctx->submit(
//             OpCode::SEND, 
//             mem_region_assign, 
//             [this, &recv_batch](int code)
//             {
//                 std::cout<<"7"<<std::endl;
//                 auto atx = data_ctx->submit(OpCode::RECV, recv_batch);
//                 atx->wait();
//             }
//         );


// void send(AssignmentBatch& send_batch)
// {

//     size_t batch_size = send_batch.size();
    
//     const size_t meta_data_buf_size = batch_size * sizeof(meta_data_t);
//     void *meta_data_buf = malloc(meta_data_buf_size);
//     memset(meta_data_buf, 0, meta_data_buf_size);
//     mem_region_ctx->register_memory_region("SEND_Meta_DATA_KEY", reinterpret_cast<uintptr_t>(meta_data_buf), meta_data_buf_size);


//     AssignmentBatch meta_recv_batch;
//     for(size_t i = 0; i < batch_size; i++)
//     {
//         meta_recv_batch.push_back(Assignment(
//             "SEND_Meta_DATA_KEY",
//             i * sizeof(meta_data_t),
//             0,
//             sizeof(meta_data_t)
//         ));
//     }

//     std::cout<<"!!!!"<<std::endl;
//     mem_region_ctx->submit(
//         OpCode::RECV,
//         meta_recv_batch,
//         [this, &meta_data_buf, meta_data_buf_size, batch_size, &send_batch](int code)
//         {   
//             std::cout<<"J"<<std::endl;
//             meta_data_t *meta_data = reinterpret_cast<meta_data_t*>(meta_data_buf);
//             std::cout<<"K"<<std::endl;
//             for(size_t i = 0; i < batch_size; i++)
//             {
//                 uint64_t remote_addr = meta_data[i].addr;
//                 uint32_t remote_rkey = meta_data[i].rkey;
//                 uint32_t length = meta_data[i].length;
//                 uint32_t index = meta_data[i].index;
//                 send_batch[index].remote_addr = remote_addr;
//                 send_batch[index].remote_rkey = remote_rkey;
//                 send_batch[index].length = length;
//             }
//             std::cout<<meta_data[0].addr<<std::endl;
//             std::cout<<"8"<<std::endl;
//             auto RDMA_atx = data_ctx->submit(OpCode::WRITE_WITH_IMM, send_batch, nullptr);
//             RDMA_atx->wait();
//         }
//     );


//     //free(meta_data_buf);

// }