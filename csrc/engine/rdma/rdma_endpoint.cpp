#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_buffer.h"


#include <condition_variable>
#include <mutex>
#include <vector>
#include <queue>
#include <memory>
#include <map>
#include <unordered_map>
#include <stdexcept>
#include <infiniband/verbs.h>


namespace slime {


typedef struct meta_data{

    uint64_t mr_addr;
    uint32_t mr_rkey;
    uint32_t mr_size;
    uint32_t mr_slot;
    uint32_t padding;

} __attribute__((packed)) meta_data_t;



class RingBuffer
{

    public:

        class BufferFullException : public std::runtime_error
        {
            public:
                BufferFullException() : std::runtime_error("Ring buffer is full") {}
        };


        explicit RingBuffer(size_t _max_buffer_size = 64)
        : max_buffer_size_(_max_buffer_size),
          assignmentbatch_queue_(_max_buffer_size),
          head_(0),
          tail_(0),
          current_size_(0)
        {
            if (max_buffer_size_ == 0) {
                throw std::invalid_argument("Buffer capacity cannot be zero");
            }
        }

        ~RingBuffer() = default;


        void PushAssignmentBatch(AssignmentBatch& assignment_batch)
        {

            std::lock_guard<std::mutex> lock(queue_mutex_);

            if (current_size_ == max_buffer_size_)
            {
                throw BufferFullException();
            }

            // 将assignmentbatch加入到队尾
            assignmentbatch_queue_[tail_] = std::move(assignment_batch);
            tail_ = (tail_ + 1) % max_buffer_size_;
            current_size_ += 1;
            queue_cv_.notify_one();
        }


        void  WaitAndPop(AssignmentBatch& assignment_batch)
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return current_size_ > 0; });

            //弹出首元素
            assignment_batch = std::move(assignmentbatch_queue_[head_]);
            head_ = (head_ + 1) % max_buffer_size_;
            current_size_--;
        }

        std::shared_ptr<AssignmentBatch> wait_and_pop() 
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return current_size_ > 0; });
            
            auto result = std::make_shared<AssignmentBatch>(std::move(assignmentbatch_queue_[head_]));
            head_ = (head_ + 1) % max_buffer_size_;
            current_size_--;
            return result;
        }


        bool IsEmpty() const
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            return current_size_ == 0;
        }


        size_t Size() const
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            return current_size_;
        }


        size_t MaxBufferSize() const
        {
            return max_buffer_size_;
        }


    private:
        const size_t max_buffer_size_;
        std::vector<AssignmentBatch> assignmentbatch_queue_;

        size_t head_;
        size_t tail_;
        size_t current_size_;

        mutable std::mutex queue_mutex_;
        std::condition_variable queue_cv_;

};


class RDMAEndpoint
{

    public:

        RDMAEndpoint(const std::string &dev_name, uint8_t ib_port, const std::string &link_type,
                     size_t buffer_size)
        {


            std::cout<<"Init the Contexts and RDMA Devices..." << std::endl;
            SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
            send_data_ctx_ = std::make_shared<RDMAContext>();
            send_meta_ctx_ = std::make_shared<RDMAContext>();
            recv_data_ctx_ = std::make_shared<RDMAContext>();
            recv_meta_ctx_ = std::make_shared<RDMAContext>();

            send_data_ctx_->init(dev_name, ib_port, link_type);
            send_meta_ctx_->init(dev_name, ib_port, link_type);
            recv_data_ctx_->init(dev_name, ib_port, link_type);
            recv_meta_ctx_->init(dev_name, ib_port, link_type);

            std::cout<<"Init the SEND/RECV ring buffer..." << std::endl;
            SLIME_LOG_INFO("Init the SEND/RECV ring buffer...");

            send_buffer_ = std::make_unique<RingBuffer>(buffer_size);
            recv_buffer_ = std::make_unique<RingBuffer>(buffer_size);


            send_slot_id_ = 0;
            recv_slot_id_ = 0;

            std::cout<<"RDMA Endpoint Init Success..." << std::endl;
            SLIME_LOG_INFO("RDMA Endpoint Init Success...");

        }

        ~RDMAEndpoint() = default;


        void ContextConnect(const json& data_ctx_info, const json& meta_ctx_info)
        {
            send_data_ctx_->connect(data_ctx_info);
            recv_data_ctx_->connect(data_ctx_info);

            send_meta_ctx_->connect(meta_ctx_info);
            recv_meta_ctx_->connect(meta_ctx_info);

            send_data_ctx_->launch_future();
            recv_data_ctx_->launch_future();
            send_meta_ctx_->launch_future();
            recv_meta_ctx_->launch_future();

        }



        void Launch(int max_threads = 4)
        {
            RECV_RUN = true;
            SEND_RUN = true;


            for (int i = 0; i < max_threads; ++i) 
                recv_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::AsyncRecvData,this));


            for (int i = 0; i < max_threads; ++i) 
                send_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::AsyncSendData,this));

            std::cout<<"The SEND and RECV Threads are Started..." << std::endl;

        }

        void WaitRecv()
        {
            for (auto& future : send_futures_) 
                future.wait();
            std::cout << "All RECV Tasks have finished." << std::endl;
        }

   


        void WaitSend()
        {
            for (auto& future : send_futures_) 
                future.wait();

            std::cout << "All SEND Tasks have finished." << std::endl;
        }

        void Stop() 
        {
            if (!SEND_RUN && !RECV_RUN) return;

            SEND_RUN = false;
            RECV_RUN = false;

            std::cout<<"STOP"<<std::endl;

        }

        // Need to merge
        json GetRecvDataContextInfo() const
        {
            return recv_data_ctx_->endpoint_info();
        }

        json GetRecvMetaContextInfo() const
        {
            return recv_meta_ctx_->endpoint_info();
        }

        json GetSendDataContextInfo() const
        {
            return send_data_ctx_->endpoint_info();
        }

        json GetSendMetaContextInfo() const
        {
            return send_meta_ctx_->endpoint_info();
        }


        void RegisterRecvMemRegion(std::string &str, void* ptr, size_t data_size)
        {
            recv_data_ctx_->register_memory_region(str, (uintptr_t) ptr, data_size);
        }

        void RegisterSendMemRegion(std::string &str, void* ptr, size_t data_size)
        {
            send_data_ctx_->register_memory_region(str, (uintptr_t) ptr, data_size);
        }

        // The mr key is followed by the form: str+"i"+"RECV"
        void RegisterRecvMemRegionBatch(std::string &str, void** ptrs, size_t* data_size, uint32_t batch_size)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "_" + std::to_string(i);
                RegisterRecvMemRegion(KEY, ptrs[i], data_size[i]);
            }
        }
        // The mr key is followed by the form: str+"i"+"SEND"
        void RegisterSendMemRegionBatch(std::string &str, void** ptrs, size_t* data_size, uint32_t batch_size)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "_" + std::to_string(i);
                RegisterSendMemRegion(KEY, ptrs[i], data_size[i]);
            }
        }


        void UnregisterRecvMemRegionBatch(std::string &str, uint32_t batch_size)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "_" + std::to_string(i);
                recv_data_ctx_->unregister_memory_region(KEY);
            }
        }



        void UnregisterSendMemRegionBatch(std::string &str, uint32_t batch_size)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "_" + std::to_string(i) ;
                send_data_ctx_->unregister_memory_region(KEY);
            }
        }



        void Recv(void** ptrs, size_t* data_size, uint32_t batch_size)
        {   
            recv_slot_id_++;
            std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);
            RegisterRecvMemRegionBatch(cur_key, ptrs, data_size, batch_size);

            AssignmentBatch recv_data_batch;
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = cur_key + "_" + std::to_string(i);
                recv_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
                recv_data_batch[i].slot_id = recv_slot_id_;
            }

            recv_buffer_->PushAssignmentBatch(recv_data_batch);
            
            
        }

        void Send(void** ptrs, size_t* data_size, uint32_t batch_size)
        {   
            send_slot_id_++;
            std::string cur_key = "SEND_KEY_" +  std::to_string(send_slot_id_);
            RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size);
          
            AssignmentBatch send_data_batch;
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = cur_key + "_" + std::to_string(i); // 生成和注册内存区域相同的KEY
                send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
                send_data_batch[i].slot_id = send_slot_id_;
            }

            send_buffer_->PushAssignmentBatch(send_data_batch);
        }



    private:

        void AsyncRecvData()
        {
            while(RECV_RUN)
            {
                if(recv_buffer_->IsEmpty())
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
        
                AssignmentBatch recv_data_batch;
                recv_buffer_->WaitAndPop(recv_data_batch);
                size_t batch_size = recv_data_batch.size();
                auto meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);

                std::string META_KEY = "RECV_META_" + std::to_string(recv_data_batch[0].slot_id);

                {
                    std::lock_guard<std::mutex> lock(meta_data_mutex);
                    for(size_t i = 0; i < batch_size; ++i)
                    {
                        meta_data_buf[i].mr_addr = reinterpret_cast<uint64_t>(recv_data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->addr);
                        meta_data_buf[i].mr_rkey = recv_data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->rkey;
                        meta_data_buf[i].mr_size = recv_data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->length;
                        meta_data_buf[i].mr_slot = recv_data_batch[i].slot_id;
                    }


                    
                    recv_meta_ctx_->register_memory_region(META_KEY,
                                                            reinterpret_cast<uintptr_t>(meta_data_buf.get()),
                                                            batch_size * sizeof(meta_data_t));
                }
                AssignmentBatch meta_data_assignments(batch_size);
                for(size_t i = 0; i < batch_size; ++i)
                {
                    meta_data_assignments[i] = Assignment(
                        META_KEY,
                        0,
                        i * sizeof(meta_data_t),
                        sizeof(meta_data_t)
                    );
                }
                {   
                    std::lock_guard<std::mutex> lock(meta_data_mutex);
                    auto meta_atx = recv_meta_ctx_->submit(OpCode::SEND, meta_data_assignments);
                    meta_atx->wait();
        
                    auto data_atx = recv_data_ctx_->submit(OpCode::RECV, recv_data_batch);
                    data_atx->wait();

                    std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);
                    //UnregisterRecvMemRegionBatch(cur_key,batch_size);
                    recv_meta_ctx_->unregister_memory_region(META_KEY);
                }
                std::cout<< "Data has been successfully Received " << std::endl;
            }

        }

        void AsyncSendData()
        {
            while(SEND_RUN)
            {   
                if(send_buffer_->IsEmpty())
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }

                AssignmentBatch send_data_batch;
                send_buffer_->WaitAndPop(send_data_batch);
                

                size_t batch_size = send_data_batch.size();
                auto meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
                std::string META_KEY = "SEND_META_" + std::to_string(send_data_batch[0].slot_id);
                {
                    std::lock_guard<std::mutex> lock(meta_data_mutex);
                    send_meta_ctx_->register_memory_region(META_KEY,
                                                        reinterpret_cast<uintptr_t>(meta_data_buf.get()),
                                                        batch_size * sizeof(meta_data_t));
                }

                AssignmentBatch meta_data_assignments(batch_size);
                for(size_t i = 0; i < batch_size; ++i)
                {
                    meta_data_assignments[i] = Assignment(
                        META_KEY,
                        0,
                        i * sizeof(meta_data_t),
                        sizeof(meta_data_t)
                    );
                }

                {
                    std::lock_guard<std::mutex> lock(meta_data_mutex);
                    auto meta_atx = send_meta_ctx_->submit(OpCode::RECV, meta_data_assignments);
                    meta_atx->wait();
                }
                {
                    std::lock_guard<std::mutex> lock(meta_data_mutex);
                    for (size_t i = 0; i < batch_size; ++i)
                    {
                        send_data_batch[i].remote_addr = meta_data_buf[i].mr_addr;
                        send_data_batch[i].remote_rkey = meta_data_buf[i].mr_rkey;
                        send_data_batch[i].length = meta_data_buf[i].mr_size;
                        send_data_batch[i].target_offset = 0;
                        if(send_data_batch[i].slot_id != meta_data_buf[i].mr_slot)
                            std::cout<<"Error in slot it" << std::endl;
                    }

                    auto data_atx = send_data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data_batch);
                    data_atx->wait();
                    std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);
                    //UnregisterSendMemRegionBatch(cur_key, batch_size);
                    send_meta_ctx_->unregister_memory_region(META_KEY);
                    
                }
                std::cout<< "Data has been successfully Transmitted " << std::endl;
            }


        }


        std::vector<std::future<void>> recv_futures_;
        std::vector<std::future<void>> send_futures_;

        std::atomic<uint32_t> send_slot_id_;
        std::atomic<uint32_t> recv_slot_id_;

        uint32_t batch_size_;

        std::atomic<bool> RECV_RUN{true};
        std::unique_ptr<RingBuffer> recv_buffer_;


        std::atomic<bool> SEND_RUN{true};
        std::unique_ptr<RingBuffer> send_buffer_;


        std::shared_ptr<RDMAContext> recv_data_ctx_;
        std::shared_ptr<RDMAContext> recv_meta_ctx_;
        std::shared_ptr<RDMAContext> send_data_ctx_;
        std::shared_ptr<RDMAContext> send_meta_ctx_;


        std::mutex meta_data_mutex;

};

}
