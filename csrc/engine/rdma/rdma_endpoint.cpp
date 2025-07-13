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


// 元数据的结构
// mr_size表示当前一个batch内数据的长度
// mr_slot表示节点的全局rank_id，用于发送端识别不同的节点

typedef struct meta_data{

    uint64_t mr_addr;   
    uint32_t mr_rkey;   
    uint32_t mr_size; 
    uint32_t mr_slot;  
    uint32_t padding; 

} __attribute__((packed)) meta_data_t;  



// 环形Buffer用于发送端：准备发送的AssignmentBatch送入RingBuffer
// 检测RingBuffer中是否存在元素，有则弹出用于发送
// 没有则等待RingBuffer中加入元素

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


        void PushAssignmentBatch(const AssignmentBatch& _assignment_batch)
        {
            
            std::lock_guard<std::mutex> lock(queue_mutex_);

            if (current_size_ == max_buffer_size_)
            {
                throw BufferFullException();
            }

            // 将assignmentbatch加入到队尾
            assignmentbatch_queue_[tail_] = _assignment_batch;
            tail_ = (tail_ + 1) % max_buffer_size_;
            current_size_ += 1;
            queue_cv_.notify_one();
        }


        void WaitAndPop(AssignmentBatch& _assignment_batch)
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return current_size_ > 0; });

            //弹出首元素
            size_t pop_pos = head_;
            _assignment_batch = assignmentbatch_queue_[pop_pos];
            head_ = (head_ + 1) % max_buffer_size_;
            current_size_--;
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
        RDMAEndpoint(size_t max_rank, const std::string &dev_name, uint8_t ib_port, const std::string &link_type, 
                     size_t buffer_size, uint8_t local_id) 
        {
            // 一共有max_rank节点，那么当前节点x需要和其他(max_rank - 1)个节点建立连接
            // 让每一个节点都可以同时收发，要注册完所有的RDMAContext
            send_data_ctx_.resize(max_rank - 1);
            send_meta_ctx_.resize(max_rank - 1);
            recv_data_ctx_.resize(max_rank - 1);
            recv_meta_ctx_.resize(max_rank - 1);
            std::cout<<"Init the RDMA Devices..." << std::endl;
            for(size_t i = 0; i < max_rank - 1; ++i)
            {
                send_data_ctx_[i] = std::make_shared<RDMAContext>();
                send_meta_ctx_[i] = std::make_shared<RDMAContext>();
                recv_data_ctx_[i] = std::make_shared<RDMAContext>();
                recv_meta_ctx_[i] = std::make_shared<RDMAContext>();

                send_data_ctx_[i]->init(dev_name, ib_port, link_type);
                send_meta_ctx_[i]->init(dev_name, ib_port, link_type);
                recv_data_ctx_[i]->init(dev_name, ib_port, link_type);
                recv_meta_ctx_[i]->init(dev_name, ib_port, link_type);
            }

            send_buffer_ = std::make_unique<RingBuffer>(buffer_size);
            recv_buffer_ = std::make_unique<RingBuffer>(buffer_size);


            max_slot_ = max_rank - 1;
            local_id_ = local_id;
            max_rank_ = max_rank;

        }



        // 对于发送和接受的任务都需要先注册对应的内存区域
        void RegisterRecvMemRegion(std::string &str, void* ptr, size_t data_size, uint8_t src_id)
        {
            uint8_t slot_id = (src_id - local_id_) % max_rank_;
            recv_data_ctx_[slot_id]->register_memory_region(str, (uintptr_t) ptr, data_size);
        }

        void RegisterSendMemRegion(std::string &str, void* ptr, size_t data_size, uint8_t dst_id)
        {
            uint8_t slot_id = (dst_id - local_id_) % max_rank_;
            send_data_ctx_[slot_id]->register_memory_region(str, (uintptr_t) ptr, data_size);
        }


        // 传入的是一个Batch的数据
        // 每一个单元数据用不同的KEY进行识别
        void RegisterRecvMemRegionBatch(std::string &str, void** ptrs, size_t* data_size, uint32_t batch_size, uint8_t src_id)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "+" + std::to_string(i) + "RECV";
                RegisterRecvMemRegion(KEY, ptrs[i], data_size[i], src_id);
            }
        }
        void RegisterSendMemRegionBatch(std::string &str, void** ptrs, size_t* data_size, uint32_t batch_size, uint8_t dst_id)
        {
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = str + "+" + std::to_string(i) + "SEND";
                RegisterSendMemRegion(KEY, ptrs[i], data_size[i], dst_id);
            }
        }
        

        void Recv(void** ptrs, size_t* data_size, uint32_t batch_size, uint8_t src_id)
        {
            // 注册内存区域
            std::string cur_key = "RECV_KEY" + std::to_string(src_id);
            RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size, src_id);

            // 生成对应的RDMA任务
            AssignmentBatch recv_batch;
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = cur_key + "+" + std::to_string(i) + "RECV";
                recv_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
                recv_batch[i].target_rank_id = src_id;
                recv_batch[i].local_rank_id  = local_id_;
            }

            // 推入接受缓冲区
            send_buffer_->PushAssignmentBatch(recv_batch);

        }

        void Send(void** ptrs, size_t* data_size, uint32_t batch_size, uint8_t dst_id)
        {   
            // 注册内存区域
            std::string cur_key = "SEND_KEY" + std::to_string(dst_id);
            RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size, dst_id);
            
            // 生成对应的RDMA任务
            AssignmentBatch send_data_batch;
            for(uint32_t i = 0; i < batch_size; ++i)
            {
                std::string KEY = cur_key + "+" + std::to_string(i) + "SEND"; // 生成和注册内存区域相同的KEY
                send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
                send_data_batch[i].target_rank_id = dst_id;
                send_data_batch[i].local_rank_id  = local_id_;
            }

            // 推入发送缓冲区
            send_buffer_->PushAssignmentBatch(send_data_batch);
        }


    private:

    
        // 对于不同slot_id的任务，并发执行，对于相同slot_id的内容，串行执行
        void AsyncRecvData()
        {

        }

        // 对于不同slot_id的任务，并发执行，对于相同slot_id的内容，串行执行
        void AsyncSendData()
        {

            while(SEND_RUN)
            {   
                
                auto send_data_batch = std::make_shared<AssignmentBatch>();

                send_buffer_->WaitAndPop(*send_data_batch);

                uint8_t slot_id = ((*send_data_batch)[0].target_rank_id - local_id_) % max_rank_;


                auto callback = [this, send_data_batch, slot_id](int status)
                {
                    if (status != 0) 
                    {
                        std::cerr << "Failed to receive meta data for slot " 
                                << static_cast<int>(slot_id) << std::endl;
                        return;
                    }

                    std::unique_lock<std::mutex> lock(meta_data_mutex);

                    if (meta_data_buf.find(slot_id) == meta_data_buf.end() || meta_data_buf[slot_id].empty()) 
                    {
                        std::cerr << "No meta data available for slot " 
                                << static_cast<int>(slot_id) << std::endl;
                        return;
                    }
               
                        auto meta_data_batch = std::move(meta_data_buf[slot_id].front());
                        meta_data_buf[slot_id].pop_front();
                        lock.unlock();
                        for (size_t i = 0; i < send_data_batch->size(); ++i) 
                        {
                            (*send_data_batch)[i].remote_addr = meta_data_batch[i].mr_addr;
                            (*send_data_batch)[i].remote_rkey = meta_data_batch[i].mr_rkey;
                            (*send_data_batch)[i].length = meta_data_batch[i].mr_size;
                            (*send_data_batch)[i].target_offset = 0;
                        }

                        PostSendData(*send_data_batch);


                    
                };

                PostRecvMetaData(slot_id, send_data_batch->size(), callback);

            }
        }


        void PostRecvMetaData(uint8_t slot_id, uint32_t batch_size, std::function<void(int)> callback)
        {

            try {
                    std::unique_lock<std::mutex> lock(meta_data_mutex);
        
                    auto meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
                    std::string KEY = "RECV_META" + std::to_string(slot_id);

                    send_meta_ctx_[slot_id]->register_memory_region(
                        KEY, 
                        reinterpret_cast<uintptr_t>(meta_data_buf.get()), 
                        batch_size * sizeof(meta_data_t));
        
                
                    AssignmentBatch meta_data_assignments(batch_size);
                    for(size_t i = 0; i < batch_size; ++i) 
                    {
                        meta_data_assignments[i] = Assignment(
                            KEY,
                            0,
                            i * sizeof(meta_data_t),  
                            sizeof(meta_data_t)
                        );
                    }
        
         
                    this->meta_data_buf[slot_id].emplace_back(std::move(meta_data_buf));
                    lock.unlock();
        
               
                    send_meta_ctx_[slot_id]->submit(OpCode::RECV, meta_data_assignments, 
                        [callback](int status) {
                            callback(status);
                        });
            
                } 
                catch (const std::exception& e) 
                {
                    std::cerr << "Error in PostRecvMetaData: " << e.what() << std::endl;
                    callback(-1);  // 通知回调失败
                }

        }


        void PostSendData(AssignmentBatch &send_data_batch)
        {
            uint8_t  target_rank_id = send_data_batch[0].target_rank_id;

            uint8_t slot_id = (target_rank_id - local_id_) % max_rank_;

            std::cout << "From rank id: " << local_id_ << std::endl;
            std::cout << "AsyncSendData: Sending data for rank: " 
                      << target_rank_id
                      << " via RDMA WRITE WITH IMM DATA..." << std::endl;
            

            auto data_atx = send_data_ctx_[slot_id]->submit(OpCode::WRITE_WITH_IMM, send_data_batch,
                [this, target_rank_id ](int status) 
                {
                    std::cout << "RDMA WRITE COMPLETE FOR" << target_rank_id << "FROM: " << local_id_ << std::endl;
                }
            );
            
        }



        uint8_t max_slot_;
        uint8_t local_id_;
        uint8_t max_rank_;


        uint32_t batch_size_;

        // 启动recv的标志
        std::atomic<bool> RECV_RUN{true};
        std::unique_ptr<RingBuffer> recv_buffer_;


        std::atomic<bool> SEND_RUN{true};
        std::unique_ptr<RingBuffer> send_buffer_;




    
        // recv_data_ctx_用于当前节点接收RDMA数据的ctx
        // recv_meta_ctx_用于当前节点发送META数据的ctx
        // send_data_ctx_用于当前节点发送RDMA数据的ctx
        // send_meta_ctx_用于当前节点接受META数据的ctx
        // 注意大小为(max_rank_size_ - 1)
        // 同时是相对关系，比如：
        // 若节点1向节点2发送数据，节点1应该调用自己的send_data_ctx_[(2 - 1 - 1) mod (max_rank_size_)]这一个ctx
        // 而节点2也需要调用recv_data_ctx_[(1 - 2 - 1) mod (max_rank_size_)]这一个ctx
        // 总之都按对(端节节点id)-(自己节点id) - 1 mod max_rank_size_来确定调用ctx

        std::vector<std::shared_ptr<RDMAContext>> recv_data_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> recv_meta_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> send_data_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> send_meta_ctx_;

        std::mutex meta_data_mutex;
        std::unordered_map<int, std::deque<std::unique_ptr<meta_data_t[]>>> meta_data_buf;




};











}