#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_buffer.h"


#include <condition_variable>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_map>
#include <infiniband/verbs.h>

namespace slime {


typedef struct meta_data{

    uint64_t mr_addr;   
    uint32_t mr_rkey;   
    uint32_t mr_size; 
    uint32_t mr_slot;  
    uint32_t padding; 

} __attribute__((packed)) meta_data_t;  



class Buffer
{
    public:

        void PushAssignmentBatch(const AssignmentBatch& _assignment_batch)
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            assignmentbatch_queue_.push(_assignment_batch);
            queue_cv_.notify_one();
        }

        bool CheckAssignmentBatchQueue(AssignmentBatch& _assignment_batch)
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (assignmentbatch_queue_.empty())
                return false;

            _assignment_batch = assignmentbatch_queue_.front();
            assignmentbatch_queue_.pop();
            return true;
        }

        void WaitAndPop(AssignmentBatch& _assignment_batch)
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] { return !assignmentbatch_queue_.empty();});
            _assignment_batch = assignmentbatch_queue_.front();
            assignmentbatch_queue_.pop();
        }

        bool IsEmpty() 
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            return assignmentbatch_queue_.empty();
        }

    private:
        std::queue<AssignmentBatch> assignmentbatch_queue_;
        std::mutex queue_mutex_;
        std::condition_variable queue_cv_;

};



class RingBuffer 
{
    private:

};


class RDMAEndpoint{

    public:

        RDMAEndpoint(size_t _max_rank)  
        {   

            // 一共有_max_rank节点，那么当前节点x需要和其他(_max_rank - 1)个节点建立连接
            // 让每一个节点都可以同时收发，要注册完所有的RDMAContext
            send_data_ctx_.resize(_max_rank - 1);
            send_meta_ctx_.resize(_max_rank - 1);
            recv_data_ctx_.resize(_max_rank - 1);
            recv_meta_ctx_.resize(_max_rank - 1);

            for(size_t i = 0; i < _max_rank - 1; ++i)
            {
                send_data_ctx_[i] = std::make_shared<RDMAContext>();
                send_meta_ctx_[i] = std::make_shared<RDMAContext>();
                recv_data_ctx_[i] = std::make_shared<RDMAContext>();
                recv_meta_ctx_[i] = std::make_shared<RDMAContext>();
            }
            

        }

        ~RDMAEndpoint() = default;

        void init(const std::string &dev_name, uint8_t ib_port, const std::string &link_type)
        {

        }


        void connect(const json& EP_DATA, const json& EP_MR)
        {
        }


        // 同步模式：发送和接收都会阻塞
        void LaunchSyncSendRecv()
        {

        }

        // 异步模式：发送和接收都不会阻塞
        void LaunchAsyncSendRecv()
        {

        }

        void SyncReceiveData()
        {
            
        }


        void Recv(AssignmentBatch& _assignment_batch)
        {
            RECV_Buffer->PushAssignmentBatch(_assignment_batch);
        }

        void AsyncReceiveData()
        {   
            while(RECV_RUN)
            {
                auto recv_data_batch = std::make_shared<AssignmentBatch>();
                RECV_Buffer->WaitAndPop(*recv_data_batch); //阻塞等待RECV_Buffer中有接收任务

                auto meta_data_batch = std::make_shared<meta_data_t[]>(batch_size_);


                auto recv_data_future = std::async(std::launch::async, [this, recv_data_batch]
                {PostRecvAssignment(*recv_data_batch);}).share();


                auto meta_data_future = std::async(std::launch::async, 
                                    [this, 
                                     recv_data_batch,
                                     meta_data_batch = std::move(meta_data_batch)]
                                    {TransmitMetaData(*recv_data_batch, meta_data_batch);}).share();
                
                    
                std::cout<< "meta_data_future is finished, check the buffer again..." << std::endl;
            }
        }

        void PostRecvAssignment(AssignmentBatch& _assignment_batch)
        {
            std::cout<<"Post Recv..." << std::endl;
            auto recv_data_atx = recv_data_ctx_->submit(OpCode::RECV, _assignment_batch);
            recv_data_atx->wait();
            std::cout<<"RECV finished" << std::endl;
        }


        void TransmitMetaData(AssignmentBatch& _assignment_batch, std::shared_ptr<meta_data_t[]> meta_data_batch)
        {

            std::cout<<"Packing and sending RDMA region..." << std::endl;

            // 填充meta_data数据
            for(size_t i = 0; i < batch_size_; ++i)
            {
                meta_data_batch[i].mr_addr = reinterpret_cast<uint64_t>(recv_data_ctx_->memory_pool_->get_mr(_assignment_batch[i].mr_key)->addr);
                meta_data_batch[i].mr_rkey = recv_data_ctx_->memory_pool_->get_mr(_assignment_batch[i].mr_key)->rkey;
                meta_data_batch[i].mr_size = _assignment_batch[i].length;
                meta_data_batch[i].mr_slot = slot_id_;
            }

            std::string KEY = "RECV_Meta_DATA_KEY_" + std::to_string(reinterpret_cast<uintptr_t>(meta_data_batch.get()));
            meta_data_ctx_->register_memory_region( KEY,
                                                    reinterpret_cast<uintptr_t>(meta_data_batch.get()),
                                                    batch_size_ * sizeof(meta_data_t));

            
            // 注册RMDA任务并提交
            AssignmentBatch meta_data_assignments(batch_size_); 
            for(size_t i = 0; i < batch_size_; ++i)
                meta_data_assignments[i] = Assignment(
                    KEY,
                    0,
                    i * sizeof(meta_data_t),
                    sizeof(meta_data_t)
                );
            
  
            auto meta_data_atx = meta_data_ctx_->submit(OpCode::SEND_WITH_IMM, meta_data_assignments);
            meta_data_atx->wait();

            std::cout<<"Packing and sending RDMA region are finished" << std::endl;
            meta_data_ctx_->unregister_memory_region(KEY);

        }

        // push需要发送的任务
        void Send(AssignmentBatch& _assignment_batch)
        {
            SEND_Buffer->PushAssignmentBatch(_assignment_batch);
        }


        void AsyncSendData() 
        {
            while (SEND_RUN_) 
            {

                auto send_data_batch = std::make_shared<AssignmentBatch>();
                SEND_Buffer->WaitAndPop(*send_data_batch);

                auto meta_future = std::async(std::launch::async, [this] {
                ReceiveMetaData();
                });

                auto send_future = std::async(std::launch::async, [this, send_data_batch] {
                PostSendAssignment(*send_data_batch);
                });
            }
        }


        void SyncSendData()
        {
            while(SEND_RUN_)
            {
                auto send_data_batch = std::make_shared<AssignmentBatch>();
                SEND_Buffer->WaitAndPop(*send_data_batch);

                auto meta_data_batch = std::make_unique<meta_data_t[]>(batch_size_);
                meta_data_ctx_->register_memory_region("SEND_Meta_DATA_KEY", 
                                                        reinterpret_cast<uintptr_t>(meta_data_batch.get()), 
                                                        batch_size_ * sizeof(meta_data_t));
                
                AssignmentBatch recv_meta_data_batch(batch_size_);
                for(size_t i = 0; i < batch_size_; ++i)
                    recv_meta_data_batch[i] = Assignment(
                    "SEND_Meta_DATA_KEY",
                    0,
                    reinterpret_cast<uint64_t>(i * sizeof(meta_data_t)),
                    sizeof(meta_data_t)
                );

                auto meta_data_atx = meta_data_ctx_->submit(OpCode::RECV, recv_meta_data_batch);
                meta_data_atx->wait(); // 等待meta_data接收完成
                std::cout<< "mem region has received... " << std::endl;

                for(size_t i = 0; i < batch_size_; ++i)
                {
                    (*send_data_batch)[i].remote_addr = meta_data_batch[i].mr_addr;
                    (*send_data_batch)[i].remote_rkey = meta_data_batch[i].mr_rkey;
                    (*send_data_batch)[i].length      = meta_data_batch[i].mr_size;
                    (*send_data_batch)[i].target_offset = 0;
                }


                std::cout<< "Tx Data via RDMA WRITE WITH IMM DATA... " << std::endl;
                auto data_atx = send_data_ctx_->submit(OpCode::WRITE_WITH_IMM, *send_data_batch);
                data_atx->wait();
                std::cout<< "Tx Data finished... " << std::endl;
                meta_data_ctx_->unregister_memory_region("SEND_Meta_DATA_KEY");

            }
        }


        void PostSendAssignment(AssignmentBatch& _assignment_batch)
        {
            std::cout<<"Post Send..." << std::endl;
            std::unique_lock<std::mutex> lock(queue_mutex);
            uint32_t _target_slot_id = _assignment_batch[0].slot_id; 
            if (meta_data_deque.find(_target_slot_id) != meta_data_deque.end() && !meta_data_deque[_target_slot_id].empty())
            {
                auto meta_data_batch = std::move(meta_data_deque[_target_slot_id].front());
                meta_data_deque[_target_slot_id].pop_front();
                lock.unlock();
                for (size_t i = 0; i < batch_size_; ++i)
                {
                    _assignment_batch[i].remote_addr = meta_data_batch[i].mr_addr;
                    _assignment_batch[i].remote_rkey = meta_data_batch[i].mr_rkey;
                    _assignment_batch[i].length      = meta_data_batch[i].mr_size;
                    _assignment_batch[i].target_offset = 0;
                }
                auto data_atx = send_data_ctx_->submit(OpCode::WRITE_WITH_IMM, _assignment_batch);
                data_atx->wait();
                std::cout << "Send finished via RDMA WRITE WITH IMM..." << std::endl;
            }
            else
            {
                pend_send_task_deque[_target_slot_id].push_back(
                        [this, 
                         batch = _assignment_batch](int slot) mutable
                        {
                            PostSendAssignment(batch);
                        }
                    );
                lock.unlock();
                std::cout << "No meta data for slot " << _target_slot_id << ", task queued in pend_send_task_deque" << std::endl;
            }
        }

        void ReceiveMetaData()
        {
            std::cout<<"Receiving and unpacking the remote RDMA region..." << std::endl;
            auto meta_data_batch = std::make_unique<meta_data_t[]>(batch_size_);
            
            std::string KEY = "SEND_META_DATA_KEY_" + std::to_string(reinterpret_cast<uintptr_t>(meta_data_batch.get()));
            meta_data_ctx_->register_memory_region(KEY, 
                                                   reinterpret_cast<uintptr_t>(meta_data_batch.get()), 
                                                   batch_size_ * sizeof(meta_data_t));
            
            AssignmentBatch meta_data_assignments(batch_size_);
            for(size_t i = 0; i < batch_size_; ++i)
                    meta_data_assignments[i] = Assignment(
                    KEY,
                    0,
                    0,//reinterpret_cast<uint64_t>(i * sizeof(meta_data_t)),
                    sizeof(meta_data_t)
                );

            auto meta_data_atx = meta_data_ctx_->submit(OpCode::RECV, meta_data_assignments);
            meta_data_atx->wait(); // 等待meta_data接收完成      
            int target_slot_id = meta_data_batch[0].mr_slot;


            std::unique_lock<std::mutex> lock(queue_mutex);
            if (pend_send_task_deque.find(target_slot_id) != pend_send_task_deque.end() && !pend_send_task_deque[target_slot_id].empty())
            {
                auto task = std::move(pend_send_task_deque[target_slot_id].front());
                pend_send_task_deque[target_slot_id].pop_front();
                lock.unlock();
                task(target_slot_id);
                std::cout << "Pending task executed for slot " << target_slot_id << std::endl;
            }
            else
            {
                meta_data_deque[target_slot_id].push_back(std::move(meta_data_batch));
                lock.unlock();
                std::cout << "No pending tasks for slot " << target_slot_id << ", meta data stored in meta_data_deque" << std::endl;
            }

            meta_data_ctx_->unregister_memory_region(KEY);
            std::cout<<"The remote RDMA region received..." << std::endl;
        }


        uint8_t  max_slot_;
        uint32_t slot_id_{0};
        uint32_t batch_size_; 

        std::unique_ptr<Buffer> RECV_Buffer;
        std::atomic<bool> RECV_RUN;


        std::unique_ptr<Buffer> SEND_Buffer;
        std::atomic<bool> SEND_RUN_;

        std::shared_ptr<RDMAContext> meta_data_ctx_;

        // 假设有A，B，C，D
        std::vector<std::shared_ptr<RDMAContext>> recv_data_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> recv_meta_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> send_data_ctx_;
        std::vector<std::shared_ptr<RDMAContext>> send_meta_ctx_;

 
        std::mutex queue_mutex; 
        std::unordered_map<int, std::deque<std::unique_ptr<meta_data_t[]>>> meta_data_deque;   
        std::unordered_map<int, std::deque<std::function<void(int)>>> pend_send_task_deque;

};

}

       


// recv_data_ctx_->connect(EP_DATA); 
// mem_region_ctx->connect(EP_MR); 

// data_ctx->launch_future();
// mem_region_ctx->launch_future();


// data_ctx(std::make_shared<RDMAContext>()), mem_region_ctx(std::make_shared<RDMAContext>())
// recv_meta_data_batch_ = std::make_unique<meta_data_t[]>(batch_size_);

// meta_data_ctx_->register_memory_region( 
//                 "RECV_Meta_DATA_KEY", 
//                 reinterpret_cast<uintptr_t>(recv_meta_data_batch_.get()),
//                 batch_size_ * sizeof(meta_data_t));

            // data_ctx->init(dev_name, ib_port, link_type);
// mem_region_ctx->init(dev_name, ib_port, link_type);