#include "common.h"


static volatile bool g_running = true;
extern "C" void handle_signal(int) { g_running = false; }

// ===================================================================
// TimerHandler — 0.1초마다 Reactor 가 handle_timeout() 호출
// ===================================================================
class TimerHandler : public ACE_Event_Handler {
public:
    TimerHandler(QUEUE* queue, QUEUE_ALLOCATOR* shm_alloc,
                 const char* ready_path, ACE_Process_Mutex* mutex)
        : queue_(queue), shm_alloc_(shm_alloc)
        , ready_path_(ready_path), mutex_(mutex)
        , total_popped_(0), total_bytes_(0)
        , last_popped_(0), last_bytes_(0) {}

    virtual int handle_timeout(const ACE_Time_Value&,
                               const void*) override {
        if (!g_running) return -1;

        // ---- producer 생존 확인 ----
        if (!is_ready(ready_path_)) {
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("%T (%P | %t) [Consumer] producer gone, stopping\n")));
            g_running = false;
            return -1;
        }

        int       popped = 0;
        long long bytes  = 0;

        // ================================================================
        // QueueOUT (Pool_Growth::QueueOUT) — 0.1초 동안 최대한 consume
        // ================================================================
        auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(100);

        while (g_running && std::chrono::steady_clock::now() < deadline) {
            Record record;

            {
                ACE_Guard<ACE_Process_Mutex> guard(*mutex_);

                if (queue_->is_empty()) {
                    ACE_OS::thr_yield();
                    break;
                }

                if (queue_->dequeue_head(record, shm_alloc_) == -1) {
                    ACE_DEBUG((LM_ERROR,
                        ACE_TEXT("%T (%P | %t) [Consumer] dequeue_head failed\n")));
                    break;
                }
            }

            ++popped;
            bytes += record.size();

            // ---- 데이터 처리 (여기서는 free 만) ----
            shm_alloc_->free(record.data());
        }

        total_popped_ += popped;
        total_bytes_  += bytes;

        // ---- 1초 간격 throughput 리포트 ----
        static int tick = 0;
        ++tick;
        if (tick >= 10) {   // 0.1s x 10 = 1s
            if (popped > 0 || total_popped_ > last_popped_) {
                int p = total_popped_ - last_popped_;
                long long b = total_bytes_ - last_bytes_;
                ACE_DEBUG((LM_INFO,
                    ACE_TEXT("%T (%P | %t) [Consumer] %s msg/s  %s  queue=%d\n"),
                    comma_fmt(p), mib_fmt(b),
                    queue_->size()));
            }
            last_popped_ = total_popped_;
            last_bytes_  = total_bytes_;
            tick = 0;
        }

        return 0;
    }

    int total_popped() const { return total_popped_; }

private:
    QUEUE*              queue_;
    QUEUE_ALLOCATOR*    shm_alloc_;
    const char*         ready_path_;
    ACE_Process_Mutex*  mutex_;
    int                 total_popped_;
    long long           total_bytes_;
    int                 last_popped_;
    long long           last_bytes_;
};

// ===================================================================
// find_queue — MMF allocator attach + Queue lookup (Pool_Growth::MapOpen)
// ===================================================================
static QUEUE* find_queue(QUEUE_ALLOCATOR* shm_alloc,
                         const ACE_TCHAR* queue_name) {
    void* temp = nullptr;
    if (shm_alloc->find(queue_name, temp) != 0) {
        ACE_ERROR((LM_ERROR,
            ACE_TEXT("%T (%P | %t) [Consumer] queue not found\n")));
        return nullptr;
    }
    return static_cast<QUEUE*>(temp);
}

// ===================================================================
// main
// ===================================================================
int ACE_TMAIN(int argc, ACE_TCHAR* argv[]) {
    ACE_OS::signal(SIGINT,  handle_signal);
    ACE_OS::signal(SIGTERM, handle_signal);
    ACE_LOG_MSG->set_flags(ACE_Log_Msg::STDERR);

    Config cfg;
    config_parse(cfg, argc, argv);

    if (!cfg.shm_file)   cfg.shm_file   = ACE_TEXT("/dev/shm/ace_mmf.dat");
    if (!cfg.lock_name)  cfg.lock_name  = ACE_TEXT("ace_mmf_lock");
    if (!cfg.queue_name) cfg.queue_name = ACE_TEXT("queue_test");

    // ---- producer 가 ReadyMarker 를 쓸 때까지 대기 ----
    char rdy_path[512];
    ready_path(rdy_path, sizeof(rdy_path), cfg.shm_file);

    while (g_running && !is_ready(rdy_path)) {
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Consumer] waiting for producer ...\n")));
        ACE_OS::sleep(1);
    }
    if (!g_running) return 1;

    // ---- MMF allocator attach (create=false) ----
    ACE_MMAP_Memory_Pool_Options pool_opts(
        ACE_DEFAULT_BASE_ADDR,
        ACE_MMAP_Memory_Pool_Options::ALWAYS_FIXED,
        false,
        cfg.mmf_size);

    QUEUE_ALLOCATOR shm_alloc(cfg.shm_file, 0, &pool_opts);

    // ---- Queue lookup ----
    QUEUE* queue = find_queue(&shm_alloc, cfg.queue_name);
    if (!queue) return 1;

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Consumer] start  MMF=%s\n"),
        cfg.shm_file));

    // ---- Producer 와 동일한 Mutex ----
    ACE_Process_Mutex mutex(cfg.lock_name);

    // ---- Reactor Timer 등록 (0.1초 간격) ----
    TimerHandler handler(queue, &shm_alloc, rdy_path, &mutex);

    ACE_Reactor::instance()->schedule_timer(
        &handler, 0,
        ACE_Time_Value::zero,
        ACE_Time_Value(0, 100000));   // 0.1초 = 100,000 usec

    // ---- Reactor event loop ----
    while (g_running) {
        if (ACE_Reactor::instance()->handle_events() <= 0)
            break;
    }

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Consumer] exit  consumed=%d\n"),
        handler.total_popped()));

    return 0;
}
