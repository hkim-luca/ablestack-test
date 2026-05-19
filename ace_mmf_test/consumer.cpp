#include "common.h"

#include <csignal>
#include <chrono>

static volatile bool g_running = true;
extern "C" void handle_signal(int) { g_running = false; }

// -------------------------------------------------------------------
// TimerHandler  — invoked every 0.1 s by ACE_Reactor
// -------------------------------------------------------------------
class TimerHandler : public ACE_Event_Handler {
public:
    TimerHandler(SharedHeader* hdr, char* ring_data,
                 ACE_Process_Mutex* mtx,
                 ACE_Process_Semaphore* sem,
                 FILE* log,
                 const char* ready_path)
        : hdr_(hdr), ring_data_(ring_data)
        , mtx_(mtx), sem_(sem), log_(log)
        , ready_path_(ready_path)
        , tick_(0), acc_msg_(0), acc_byte_(0LL) {}

    virtual int handle_timeout(const ACE_Time_Value&,
                               const void*) override {
        if (!g_running) return -1;

        int       popped = 0;
        long long bytes  = 0;

        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
        while (g_running && std::chrono::steady_clock::now() < deadline) {
            if (sem_->tryacquire() != 0) {
                ACE_OS::thr_yield();
                continue;
            }

            int  sig = 0;
            int  len = 0;
            char* payload = nullptr;

            {
                ACE_Guard<ACE_Process_Mutex> guard(*mtx_);
                if (hdr_->count <= 0) break;

                char* slot = ring_data_ +
                    static_cast<size_t>(hdr_->read_idx) * hdr_->slot_stride;
                sig     = *slot_sig(slot);
                len     = *slot_len(slot);
                payload = slot_payload(slot);

                hdr_->read_idx = (hdr_->read_idx + 1) % hdr_->capacity;
                hdr_->count--;
            }

            if (sig == SIGNATURE && len > 0) {
                ++popped;
                bytes += HEADER_SIZE + len;

                if (log_) {
                    int show = len < 256 ? len : 255;
                    char save = payload[show];
                    payload[show] = '\0';
                    ACE_OS::fprintf(log_,
                        "[%d] sig=%d len=%d  %s\n",
                        popped, sig, len, payload);
                    payload[show] = save;
                }
            }
            // 슬롯은 ring_data 내 고정 영역 — free 불필요
        }

        acc_msg_  += popped;
        acc_byte_ += bytes;
        ++tick_;

        if (tick_ >= 10) {
            if (!pool_is_ready(ready_path_)) {
                ACE_DEBUG((LM_INFO,
                    ACE_TEXT("%T (%P | %t) [Consumer] producer gone, stopping\n")));
                g_running = false;
                return -1;
            }
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("%T (%P | %t) [Consumer] %s msg/s  %s  pending: %d\n"),
                comma_fmt(acc_msg_), mib_fmt(acc_byte_), hdr_->count));
            tick_     = 0;
            acc_msg_  = 0;
            acc_byte_ = 0;
        }
        return 0;
    }

private:
    SharedHeader*      hdr_;
    char*              ring_data_;
    ACE_Process_Mutex* mtx_;
    ACE_Process_Semaphore* sem_;
    FILE*              log_;
    const char*        ready_path_;
    int                tick_;
    int                acc_msg_;
    long long          acc_byte_;
};

// -------------------------------------------------------------------
// main
// -------------------------------------------------------------------
int ACE_TMAIN(int argc, ACE_TCHAR* argv[]) {
    ACE_OS::signal(SIGINT,  handle_signal);
    ACE_OS::signal(SIGTERM, handle_signal);
    ACE_LOG_MSG->set_flags(ACE_Log_Msg::STDERR);

    Config cfg;
    config_parse(cfg, argc, argv);

    if (!cfg.shm_file)   cfg.shm_file   = ACE_TEXT("/dev/shm/ace_mmf.dat");
    if (!cfg.mutex_name) cfg.mutex_name = ACE_TEXT("ace_mmf_mutex");
    if (!cfg.sem_name)   cfg.sem_name   = ACE_TEXT("ace_mmf_sem");

    char ready_path[512];
    pool_ready_path(ready_path, sizeof(ready_path), cfg.shm_file);

    while (g_running && !pool_is_ready(ready_path)) {
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Consumer] waiting for producer ...\n")));
        ACE_OS::sleep(1);
    }
    if (!g_running) return 1;

    ACE_MMAP_Memory_Pool_Options pool_opts(
        ACE_DEFAULT_BASE_ADDR,
        ACE_MMAP_Memory_Pool_Options::ALWAYS_FIXED,
        false,
        cfg.mmf_size);
    SHM_ALLOC shm_alloc(cfg.shm_file, 0, &pool_opts);

    void* tmp = nullptr;
    if (shm_alloc.find(ACE_TEXT("hdr"), tmp) != 0) {
        ACE_ERROR_RETURN((LM_ERROR,
            ACE_TEXT("%T (%P | %t) [Consumer] hdr not found after ready\n")), 1);
    }
    SharedHeader* hdr = static_cast<SharedHeader*>(tmp);
    if (hdr->magic != HEADER_MAGIC) {
        ACE_ERROR_RETURN((LM_ERROR,
            ACE_TEXT("%T (%P | %t) [Consumer] invalid hdr magic\n")), 1);
    }

    shm_alloc.find(ACE_TEXT("ring"), tmp);
    char* ring_data = static_cast<char*>(tmp);

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Consumer] MMF=%s  max_payload=%s B  "
                 "capacity=%d\n"),
        cfg.shm_file, comma_fmt(hdr->max_payload), hdr->capacity));

    ACE_Process_Mutex mutex(cfg.mutex_name);
    ACE_Process_Semaphore sem(0, cfg.sem_name);

    char log_path[256];
    ACE_OS::sprintf(log_path, "consumer_%d.log",
                    static_cast<int>(ACE_OS::getpid()));
    FILE* log_fp = ACE_OS::fopen(log_path, "w");
    if (!log_fp) {
        ACE_ERROR((LM_ERROR,
                   ACE_TEXT("%T (%P | %t) [Consumer] cannot open %s\n"), log_path));
        return 1;
    }

    TimerHandler handler(hdr, ring_data, &mutex, &sem, log_fp, ready_path);
    ACE_Reactor::instance()->schedule_timer(
        &handler, 0,
        ACE_Time_Value::zero,
        ACE_Time_Value(0, 100000));

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Consumer] reactor timer started (100 ms)\n")));

    while (g_running) {
        if (ACE_Reactor::instance()->handle_events() <= 0)
            break;
    }

    ACE_DEBUG((LM_INFO, ACE_TEXT("%T (%P | %t) [Consumer] exit\n")));
    ACE_OS::fclose(log_fp);
    return 0;
}
