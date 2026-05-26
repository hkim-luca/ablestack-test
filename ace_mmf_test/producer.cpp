#include "common.h"

#include <vector>

static volatile bool g_running = true;
extern "C" void handle_signal(int) { g_running = false; }

// ===================================================================
// build_message — 테스트용 메시지 생성
// ===================================================================
static int build_message(char* buf, int seq, int max_payload) {
    time_t raw = ACE_OS::gettimeofday().sec();
    struct tm tm_buf;
    ACE_OS::localtime_r(&raw, &tm_buf);
    char ts[32];
    ACE_OS::strftime(ts, sizeof(ts), "%Y%m%d%H%M%S", &tm_buf);

    char header[128];
    int hdr_len = ACE_OS::sprintf(header,
        "$HEAD%08X%08XG100 { %d \"%s\" } ", 0, 0, seq, ts);

    int pct    = (seq % 10 + 1) * 10;
    int target = static_cast<int>(static_cast<long long>(max_payload) * pct / 100);
    int min_sz = hdr_len + 5;
    if (target < min_sz)     target = min_sz;
    if (target > max_payload) target = max_payload;

    ACE_OS::memcpy(buf, header, hdr_len);
    int fill = target - hdr_len - 5;
    if (fill > 0)
        ACE_OS::memset(buf + hdr_len, '.', fill);
    ACE_OS::memcpy(buf + target - 5, "@REAR", 5);
    buf[target] = '\0';

    return target;   // payload 길이만 반환 (Record.size_에 저장)
}

// ===================================================================
// init_queue — MMF allocator 생성 + Queue find-or-create (Pool_Growth::init_MAP_Queue)
// ===================================================================
static QUEUE* init_queue(QUEUE_ALLOCATOR* shm_alloc,
                         const ACE_TCHAR* queue_name) {
    void* temp = nullptr;

    if (shm_alloc->find(queue_name, temp) != 0) {
        // 신규: Queue 를 MMF 에 할당
        temp = shm_alloc->malloc(sizeof(QUEUE));
        if (!temp) {
            ACE_ERROR((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] malloc(QUEUE) failed\n")));
            return nullptr;
        }
        new (temp) QUEUE(shm_alloc);

        if (shm_alloc->bind(queue_name, temp) == -1) {
            ACE_ERROR((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] bind(queue) failed\n")));
            return nullptr;
        }
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Producer] queue created in MMF\n")));
    } else {
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Producer] attached to existing queue\n")));
    }

    return static_cast<QUEUE*>(temp);
}

// ===================================================================
// main — QueueIN pattern (Pool_Growth::QueueIN)
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

    // ---- stale 정리 ----
    char rdy_path[512];
    ready_path(rdy_path, sizeof(rdy_path), cfg.shm_file);
    if (!is_ready(rdy_path)) {
        ::unlink(rdy_path);
        ACE_OS::unlink(cfg.shm_file);
    }

    // ---- MMF allocator 생성 (Pool_Growth::init_MAP_Queue) ----
    ACE_MMAP_Memory_Pool_Options pool_opts(
        ACE_DEFAULT_BASE_ADDR,
        ACE_MMAP_Memory_Pool_Options::ALWAYS_FIXED,
        true,                           // use_fixed_addr = true (생성)
        cfg.mmf_size);

    QUEUE_ALLOCATOR shm_alloc(cfg.shm_file, 0, &pool_opts);

    // ---- Queue 초기화 (find-or-create) ----
    QUEUE* queue = init_queue(&shm_alloc, cfg.queue_name);
    if (!queue) return 1;

    // ---- write ReadyMarker ----
    if (!write_ready(rdy_path)) {
        ACE_ERROR_RETURN((LM_ERROR,
            ACE_TEXT("%T (%P | %t) [Producer] write_ready failed\n")), 1);
    }

    // ---- Mutex (lock_name 으로 생성) ----
    ACE_Process_Mutex mutex(cfg.lock_name);

    // ---- Producer Loop (QueueIN pattern) ----
    std::vector<char> msg_buf(static_cast<size_t>(cfg.max_payload) + 1);
    int       seq         = 0;
    long long total_bytes = 0;
    long long last_bytes  = 0;
    int       last_seq    = 0;
    auto      t_report    = std::chrono::steady_clock::now();

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Producer] start  MMF=%s  size=%s B\n"),
        cfg.shm_file, comma_fmt(static_cast<int>(cfg.mmf_size))));

    while (g_running) {
        int payload_len = build_message(msg_buf.data(), ++seq, cfg.max_payload);
        int msg_len     = payload_len;   // build_message 는 payload 만 기록

        // ================================================================
        // QueueIN (Pool_Growth::QueueIN)
        //   1. mutex lock
        //   2. shmem_allocator_->malloc()  → MMF 에 공간 할당
        //   3. memcpy 데이터 복사
        //   4. Record 생성
        //   5. once_queue_->enqueue_tail()
        // ================================================================
        {
            ACE_Guard<ACE_Process_Mutex> guard(mutex);

            // --- malloc from MMF (Pool_Growth: "char *alloc_str = (char *)this->once_shmem_allocator_->malloc(str_size + 1)") ---
            char* alloc_str = static_cast<char*>(shm_alloc.malloc(msg_len + 1));
            if (!alloc_str) {
                ACE_DEBUG((LM_WARNING,
                    ACE_TEXT("%T (%P | %t) [Producer] malloc(%d) failed, "
                             "queue size=%d\n"),
                    msg_len, queue->size()));
                ACE_OS::thr_yield();
                continue;
            }

            // --- memcpy (Pool_Growth: "ACE_OS::memcpy((char *)alloc_str, (char *)str_data, str_size)") ---
            ACE_OS::memcpy(alloc_str, msg_buf.data(), msg_len);
            alloc_str[msg_len] = '\0';

            // --- Record 생성 및 enqueue (Pool_Growth: "Record newRecord(dest_type, str_size, alloc_str)") ---
            Record newRecord(1, msg_len, alloc_str);
            if (queue->enqueue_tail(newRecord, &shm_alloc) == -1) {
                ACE_DEBUG((LM_WARNING,
                    ACE_TEXT("%T (%P | %t) [Producer] enqueue_tail failed\n")));
                shm_alloc.free(alloc_str);
                ACE_OS::thr_yield();
                continue;
            }
        }

        total_bytes += msg_len;

        // ---- 1초 간격 throughput 리포트 ----
        auto elapsed = std::chrono::steady_clock::now() - t_report;
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
        if (secs >= 1) {
            long long bdiff = total_bytes - last_bytes;
            int r = static_cast<int>((seq - last_seq) / secs);
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("%T (%P | %t) [Producer] %s msg/s  %s  queue=%d\n"),
                comma_fmt(r), mib_fmt(bdiff / secs), queue->size()));
            t_report   = std::chrono::steady_clock::now();
            last_seq   = seq;
            last_bytes = total_bytes;
        }
    }

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Producer] exit  produced=%s  remaining=%d\n"),
        comma_fmt(seq), queue->size()));

    ::unlink(rdy_path);
    ACE_OS::unlink(cfg.shm_file);

    return 0;
}
