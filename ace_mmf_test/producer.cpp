#include "common.h"

#include <csignal>
#include <chrono>
#include <vector>

static volatile bool g_running = true;
extern "C" void handle_signal(int) { g_running = false; }

// -------------------------------------------------------------------
// build_message  : msg 버퍼에 [sig][len][payload] 기록
//                  반환값: 기록된 총 바이트 수 (HEADER_SIZE + payload)
// -------------------------------------------------------------------
static int build_message(char* buf, int seq, int max_payload)
{
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
    if (target < min_sz)    target = min_sz;
    if (target > max_payload) target = max_payload;

    char* payload = buf + HEADER_SIZE;
    ACE_OS::memcpy(payload, header, hdr_len);

    int fill = target - hdr_len - 5;
    if (fill > 0) ACE_OS::memset(payload + hdr_len, '.', fill);
    ACE_OS::memcpy(payload + target - 5, "@REAR", 5);

    *slot_sig(buf) = SIGNATURE;
    *slot_len(buf) = target;
    return HEADER_SIZE + target;
}

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

    if (!pool_is_ready(ready_path)) {
        ::unlink(ready_path);
        ACE_OS::unlink(cfg.shm_file);
    }

    ACE_MMAP_Memory_Pool_Options pool_opts(
        ACE_DEFAULT_BASE_ADDR,
        ACE_MMAP_Memory_Pool_Options::ALWAYS_FIXED,
        true,
        cfg.mmf_size);
    SHM_ALLOC shm_alloc(cfg.shm_file, 0, &pool_opts);

    SharedHeader* hdr       = nullptr;
    char*         ring_data = nullptr;
    void* tmp = nullptr;

    if (shm_alloc.find(ACE_TEXT("hdr"), tmp) != 0) {
        // 신규 초기화
        // capacity: mmf_size의 80%를 슬롯으로 채울 수 있는 최대값 (상한 MAX_RING_CAPACITY)
        int slot_stride = HEADER_SIZE + cfg.max_payload;
        int capacity    = static_cast<int>((cfg.mmf_size * 8 / 10) / slot_stride);
        if (capacity < 4)                capacity = 4;
        if (capacity > MAX_RING_CAPACITY) capacity = MAX_RING_CAPACITY;

        hdr = static_cast<SharedHeader*>(shm_alloc.malloc(sizeof(SharedHeader)));
        if (!hdr) {
            ACE_ERROR_RETURN((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] malloc(SharedHeader) failed\n")), 1);
        }
        hdr->magic       = HEADER_MAGIC;
        hdr->write_idx   = 0;
        hdr->read_idx    = 0;
        hdr->count       = 0;
        hdr->capacity    = capacity;
        hdr->max_payload = cfg.max_payload;
        hdr->mmf_size    = static_cast<int>(cfg.mmf_size);
        hdr->slot_stride = slot_stride;
        shm_alloc.bind(ACE_TEXT("hdr"), hdr);

        // ring 슬롯 사전 할당 — 동적 malloc/free 없이 직접 복사
        size_t ring_bytes = static_cast<size_t>(capacity) * slot_stride;
        ring_data = static_cast<char*>(shm_alloc.malloc(ring_bytes));
        if (!ring_data) {
            ACE_ERROR_RETURN((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] malloc(ring_data) failed\n")), 1);
        }
        ACE_OS::memset(ring_data, 0, ring_bytes);
        shm_alloc.bind(ACE_TEXT("ring"), ring_data);

        if (!pool_write_ready(ready_path)) {
            ACE_ERROR_RETURN((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] pool_write_ready failed\n")), 1);
        }
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Producer] pool created "
                     "(capacity=%d  slot=%d B)\n"),
            capacity, slot_stride));
    } else {
        hdr = static_cast<SharedHeader*>(tmp);
        if (hdr->magic != HEADER_MAGIC) {
            ACE_ERROR_RETURN((LM_ERROR,
                ACE_TEXT("%T (%P | %t) [Producer] invalid hdr magic\n")), 1);
        }
        shm_alloc.find(ACE_TEXT("ring"), tmp);
        ring_data = static_cast<char*>(tmp);
        ACE_DEBUG((LM_INFO,
            ACE_TEXT("%T (%P | %t) [Producer] attached to existing pool\n")));
    }

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Producer] MMF=%s  max_payload=%s B  "
                 "capacity=%d\n"),
        cfg.shm_file, comma_fmt(hdr->max_payload), hdr->capacity));

    ACE_Process_Semaphore sem(0, cfg.sem_name);
    ACE_Process_Mutex mutex(cfg.mutex_name);

    int       seq        = 0;
    int       last_seq   = 0;
    long long total_bytes = 0;
    long long last_bytes  = 0;
    auto t_report = std::chrono::steady_clock::now();

    // 슬롯 크기만큼의 임시 버퍼 (스택 할당)
    std::vector<char> msg_buf(hdr->slot_stride);

    while (g_running) {
        int msg_len = build_message(msg_buf.data(), ++seq, hdr->max_payload);
        total_bytes += msg_len;

        bool wrote = false;
        {
            ACE_Guard<ACE_Process_Mutex> guard(mutex);
            if (hdr->count < hdr->capacity) {
                char* slot = ring_data + static_cast<size_t>(hdr->write_idx) * hdr->slot_stride;
                ACE_OS::memcpy(slot, msg_buf.data(), msg_len);
                hdr->write_idx = (hdr->write_idx + 1) % hdr->capacity;
                hdr->count++;
                wrote = true;
            }
        }
        if (wrote) sem.release();

        auto elapsed = std::chrono::steady_clock::now() - t_report;
        auto secs = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
        if (secs >= 1) {
            long long bdiff = total_bytes - last_bytes;
            int r = static_cast<int>((seq - last_seq) / secs);
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("%T (%P | %t) [Producer] %s msg/s  %s\n"),
                comma_fmt(r), mib_fmt(bdiff / secs)));
            t_report   = std::chrono::steady_clock::now();
            last_seq   = seq;
            last_bytes = total_bytes;
        }

        ACE_OS::thr_yield();
    }

    ACE_DEBUG((LM_INFO,
        ACE_TEXT("%T (%P | %t) [Producer] exit  produced=%s\n"),
        comma_fmt(seq)));

    ::unlink(ready_path);
    ACE_OS::unlink(cfg.shm_file);
    return 0;
}
