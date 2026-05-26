#ifndef ACE_MMF_TEST_COMMON_H
#define ACE_MMF_TEST_COMMON_H

#include <fcntl.h>
#include <unistd.h>
#include <csignal>
#include <chrono>
#include <cstdint>

#include "ace/Malloc_T.h"
#include "ace/MMAP_Memory_Pool.h"
#include "ace/Guard_T.h"
#include "ace/Process_Mutex.h"
#include "ace/Log_Msg.h"
#include "ace/OS.h"
#include "ace/Get_Opt.h"
#include "ace/Reactor.h"
#include "ace/Time_Value.h"
#include "ace/Unbounded_Queue.h"

// ===================================================================
// Pool_Growth 에서 추출한 핵심 타입
// ===================================================================

// ---- Record (SimUtil/record.h 호환) ----
struct Record {
    int    type_;
    int    size_;
    char*  data_;

    Record() : type_(0), size_(0), data_(nullptr) {}
    Record(int t, int s, char* d) : type_(t), size_(s), data_(d) {}
    void Init() { type_ = 0; size_ = 0; data_ = nullptr; }
    char* data() const { return data_; }
    int   size() const { return size_; }
};

// ---- Unbounded_Queue wrapper (Pool_Growth.h) ----
template <class T>
class Unbounded_Queue : public ACE_Unbounded_Queue<T> {
public:
    typedef ACE_Unbounded_Queue<T> BASE;

    Unbounded_Queue(ACE_Allocator* allocator = nullptr)
        : ACE_Unbounded_Queue<T>(allocator) {}

    int enqueue_tail(const T& new_item, ACE_Allocator* allocator) {
        this->allocator_ = allocator;
        return BASE::enqueue_tail(new_item);
    }

    int dequeue_head(T& item, ACE_Allocator* allocator) {
        this->allocator_ = allocator;
        return BASE::dequeue_head(item);
    }
};

typedef Unbounded_Queue<Record> QUEUE;

// ---- MMF-backed allocator (Pool_Growth) ----
typedef ACE_Malloc<ACE_MMAP_Memory_Pool, ACE_Process_Mutex>   MALLOC;
typedef ACE_Allocator_Adapter<MALLOC>                          QUEUE_ALLOCATOR;

// ===================================================================
// Defaults
// ===================================================================
const size_t DEFAULT_MMF_SIZE    = 50UL * 1024 * 1024;   // 50 MB
const int    DEFAULT_MAX_PAYLOAD = 1024 * 1024;           // 1 MB
const int    HEADER_MAGIC        = 0x414345;

// ===================================================================
// CLI Config
// ===================================================================
struct Config {
    size_t mmf_size    = DEFAULT_MMF_SIZE;
    int    max_payload = DEFAULT_MAX_PAYLOAD;
    const ACE_TCHAR* shm_file   = nullptr;
    const ACE_TCHAR* lock_name  = nullptr;
    const ACE_TCHAR* queue_name = nullptr;
};

inline void config_parse(Config& cfg, int argc, ACE_TCHAR* argv[]) {
    ACE_Get_Opt opt(argc, argv, ACE_TEXT("s:p:f:l:q:h"));
    for (int c; (c = opt()) != -1;) {
        switch (c) {
        case 's': cfg.mmf_size    = static_cast<size_t>(ACE_OS::atoi(opt.opt_arg())); break;
        case 'p': cfg.max_payload = ACE_OS::atoi(opt.opt_arg()); break;
        case 'f': cfg.shm_file    = opt.opt_arg(); break;
        case 'l': cfg.lock_name   = opt.opt_arg(); break;
        case 'q': cfg.queue_name  = opt.opt_arg(); break;
        case 'h':
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("Usage: %s [options]\n")
                ACE_TEXT("  -s <size>     MMF size (default: 50 MB)\n")
                ACE_TEXT("  -p <bytes>    Max payload (default: 1 MB)\n")
                ACE_TEXT("  -f <path>     MMF backing-store path\n")
                ACE_TEXT("                (default: /dev/shm/ace_mmf.dat)\n")
                ACE_TEXT("  -l <name>     Lock file / mutex name\n")
                ACE_TEXT("                (default: ace_mmf_lock)\n")
                ACE_TEXT("  -q <name>     Queue name inside MMF\n")
                ACE_TEXT("                (default: queue_test)\n")
                ACE_TEXT("  -e <name>     Optional semaphore name\n")
                ACE_TEXT("  -h            Show this help\n"),
                argc > 0 ? argv[0] : ACE_TEXT("prog")));
            ACE_OS::exit(0);
        }
    }
}

// ===================================================================
// ReadyMarker – producer init 완료 후 기록, consumer 가 대기
// ===================================================================
struct ReadyMarker {
    int32_t magic;
    int32_t pid;
};

inline void ready_path(char* buf, size_t sz, const ACE_TCHAR* shm_file) {
    ACE_OS::snprintf(buf, sz, ACE_TEXT("%s.rdy"), shm_file);
}

inline bool write_ready(const char* ready_path) {
    ReadyMarker m;
    m.magic = HEADER_MAGIC;
    m.pid   = static_cast<int32_t>(ACE_OS::getpid());
    int fd = ::open(ready_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    bool ok = ::write(fd, &m, sizeof(m)) == static_cast<ssize_t>(sizeof(m));
    ::close(fd);
    return ok;
}

inline bool is_ready(const char* ready_path) {
    ReadyMarker m = {};
    int fd = ::open(ready_path, O_RDONLY);
    if (fd < 0) return false;
    bool ok = ::read(fd, &m, sizeof(m)) == static_cast<ssize_t>(sizeof(m));
    ::close(fd);
    return ok && m.magic == HEADER_MAGIC && ::kill(static_cast<pid_t>(m.pid), 0) == 0;
}

// ===================================================================
// Formatters
// ===================================================================
inline const char* comma_fmt(int val) {
    static char bufs[4][32];
    static int idx = 0;
    idx = (idx + 1) % 4;
    char* buf = bufs[idx];
    char tmp[32];
    int len = ACE_OS::snprintf(tmp, sizeof(tmp), "%d", val);
    char* p = buf;
    int remain = len;
    int first = remain % 3 ? remain % 3 : 3;
    ACE_OS::memcpy(p, tmp, first);
    p += first;
    remain -= first;
    while (remain > 0) {
        *p++ = ',';
        ACE_OS::memcpy(p, tmp + len - remain, 3);
        p += 3;
        remain -= 3;
    }
    *p = '\0';
    return buf;
}

inline const char* mib_fmt(long long bytes_per_sec) {
    static char bufs[4][32];
    static int idx = 0;
    idx = (idx + 1) % 4;
    double mib = bytes_per_sec / (1024.0 * 1024.0);
    ACE_OS::snprintf(bufs[idx], sizeof(bufs[idx]), "%.3f MiB/s", mib);
    return bufs[idx];
}

#endif // ACE_MMF_TEST_COMMON_H
