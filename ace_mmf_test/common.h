#ifndef ACE_MMF_TEST_COMMON_H
#define ACE_MMF_TEST_COMMON_H

#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#include "ace/Malloc_T.h"
#include "ace/MMAP_Memory_Pool.h"
#include "ace/Guard_T.h"
#include "ace/Process_Mutex.h"
#include "ace/Process_Semaphore.h"
#include "ace/Log_Msg.h"
#include "ace/OS.h"
#include "ace/Reactor.h"
#include "ace/Time_Value.h"
#include "ace/Get_Opt.h"

// ---- shared memory allocator (MMF-backed, process-safe) ----
typedef ACE_Malloc_T<ACE_MMAP_Memory_Pool, ACE_Process_Mutex, ACE_Control_Block> SHM_ALLOC;

const int  SIGNATURE    = 123456789;
const int  HEADER_SIZE  = 8;          // signature(4) + length(4)
const int  HEADER_MAGIC = 0x414345;   // marks initialized header

// ---- defaults -----------------------------------------
const size_t DEFAULT_MMF_SIZE    = 300UL * 1024 * 1024;  // 300 MB
const int    DEFAULT_MAX_PAYLOAD = 1 * 1024 * 1024;       // 1 MB
const int    MAX_RING_CAPACITY   = 8192;                   // 상한

// ---- shared-memory header ----
// ring 슬롯은 SharedHeader 뒤에 사전 할당된 연속 영역에 있음:
//   ring_data[i * slot_stride]  (i = 0 .. capacity-1)
// 슬롯 레이아웃: [int sig][int len][char payload[max_payload]]
// 동적 malloc/free 없음 → /dev/shm OOM 원천 차단
struct SharedHeader {
    int  magic;
    int  write_idx;
    int  read_idx;
    int  count;
    int  capacity;      // 실제 ring 슬롯 수 (mmf_size에 맞게 자동 계산)
    int  max_payload;
    int  mmf_size;
    int  slot_stride;   // HEADER_SIZE + max_payload (슬롯 1개 크기)
};

// ---- ring 슬롯 접근 헬퍼 ----
inline int*  slot_sig(char* slot)     { return reinterpret_cast<int*>(slot); }
inline int*  slot_len(char* slot)     { return reinterpret_cast<int*>(slot) + 1; }
inline char* slot_payload(char* slot) { return slot + HEADER_SIZE; }

// ---- comma formatting for throughput numbers -------------
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

// ---- MiB/s formatter ------------------------------------
inline const char* mib_fmt(long long bytes_per_sec) {
    static char bufs[4][32];
    static int idx = 0;
    idx = (idx + 1) % 4;
    double mib = bytes_per_sec / (1024.0 * 1024.0);
    ACE_OS::snprintf(bufs[idx], sizeof(bufs[idx]), "%.3f MiB/s", mib);
    return bufs[idx];
}

// ---- pool ready-marker (파일 경로: shm_file + ".rdy") ----
// producer가 pool 완전 초기화 후 기록. consumer는 이 파일이 유효할 때만 pool 접근.
struct ReadyMarker {
    int32_t magic;   // HEADER_MAGIC
    int32_t pid;     // producer PID (생존 여부 확인용)
};

inline void pool_ready_path(char* buf, size_t sz, const char* shm_file) {
    ACE_OS::snprintf(buf, sz, "%s.rdy", shm_file);
}

inline bool pool_write_ready(const char* ready_path) {
    ReadyMarker m;
    m.magic = HEADER_MAGIC;
    m.pid   = static_cast<int32_t>(ACE_OS::getpid());
    int fd = ::open(ready_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    bool ok = ::write(fd, &m, sizeof(m)) == static_cast<ssize_t>(sizeof(m));
    ::close(fd);
    return ok;
}

inline bool pool_is_ready(const char* ready_path) {
    ReadyMarker m = {};
    int fd = ::open(ready_path, O_RDONLY);
    if (fd < 0) return false;
    bool ok = ::read(fd, &m, sizeof(m)) == static_cast<ssize_t>(sizeof(m));
    ::close(fd);
    // magic 확인 + producer 프로세스 생존 확인
    return ok && m.magic == HEADER_MAGIC && ::kill(static_cast<pid_t>(m.pid), 0) == 0;
}

// ---- CLI / env helper ------------------------------------
struct Config {
    size_t mmf_size    = DEFAULT_MMF_SIZE;
    int    max_payload = DEFAULT_MAX_PAYLOAD;
    const ACE_TCHAR* shm_file   = 0;
    const ACE_TCHAR* mutex_name = 0;
    const ACE_TCHAR* sem_name   = 0;
};

inline void config_parse(Config& cfg, int argc, ACE_TCHAR* argv[]) {
    ACE_Get_Opt opt(argc, argv, ACE_TEXT("s:p:f:m:e:h"));
    for (int c; (c = opt()) != -1;) {
        switch (c) {
        case 's': cfg.mmf_size    = ACE_OS::atoi(opt.opt_arg()); break;
        case 'p': cfg.max_payload = ACE_OS::atoi(opt.opt_arg()); break;
        case 'f': cfg.shm_file    = opt.opt_arg(); break;
        case 'm': cfg.mutex_name  = opt.opt_arg(); break;
        case 'e': cfg.sem_name    = opt.opt_arg(); break;
        case 'h':
            ACE_DEBUG((LM_INFO,
                ACE_TEXT("Usage: %s [options]\n")
                ACE_TEXT("  -s <size>     MMF size (default: 300 MB)\n")
                ACE_TEXT("  -p <bytes>    Max payload (default: 1 MB)\n")
                ACE_TEXT("  -f <path>     MMF file path\n")
                ACE_TEXT("                (default: /dev/shm/ace_mmf.dat)\n")
                ACE_TEXT("  -m <name>     Process mutex name\n")
                ACE_TEXT("                (default: ace_mmf_mutex)\n")
                ACE_TEXT("  -e <name>     Process semaphore name\n")
                ACE_TEXT("                (default: ace_mmf_sem)\n")
                ACE_TEXT("  -h            Show this help\n"),
                argc > 0 ? argv[0] : ACE_TEXT("prog")));
            ACE_OS::exit(0);
        }
    }
}

#endif
