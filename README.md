# ace_mmf_test

ACE (ADAPTIVE Communication Environment) 라이브러리의 MMF(Memory-Mapped File) 기반  
**단일 Producer — 다중 Consumer IPC** 참조 구현입니다.

ACE 라이브러리 `Pool_Growth` 패턴에서 **QueueIN**(Producer) / **QueueOUT**(Consumer) 로직을 추출하여  
MMF 공유 메모리 위에서 `ACE_Unbounded_Queue`를 통해 메시지를 전달합니다.

## Features

- **MMF 기반 IPC** — `ACE_Malloc<ACE_MMAP_Memory_Pool>`로 공유 메모리 할당자 구성
- **ACE_Unbounded_Queue** — MMF에 Queue를 할당하여 Producer/Consumer가 동일한 Queue 공유
- **순서 독립 실행** — Ready Marker 파일로 Producer/Consumer 실행 순서에 관계 없이 동작
- **자동 종료** — 한쪽 프로세스 종료 시 나머지가 이를 감지하고 함께 종료
- **성능 출력** — 초당 메시지 수 / MiB/s 처리량 표시
- **Reactor Timer 기반 Consumer** — 0.1초 주기로 `ACE_Reactor` Timer가 QueueOUT 호출

## Requirements

- Linux (x86_64, aarch64)
- GCC 8+ (C++17)
- ACE 라이브러리 설치 (`ACE_ROOT` 환경변수 필요)
  - 다운로드: [ACE-src-7.1.0.tar.gz](https://github.com/DOCGroup/ACE_TAO/releases/download/ACE%2BTAO-7_1_0/ACE-src-7.1.0.tar.gz)
  - 압축 해제 후 `ACE_ROOT`을 ACE 디렉터리로 설정
- `/dev/shm` (tmpfs)

## Build

```bash
export ACE_ROOT=/path/to/ACE_wrappers
cd ace_mmf_test

make          # Release 빌드 (-O2)
make debug    # Debug 빌드 (-g -O0)
```

## Quick Start

**Producer**와 **Consumer** 실행 순서는 무관합니다.

### Terminal 1 — Producer

```bash
cd ace_mmf_test
./producer
```

### Terminal 2 — Consumer

```bash
cd ace_mmf_test
./consumer
```

Producer가 먼저 시작되면 MMF Pool을 생성하고 Ready Marker를 기록합니다.  
Consumer가 먼저 시작되면 Ready Marker가 나타날 때까지 1초 간격으로 폴링하며 대기합니다.

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `-s <bytes>` | MMF 크기 | 52428800 (50 MB) |
| `-p <bytes>` | 최대 Payload 크기 | 1048576 (1 MB) |
| `-f <path>` | MMF 파일 경로 | `/dev/shm/ace_mmf.dat` |
| `-l <name>` | Process Mutex 이름 | `ace_mmf_lock` |
| `-q <name>` | Queue 이름 (MMF 내 lookup key) | `queue_test` |
| `-e <name>` | Optional Semaphore 이름 | |
| `-h` | 도움말 | |

### 예시

```bash
# MMF 512 MB, Payload 2 MB
cd ace_mmf_test
./producer -s 536870912 -p 2097152
./consumer
```

## Architecture

### Shared Memory Layout

```
MMF 파일 (/dev/shm/ace_mmf.dat)
 ┌──────────────────────────────────────┐
 │ ACE_Malloc 관리 영역 (free list 등)    │
 ├──────────────────────────────────────┤
 │ ACE_Unbounded_Queue (head/tail/node) │
 │   ┌─────┐  ┌─────┐  ┌─────┐         │
 │   │Node │⇄│Node │⇄│Node │...       │
 │   │data→│  │data→│  │data→│         │
 │   └─────┘  └─────┘  └─────┘         │
 ├──────────────────────────────────────┤
 │ Payload 영역 (malloc/free)            │
 │ ┌──────┐ ┌──────┐ ┌──────┐           │
 │ │msg 1 │ │msg 2 │ │msg 3 │ ...       │
 │ └──────┘ └──────┘ └──────┘           │
 └──────────────────────────────────────┘
```

- **Queue 노드**와 **Payload**가 동일한 MMF 내에서 `malloc`/`free`로 관리됨
- 동적 할당이므로 메시지 크기가 가변적이며, 고정 Slot 방식보다 메모리 효율적

### QueueIN Pattern (Producer)

```cpp
ACE_Guard<ACE_Process_Mutex> guard(mutex);
char* alloc_str = shm_alloc.malloc(msg_len + 1);
memcpy(alloc_str, msg_buf.data(), msg_len);
Record newRecord(1, msg_len, alloc_str);
queue->enqueue_tail(newRecord, &shm_alloc);
```

1. Mutex lock
2. MMF에 Payload 공간 할당 (`malloc`)
3. 데이터 복사 (`memcpy`)
4. `Record` 생성 및 Queue에 삽입 (`enqueue_tail`)

### QueueOUT Pattern (Consumer)

```cpp
{
    ACE_Guard<ACE_Process_Mutex> guard(mutex);
    if (queue->is_empty()) break;
    queue->dequeue_head(record, &shm_alloc);
}
// mutex 해제 후 데이터 처리
shm_alloc.free(record.data());
```

1. Mutex lock
2. Queue에서 `Record` 추출 (`dequeue_head`)
3. Mutex unlock
4. 데이터 처리 후 MMF에 반납 (`free`)

### 동기화

| 메커니즘 | 대상 | 설명 |
|----------|------|------|
| `ACE_Process_Mutex` | Queue 자료구조 | Producer/Consumer 간 Queue 접근 직렬화 (System V semaphore) |
| `ACE_Malloc` internal lock | MMF 할당자 | MMF 내부 free list 보호 (auto-generated key) |

두 Lock은 서로 다른 이름을 사용하므로 데드락이 발생하지 않습니다.

### Ready Marker (`/dev/shm/ace_mmf.dat.rdy`)

```
struct ReadyMarker {
    int32_t magic;  // HEADER_MAGIC (0x414345)
    int32_t pid;    // Producer PID
};
```

Consumer는 `read()` 후 `magic` 검증과 `kill(pid, 0)` 생존 확인을 모두 통과해야 Pool에 접근합니다.  
이를 통해 Producer 초기화 도중 Consumer가 접근하여 발생하는 **SEGV를 원천 차단**합니다.

### Flow

```
Producer                           Consumer
  │                                  │
  ├─ unlink stale files              │
  ├─ SHM_ALLOC 생성 (create=true)    │
  ├─ QUEUE를 MMF에 할당 + bind       │
  ├─ write_ready()                   │
  │                                  ├─ is_ready() → true
  │                                  ├─ SHM_ALLOC 생성 (create=false)
  │                                  ├─ find("queue_test") → QUEUE*
  ├─ produce loop ── sem ──────────►├─ Reactor Timer (0.1s)
  │     QueueIN                      │     QueueOUT
  │   ① mutex lock                   │   ① mutex lock
  │   ② malloc(MMF)                  │   ② is_empty() / dequeue_head()
  │   ③ memcpy                       │   ③ mutex unlock
  │   ④ enqueue_tail()               │   ④ free(MMF)
  │   ⑤ mutex unlock                 │
  │                                  │
  ├─ [종료]                          │
  ├─ unlink .rdy + .dat              │
  │                                  ├─ is_ready() → false
  │                                  ├─ [종료]
```

## Files

| File | Description |
|------|-------------|
| `ace_mmf_test/common.h` | 공유 타입 정의 (Record, Unbounded_Queue, Config, ReadyMarker, 포맷터) |
| `ace_mmf_test/producer.cpp` | Producer: QueueIN 패턴 — MMF 할당 → 복사 → enqueue |
| `ace_mmf_test/consumer.cpp` | Consumer: ACE_Reactor Timer 기반 QueueOUT 패턴 — dequeue → free |
| `ace_mmf_test/Makefile` | GCC 빌드 설정 |
| `README.md` | 사용 설명서 |

## Cleanup

Producer 정상 종료 시:
- `/dev/shm/ace_mmf.dat` — MMF 파일 삭제
- `/dev/shm/ace_mmf.dat.rdy` — Ready Marker 삭제

비정상 종료(Crash) 시 파일이 남아 있으면, 다음 Producer 실행 시 `pool_is_ready()`가 실패하여 stale 파일을 자동 정리합니다.

