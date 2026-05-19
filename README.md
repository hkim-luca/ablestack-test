# ace_mmf_test

ACE (ADAPTIVE Communication Environment) 라이브러리의 MMF(Memory-Mapped File) 기반  
**단일 Producer — 다중 Consumer IPC** 참조 구현입니다.

## Features

- **고정 Ring Buffer** — Slot을 Pool 초기화 시 사전 할당하여 동적 `malloc`/`free` 제거, `/dev/shm` 소진 위험 없음
- **순서 독립 실행** — Ready Marker 파일로 Producer/Consumer 실행 순서에 관계 없이 동작
- **자동 종료** — 한쪽 프로세스 종료 시 나머지가 이를 감지하고 함께 종료
- **성능 출력** — 초당 메시지 수 / MiB/s 처리량 표시
- **상세 로깅** — Consumer는 수신한 메시지를 파일에 기록

## Requirements

- Linux (x86_64, aarch64)
- GCC 8+ (C++17)
- ACE 라이브러리 설치 (`ACE_ROOT` 환경변수 필요)
  - 다운로드: [ACE-src-7.1.0.tar.gz](https://github.com/DOCGroup/ACE_TAO/releases/download/ACE%2BTAO-7_1_0/ACE-src-7.1.0.tar.gz)
  - 압축 해제 후 `ACE_ROOT` 을 ACE 디렉터리로 설정
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
| `-s <bytes>` | MMF 크기 | 314572800 (300 MB) |
| `-p <bytes>` | 최대 Payload 크기 | 1048576 (1 MB) |
| `-f <path>` | MMF 파일 경로 | `/dev/shm/ace_mmf.dat` |
| `-m <name>` | Process Mutex 이름 | `ace_mmf_mutex` |
| `-e <name>` | Process Semaphore 이름 | `ace_mmf_sem` |
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
 SharedHeader (고정)
 ┌──────────────────────────────────────┐
 │ magic / read_idx / write_idx / count │
 │ capacity / max_payload / slot_stride │
 └──────────────────────────────────────┘
 Ring Slots (사전 할당, capacity × slot_stride)
 ┌──────────────────────────────────────┐
 │ Slot[0] : [sig(4)] [len(4)] [payload]│
 │ Slot[1] : [sig(4)] [len(4)] [payload]│
 │ ...                                  │
 │ Slot[N-1]                            │
 └──────────────────────────────────────┘
```

- **Slot 갯수**: `(mmf_size × 0.8) / slot_stride`, 최대 8,192개
- 각 Slot은 메시지를 바로 저장하므로 별도 `malloc`/`free` 불필요

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
  ├─ SHM_ALLOC 생성                  │
  ├─ malloc SharedHeader ───── bind ─┤
  ├─ malloc Ring Slots  ────── bind ─┤
  ├─ pool_write_ready()              │
  │                                  ├─ pool_is_ready() → true
  │                                  ├─ SHM_ALLOC 생성
  │                                  ├─ find("hdr") / find("ring")
  ├─ produce loop ── sem ───────────►├─ consume loop
  │                                  │
  ├─ [종료]                          │
  ├─ unlink .rdy + .dat              │
  │                                  ├─ pool_is_ready() → false
  │                                  ├─ [종료]
```

## Files

| File | Description |
|------|-------------|
| `ace_mmf_test/common.h` | 공유 타입 정의 (SharedHeader, Config, ReadyMarker, 포맷터) |
| `ace_mmf_test/producer.cpp` | Producer: 메시지 생성 및 Ring Buffer 쓰기 |
| `ace_mmf_test/consumer.cpp` | Consumer: ACE_Reactor Timer 기반 메시지 읽기 및 파일 로깅 |
| `ace_mmf_test/Makefile` | GCC 빌드 설정 |
| `README.md` | 사용 설명서 |

## Cleanup

Producer 정상 종료 시:
- `/dev/shm/ace_mmf.dat` — MMF 파일 삭제
- `/dev/shm/ace_mmf.dat.rdy` — Ready Marker 삭제

비정상 종료(Crash) 시 파일이 남아 있으면, 다음 Producer 실행 시 `pool_is_ready()`가 실패하여 stale 파일을 자동 정리합니다.
