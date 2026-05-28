# SegmentedInteger

**목적**: Int64 시퀀스를 패턴 감지 기반으로 압축하여 저장 공간 절감 및 네트워크 전송 효율화

---

## 라이브러리 구성

| 클래스 | 용도 | 입력 | 특징 |
|---|---|---|---|
| **BlockedInteger** | 임의의 Int64 데이터 | `IEnumerable<Int64>` | 순서 보장, 중복 허용 |
| **SortedSetInteger** | 정렬된 집합 (0 포함 양수) | `SortedSet<Int64>` | 2가지 청크 타입, 중복 없음 |

> **개발 순서**: `SortedSetInteger` → `BlockedInteger` (더 범용적인 확장 버전)

---

## BlockedInteger: 패턴 감지 기반 압축

임의의 `Int64` 시퀀스를 Protobuf 직렬화로 압축합니다. (순서 보장, 중복 허용)

> **특징**: 8가지 블록 타입 자동 선택

### 8가지 블록 타입 (우선순위 순)

| 우선순위 | 블록 타입 | 선택 조건 | Wire Format | 압축 효율 |
|---|---|---|---|---|
| 1 | **ConstantBlock** | 모든 값 동일, count ≥ 3 | (value, count) | 극대 |
| 2 | **ArithmeticBlock** | 등차수열, count ≥ 3 | (first, step, count) | 극대 |
| 3 | **AscendingBitmapBlock** | strictly ascending ∧ range ≤ 63 ∧ count ≥ 8 | first + uint64 bits | 높음 |
| 4 | **AscendingBlock** | 단조증가 (비내림차순) | first + uint64 diffs[] | 중상 |
| 5 | **DescendingBitmapBlock** | strictly descending ∧ range ≤ 63 ∧ count ≥ 8 | first + uint64 bits | 높음 |
| 6 | **DescendingBlock** | 단조감소 (비오름차순) | first + uint64 diffs[] | 중상 |
| 7 | **DeltaOfDeltaBlock** | max\|dod\| ≤ 63 ∧ count ≥ 3 | first + first_delta + sint64 dods[] | 중상 |
| 8 | **DeltaBlock** | range ≤ 8,191 (기본값) | reference + sint64 deltas[] | 중하 |

### 설계 원칙

**Greedy 스트리밍 선택**: 
- 입력이 동일하면 항상 동일한 출력 (Deterministic)
- 값을 1개씩 수집하며 실시간 패턴 감지
- 블록 조건 만족 시 즉시 확정 (백트래킹 없음)
- 시간 효율성 우선, 최적 압축률 약간 포기 가능

**주요 제약조건**:
- **블록당 값 수**: 최대 8,192개 (proto spec 준수)
- **ConstantBlock·ArithmeticBlock**: count ≥ 3 필수
- **BitmapBlock**: range ≤ 63, count ≥ 8 필수, strictly ascending/descending 필수
- **DeltaOfDeltaBlock**: max|dod| ≤ 63만 인코더 선택 (proto limit ≤ 8,191), count ≥ 3
- **DeltaBlock**: range ≤ 8,191 (2-byte zigzag 저장)

**블록 분리**:
- 단조성(ascending/descending)이 동시에 깨지고 range > 8,191 → 새 블록 시작
- 블록 값 수가 8,192 초과 → 자동 분리
- Constant/Arithmetic 접두부(≥5개)가 비단조 데이터 앞에 있으면 접두부를 먼저 분리하여 emit

### API

```csharp
// 인코딩
BlockedInteger.Encode(ReadOnlySpan<Int64> values, out Pb.BlockedInteger proto);
BlockedInteger.Encode(IEnumerable<Int64> values, out Pb.BlockedInteger proto);

// 디코딩 (전체)
BlockedInteger.Decode(Pb.BlockedInteger proto, out IReadOnlyList<Int64> integers);

// 디코딩 (페이지 기반 스트리밍)
Int32 pageCount = BlockedInteger.GetPageCount(proto, pageSize: 1000);
BlockedInteger.DecodePage(proto, pageIndex, pageSize, out var page);
```

### 사용 예

**기본 사용**:
```csharp
Int64[] input = [9000, 9001, 9002, -10001, -10006, -10009, 100, 100, 100];

BlockedInteger.Encode(input, out var proto);
// → 3개 블록으로 자동 분할 (그리디 스트리밍 알고리즘):
//   Block 0: ArithmeticBlock [9000, 9001, 9002]
//     (first=9000, step=1, count=3)
//   Block 1: DescendingBlock [-10001, -10006, -10009]
//     (first=-10001, diffs=[5, 3])
//   Block 2: ConstantBlock [100, 100, 100]
//     (value=100, count=3)

BlockedInteger.Decode(proto, out var decoded);
// → [9000, 9001, 9002, -10001, -10006, -10009, 100, 100, 100]
```

**페이지 기반 스트리밍**:
```csharp
Int32 pageCount = BlockedInteger.GetPageCount(proto, pageSize: 1000);
for (Int32 i = 0; i < pageCount; ++i) {
    BlockedInteger.DecodePage(proto, i, 1000, out var page);
    // → 페이지 단위로 처리
}
```
---

## SortedSetInteger

정렬된 0 포함 양수 `Int64` 집합을 두 가지 청크 방식으로 Serialize 합니다.

### 청크 타입

| 타입 | 선택 조건 | 저장 방식 |
|---|---|---|
| **BitmapChunk** | 인접 값 간격 < 64 | 시작값 + 증분 비트맵 (최대 8바이트) |
| **IncrementChunk** | 인접 값 간격 < 2,000,000 | 시작값 + 절대 오프셋 리스트 |

간격이 두 조건을 모두 초과하면 값마다 개별 `IncrementChunk`를 생성합니다.  
`BitmapChunk`는 63개 값이 모두 채워지면 `Filled=true`로 비트맵을 생략합니다.

### API

```csharp
// SortedSet 인코딩
SortedSetInteger.Encode(SortedSet<Int64> sorted, out Pb.SortedSetInteger proto);

// ReadOnlySpan 인코딩 (useSortValidation=false 시 호출자가 정렬·비음수 보장)
SortedSetInteger.Encode(ReadOnlySpan<Int64> sorted, out Pb.SortedSetInteger proto,
    bool useSortValidation = true);

// 디코딩
SortedSetInteger.Decode(Pb.SortedSetInteger proto, out SortedSet<Int64> integers);
```

### 사용 예

```csharp
SortedSet<Int64> values = [0, 1, 2, 5, 10_000, 10_001];

SortedSetInteger.Encode(values, out var proto);
// → BitmapChunk(0..5) + IncrementChunk(10000..10001)

SortedSetInteger.Decode(proto, out var decoded);
// → {0, 1, 2, 5, 10000, 10001}
```

---

## 성능 및 최적화

### System.Numerics Vector 기반 SIMD 최적화

**DecodeConstant**: `Span<long>.Fill()` (런타임 벡터화)
- 반복 값 채우기 시 런타임이 자동 벡터화

**DecodeArithmetic**: `Vector<T>` 명시적 벡터화 (FillArithmetic)
- 플랫폼 자동 선택: AVX2(width=4) / NEON(width=2) / AVX-512(width=8)
- `Vector.IsHardwareAccelerated` 확인 후 벡터 연산 사용
- 꼬리(tail) 처리: 벡터화 후 남은 요소는 스칼라로 처리
- `unchecked()` 블록: CheckForOverflowUnderflow=true 대응

**CollectionsMarshal 버퍼 관리**:
- `SetCount` + `AsSpan`으로 List 사전할당
- bounds check 제거로 메모리 할당 최소화
- GC 압력 감소

---

## Protobuf 직렬화

### 스키마 및 호환성

**사용 스키마**: `Library/Protos/default.proto` (Google Protobuf 3)

**특징**:
- **Deterministic**: 동일 입력 → 동일 바이트 시퀀스 (캐싱 및 비교 가능)
- **호환성**: 새 블록/청크 타입 추가 시에도 기존 데이터 읽기 가능 (oneof 사용)
- **자동 생성**: 빌드 시 protoc가 `Library/ProtoOuts/Default.cs` 자동 생성

**주의**: 블록 선택 로직(우선순위, 조건)을 변경하면 바이트 호환성이 깨집니다.

---

## 프로젝트 구조

```
SegmentedInteger/
│
├── SegmentedInteger/                 # 솔루션 디렉토리
│   ├── Library/                      # 핵심 라이브러리
│   │   ├── Library.csproj
│   │   ├── Protos/default.proto      # Protobuf 스키마
│   │   ├── ProtoOuts/Default.cs      # protoc 자동 생성 (git 제외)
│   │   ├── SegmentedIntegers/
│   │   │   ├── BlockedInteger.cs
│   │   │   ├── SortedSetInteger.cs
│   │   └── Disposables/
│   │       └── ElapseWriter.cs
│   │
│   ├── Library.Tests/                # 단위 테스트
│   │   ├── Library.Tests.csproj
│   │   ├── BlockedIntegerTests.cs
│   │   ├── SortedSetIntegerTests.cs
│   │   └── TestDatas/
│   │       ├── sorted_int_data_01.csv
│   │       └── sorted_int_data_02.csv
│   │
│   └── SegmentedInteger.sln
│
└── README.md                         # 이 파일 (프로젝트 개요)
```

---

## 빌드 및 테스트

### 단위 테스트

```powershell
cd SegmentedInteger
dotnet test                           # 전체 테스트
dotnet test --filter BlockedInteger   # BlockedInteger만
```

**테스트 포함 사항**:
- 라운드트립 테스트 (encode ↔ decode)
- 경계 케이스 (count=1, max values)
- Overflow wrap 검증
- CSV 파일 통합 테스트

### 의존성

| 패키지 | 버전 | 용도 |
|---|---|---|
| Google.Protobuf | 3.35.0 | Protobuf 직렬화 |
| TUnit | 1.45.29 | 단위 테스트 프레임워크 |
| CsvHelper | 33.1.0 | 테스트 데이터 로드 |
| protoc | 35.0-win64 | Proto 코드 생성 (빌드 시) |

**대상 프레임워크**: .NET 10.0
