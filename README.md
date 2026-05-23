# SegmentedInteger

Int64 시퀀스를 Google Protocol Buffers로 압축하는 C# 라이브러리.
**이식성 우선** 설계 — 최적화 트릭 없이 단순한 로직만 사용하여 다른 언어로 동일한 구현이 가능합니다.

---

## 구성

| 클래스 | 입력 조건 | 용도 |
|---|---|---|
| `SegmentedInteger` | 정렬된 비음수 Int64 집합 | 밀집/희소 정수 집합 압축 |
| `BlockedInteger` | 임의의 Int64 시퀀스 | 순서·중복 보존 압축 |

---

## SegmentedInteger

정렬된 비음수 `Int64` 값의 집합을 두 가지 세그먼트 방식으로 압축합니다.

### 세그먼트 타입

| 타입 | 조건 | 저장 방식 |
|---|---|---|
| **Segment64** | 인접 값 간격 < 64 | 시작값 + 증분 비트맵 (최대 8바이트) |
| **Segment2M** | 인접 값 간격 < 2,000,000 | 시작값 + 절대 오프셋 리스트 |

간격이 두 조건을 모두 초과하면 값마다 개별 세그먼트를 생성합니다.

Segment64는 63개 값이 모두 채워지면 `Filled=true`로 비트맵을 생략합니다.

### API

```csharp
// SortedSet 인코딩
SegmentedInteger.Encode(SortedSet<Int64> sorted, out PbSegmented proto);

// ReadOnlySpan 인코딩 (useSortValidation=false 시 호출자가 정렬·비음수 보장)
SegmentedInteger.Encode(ReadOnlySpan<Int64> sorted, out PbSegmented proto,
    bool useSortValidation = true);

// 디코딩
SegmentedInteger.Decode(PbSegmented proto, out SortedSet<Int64> integers);
```

### 사용 예

```csharp
SortedSet<Int64> values = [0, 1, 2, 5, 10_000, 10_001];

SegmentedInteger.Encode(values, out var proto);
// → Segment64(0~5) + Segment2M(10000~10001)

SegmentedInteger.Decode(proto, out var decoded);
// → {0, 1, 2, 5, 10000, 10001}
```

---

## BlockedInteger

임의의 `Int64` 시퀀스를 패턴 감지 방식으로 압축합니다. 순서와 중복을 보존합니다.

### 블록 타입 (우선순위 순)

| 우선순위 | 타입 | 조건 | 저장 방식 |
|---|---|---|---|
| 1 | **ConstantBlock** | 모든 값 동일, count ≥ 3 | (value, count) |
| 2 | **ArithmeticBlock** | 등차수열, count ≥ 3 | (first, step, count) |
| 3 | **AscendingBitmapBlock** | strictly ascending, range ≤ 63, count ≥ 10 | first + uint64 비트맵 |
| 4 | **AscendingBlock** | 단조증가(비내림차순) | first + uint64 차분 리스트 |
| 5 | **DescendingBitmapBlock** | strictly descending, range ≤ 63, count ≥ 10 | first + uint64 비트맵 |
| 6 | **DescendingBlock** | 단조감소(비오름차순) | first + uint64 차분 리스트 |
| 7 | **DeltaBlock** | range ≤ 16,382 | midpoint + sint64 델타 리스트 |

하나의 시퀀스가 여러 블록으로 분할될 수 있습니다. 각 블록은 독립적으로 위 우선순위를 적용합니다.

**BitmapBlock 임계값 근거 (count ≥ 10)**
- AscendingBlock: `tag(1B) + len(1B) + (N-1) × 1B = N+1 bytes`
- AscendingBitmapBlock: `tag(1B) + uint64 varint(최대 9B) = 최대 10B`
- N ≥ 10일 때 비트맵이 항상 이기거나 동률

### API

```csharp
// 인코딩
BlockedInteger.Encode(ReadOnlySpan<Int64> values, out PbBlockedInteger proto);
BlockedInteger.Encode(IEnumerable<Int64> values, out PbBlockedInteger proto);

// 디코딩
BlockedInteger.Decode(PbBlockedInteger proto, out IReadOnlyList<Int64> integers);
```

### 사용 예

```csharp
Int64[] values = [5, 5, 5, 5, 1, 2, 3, 4, 5, 100, 50, 30, 10];

BlockedInteger.Encode(values, out var proto);
// → ConstantBlock(5×4) + ArithmeticBlock(1~5) + DescendingBlock(100,50,30,10)

BlockedInteger.Decode(proto, out var decoded);
// → [5, 5, 5, 5, 1, 2, 3, 4, 5, 100, 50, 30, 10]
```

---

## 직렬화 형식

두 클래스 모두 `default.proto`에서 생성된 protobuf 메시지를 사용합니다.
인코딩은 **deterministic** — 같은 입력은 항상 같은 바이트 시퀀스를 생성합니다.
블록/세그먼트 타입 결정 로직을 변경하면 바이트 호환성이 깨집니다.

protoc는 빌드 시 `Library/Protos/default.proto`에서 `Library/ProtoOuts/Default.cs`를 자동 생성합니다.

---

## 프로젝트 구조

```
SegmentedInteger/
├── Library/
│   ├── Protos/default.proto          # protobuf 스키마
│   ├── ProtoOuts/Default.cs          # protoc 자동 생성 (빌드 시 갱신)
│   ├── SegmentedIntegers/
│   │   ├── SegmentedInteger.cs
│   │   └── BlockedInteger.cs
│   └── Disposables/
│       └── ElapseWriter.cs
└── Library.Tests/
    ├── SegmentedIntegerTests.cs
    ├── BlockedIntegerTests.cs
    └── TestDatas/
        ├── sorted_int_data_01.csv
        └── sorted_int_data_02.csv
```

---

## 빌드 및 테스트

```powershell
# 테스트 실행
dotnet run --project Library.Tests/Library.Tests.csproj
```

### 의존성

| 패키지 | 버전 | 용도 |
|---|---|---|
| Google.Protobuf | 3.35.0 | 직렬화 |
| TUnit | 1.45.29 | 테스트 프레임워크 |
| CsvHelper | 33.1.0 | 테스트 데이터 로드 |
| protoc | 35.0-win64 | proto 코드 생성 (빌드 시) |

대상 프레임워크: .NET 10.0
