# SegmentedInteger

`Int64` 값을 패턴에 따라 Protobuf 가변 인코딩(varint)으로 Serialize하여 용량을 줄이는 C# 라이브러리.  

---

## 클래스 개요

| 클래스 | 입력 조건 | DataType | 복잡도 |
|---|---|---|---|
| `BlockedInteger` | 임의의 `Int64` 집합 (순서 보장, 중복 허용) | `IEnumerable<Int64>` | 높음 |
| `SortedSetInteger` | 중복 없는 정렬된 0을 포함한 양수 `Int64` 집합 | `SortedSet<Int64>` | 낮음 |

> `SortedSetInteger` 이후에 `BlockedInteger`를 개발하여 더 범용적으로 사용할 수 있게 개선하였음.

---

## BlockedInteger

임의의 `Int64` 시퀀스를 패턴 감지 방식으로 다양한 방식으로 Serialize 합니다. (순서를 보장하고 중복을 허용합니다.)

### 블록 타입 (우선순위 순)

| 우선순위 | 타입 | 선택 조건 | 저장 방식 |
|---|---|---|---|
| 1 | **ConstantBlock** | 모든 값 동일, count ≥ 3 | (value, count) |
| 2 | **ArithmeticBlock** | 등차수열, count ≥ 3 | (first, step, count) |
| 3 | **AscendingBitmapBlock** | strictly ascending, range ≤ 63, count ≥ 10 | first + uint64 비트맵 |
| 4 | **AscendingBlock** | 단조증가(비내림차순) | first + uint64 차분 리스트 |
| 5 | **DescendingBitmapBlock** | strictly descending, range ≤ 63, count ≥ 10 | first + uint64 비트맵 |
| 6 | **DescendingBlock** | 단조감소(비오름차순) | first + uint64 차분 리스트 |
| 7 | **DeltaBlock** | range ≤ 16,382 (fallback) | midpoint + sint64 델타 리스트 |

하나의 시퀀스는 여러 블록으로 분할될 수 있습니다. 각 블록은 독립적으로 위 우선순위를 적용합니다.

> **블록당 값 수 상한 (8,192개)**  
> 단일 블록이 수용할 수 있는 최대 값은 8,192개입니다. 시퀀스가 이를 초과하면 자동으로 새 블록을 시작합니다.

> **ConstantBlock·ArithmeticBlock 최소 count (3)**  
> 패턴이 일치해도 count < 3이면 해당 블록을 선택하지 않고 다음 우선순위로 넘어갑니다.  
> 예: `[5, 5]`는 ConstantBlock 조건(`count ≥ 3`)을 만족하지 않아 AscendingBlock으로 처리됩니다.

> **BitmapBlock과 중복값**  
> 비트맵은 각 비트 위치가 값의 존재 여부를 나타내므로, 같은 값을 두 번 표현할 수 없습니다.  
> range ≤ 63, count ≥ 10을 만족하더라도 중복값이 있으면 strictly 조건(`_isStrictlyAscending` / `_isStrictlyDescending`)이 false가 되어  
> `AscendingBitmapBlock` → `AscendingBlock`, `DescendingBitmapBlock` → `DescendingBlock`으로 각각 fallback됩니다.

> **BitmapBlock 임계값 근거 (count ≥ 10)**  
> - AscendingBlock: `tag(1B) + len(1B) + (N−1) × 1B = N+1 bytes`  
> - AscendingBitmapBlock: `tag(1B) + uint64 varint(최대 9B) = 최대 10B`  
> - N ≥ 10일 때 비트맵이 항상 이기거나 동률

> **DeltaBlock 강제 분리 조건**  
> 오름차순·내림차순 단조성이 동시에 깨지고 범위(`max − min`)가 16,382를 초과하면  
> 현재 블록을 종료하고 새 블록을 시작합니다.

### API

```csharp
// 인코딩
BlockedInteger.Encode(ReadOnlySpan<Int64> values, out Pb.BlockedInteger proto);
BlockedInteger.Encode(IEnumerable<Int64> values, out Pb.BlockedInteger proto);

// 디코딩
BlockedInteger.Decode(Pb.BlockedInteger proto, out IReadOnlyList<Int64> integers);
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

## SortedSetInteger

정렬된 비음수 `Int64` 집합을 두 가지 청크 방식으로 압축합니다.

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

## 직렬화 형식

두 클래스 모두 `Library/Protos/default.proto`에서 생성된 protobuf 메시지를 사용합니다.

인코딩은 **deterministic** — 같은 입력은 항상 같은 바이트 시퀀스를 생성합니다.  
블록/청크 타입 결정 로직(우선순위, 분기 조건)을 변경하면 바이트 호환성이 깨집니다.

`Library/ProtoOuts/Default.cs`는 빌드 시 protoc가 자동으로 재생성합니다.

---

## 프로젝트 구조

```
SegmentedInteger/
├── Library/
│   ├── Protos/default.proto          # protobuf 스키마
│   ├── ProtoOuts/Default.cs          # protoc 자동 생성 (빌드 시 갱신, git 제외)
│   ├── SegmentedIntegers/
│   │   ├── SortedSetInteger.cs
│   │   └── BlockedInteger.cs
│   └── Disposables/
│       └── ElapseWriter.cs
└── Library.Tests/
    ├── SortedSetIntegerTests.cs
    ├── BlockedIntegerTests.cs
    └── TestDatas/
        ├── sorted_int_data_01.csv
        └── sorted_int_data_02.csv
```

---

## 빌드 및 테스트

```powershell
dotnet test Library.Tests
```

### 의존성

| 패키지 | 버전 | 용도 |
|---|---|---|
| Google.Protobuf | 3.35.0 | 직렬화 |
| TUnit | 1.45.29 | 테스트 프레임워크 |
| CsvHelper | 33.1.0 | 테스트 데이터 로드 |
| protoc | 35.0-win64 | proto 코드 생성 (빌드 시) |

대상 프레임워크: .NET 10.0
