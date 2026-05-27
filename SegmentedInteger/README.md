# SegmentedInteger: 시계열 Int64 압축 라이브러리

고성능 패턴 감지 기반 블록 압축으로 Int64 시퀀스를 효율적으로 저장 및 전송합니다.

## 핵심 특징

### 🎯 9가지 블록 타입 자동 선택
```
- ConstantBlock      모든 값 동일 (count ≥ 3)
- ArithmeticBlock    등차 수열 (count ≥ 3)
- AscendingBitmap    strictly ascending, range ≤ 63
- AscendingBlock     단조증가 (≤8191개 diff)
- DescendingBitmap   strictly descending, range ≤ 63
- DescendingBlock    단조감소 (≤8191개 diff)
- DeltaOfDelta       거의 등차 (max|dod| ≤ 31)
- DeltaBlock         range ≤ 16,382
- BitPackedBlock     range > 16,382 (≤1M)
```

### 🚀 System.Runtime.Intrinsics SIMD 최적화
**UnpackBits 슬라이딩 윈도우**
- 64비트 단위로 한 번에 읽기 → 내부 bitWidth 루프 제거
- `BinaryPrimitives.ReadUInt64LittleEndian`으로 안전한 엔디안 처리
- 경계 안전성: safeCount 기반 자동 fallback

**DecodeConstant/DecodeArithmetic 벡터화**
- `Span<long>.Fill()`: 런타임 벡터 store
- `Vector<long>`: AVX2 (4개), ARM NEON (2개), AVX-512 (8개) 자동 선택
- 페이지 기반 디코딩도 동일 최적화 적용

### 📊 검증된 성능
- 모든 테스트 통과: **155/155** ✓
- BenchmarkDotNet 인프라 준비 완료
- 벤치마크 샘플 (Count=64, BitWidth=1): **1.090 μs** (매우 일정함)

## 사용 방법

### 기본 인코딩/디코딩
```csharp
// 인코딩
Int64[] data = { 100, 100, 100, 107, 114, 121, ... };
BlockedInteger.Encode(data, out var proto);
// → protobuf 바이너리로 직렬화

// 디코딩
BlockedInteger.Decode(proto, out var decoded);
// → 원본 순서 및 중복 보존
```

### 페이지 기반 스트리밍
```csharp
Int32 pageCount = BlockedInteger.GetPageCount(proto, pageSize: 1000);

for (Int32 i = 0; i < pageCount; i++)
{
    BlockedInteger.DecodePage(proto, i, 1000, out var page);
    // → 페이지별로 처리
}
```

## 아키텍처

### 블록 선택 전략 (Greedy)
1. **스트리밍**: `BlockAccumulator`가 값을 1개씩 수집
2. **패턴 감지**: 단조성, 등차성, delta-of-delta 등 추적
3. **블록 결정**: 충분한 값이 모이면 최적 타입 선택
4. **백트래킹 없음**: 일부 조정으로 더 나은 압축 가능 (trade-off)

### Protobuf 스키마
```protobuf
message BlockedInteger {
  repeated Block blocks = 1;
}

message Block {
  oneof block_oneof {
    ConstantBlock constant = 1;
    ArithmeticBlock arithmetic = 2;
    AscendingBitmapBlock ascending_bitmap = 3;
    // ... (총 9가지)
  }
}
```

## 성능 벤치마킹

### 벤치마크 실행
```bash
# 전체 벤치마크 (32개 케이스, 20-30분)
dotnet run -c Release --project Library.Benchmarks

# 결과 예시
BitPackedDecodeBenchmarks.DecodeBitPackedOptimized (Count=8192, BitWidth=20)
Mean: ~87.6 ns/op
```

### 포함된 벤치마크
- **BitPackedDecodeBenchmarks**: UnpackBits 최적화 측정
  - Count: 64, 512, 4096, 8192
  - BitWidth: 1, 8, 17, 20

- **LinearDecodeBenchmarks**: 벡터화 성능 측정 (준비 완료)

## 빌드 및 테스트

```bash
# 빌드 (경고 없음)
dotnet build

# 단위 테스트 (155개)
dotnet test

# 포함 사항
- 라운드트립 테스트
- 경계 케이스 (count=1, max values)
- Overflow wrap 검증
- CSV 파일 통합 테스트
```

## 기술 스택

- **언어**: C# 12 (net10.0)
- **Protobuf**: Google.Protobuf 3.35.0
- **최적화**: System.Runtime.Intrinsics, System.Numerics
- **테스트**: TUnit 1.45.29

## 주요 구현 세부사항

### CheckForOverflowUnderflow=true 환경
- 모든 벡터 산술은 `unchecked()` 블록으로 감싸짐
- 스칼라 overflow도 자동 예외 처리

### CollectionsMarshal 버퍼 최적화
- `SetCount` + `AsSpan`으로 List 사전할당
- 불필요한 bounds check 제거
- 메모리 할당 및 GC 압력 감소

### Cross-platform 호환성
- `Vector<T>` 자동 디스패치 (AVX2/NEON/AVX-512)
- BMI2 같은 x86-전용 명령 미사용
- Wire format 호환성 100% 유지

## 라이선스

Internal Library

## 기여

버그 리포트 및 성능 개선 사항은 이슈로 등록해주세요.

---

**마지막 업데이트**: 2026-05-27  
**성능 최적화**: System.Runtime.Intrinsics (SIMD) 적용 완료
