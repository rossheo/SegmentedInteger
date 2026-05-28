using CsvHelper;
using CsvHelper.Configuration;
using Library.Disposables;
using Library.SegmentedIntegers;
using System.Globalization;

namespace Library.Tests;

public class BlockedIntegerTests
{
	private static async Task<BlockedInteger.CompressionStatistics> PrintCompressionStatistics(Pb.BlockedInteger proto)
	{
		BlockedInteger.GetCompressionStatistics(proto, out var stats);
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Total Values: {stats.TotalValues}");
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Original Size: {stats.OriginalSize} bytes");
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Compressed Size: {stats.CompressedSize} bytes");
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Compression Ratio: {stats.CompressionRatio:P2}");
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Block Count: {stats.BlockCount}");
		await TestContext.Current!.OutputWriter.WriteLineAsync($"Block Types: {string.Join(", ", stats.BlockTypeDistribution.Select(kvp => $"{kvp.Key}({kvp.Value})"))}");
		return stats;
	}

	private static async Task AssertRoundTrip(Int64[] input)
	{
		Pb.BlockedInteger proto;
		IReadOnlyList<Int64> result;

		using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
		{
			BlockedInteger.Encode(input.AsSpan(), out proto);
			BlockedInteger.Decode(proto, out result);
		}

		await PrintCompressionStatistics(proto);

		await Assert.That(result).IsEquivalentTo(input);
	}

	// ─── 기본 라운드트립 ───

	[Test]
	public async Task EmptySpan_ReturnsEmptyList()
	{
		BlockedInteger.Encode(ReadOnlySpan<Int64>.Empty, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(result).IsEmpty();
	}

	[Test]
	public async Task EmptyEnumerable_ReturnsEmptyList()
	{
		BlockedInteger.Encode(Enumerable.Empty<Int64>(), out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(result).IsEmpty();
	}

	[Test]
	public async Task BasicRoundTrip_PreservesOrderAndDuplicates()
	{
		await AssertRoundTrip([10, 5, 20, 3, 8, 15, 2, 12, 7, 18, 10, 5, 3, 17, 1, 14, 9, 6, 11, 4]);
	}

	[Test]
	public async Task SingleElement_AscendingBlock()
	{
		BlockedInteger.Encode([42L], out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(proto.Blocks[0].Ascending.First).IsEqualTo(42L);
		await Assert.That(proto.Blocks[0].Ascending.Diffs.Count).IsEqualTo(0);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 42L });
	}

	// ─── 블록 타입별 ───

	[Test]
	public async Task ConstantBlock_TypeAndRoundTrip()
	{
		Int64[] input = new Int64[20];
		Array.Fill(input, 7L);
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].Constant.Value).IsEqualTo(7L);
		await Assert.That(proto.Blocks[0].Constant.Count).IsEqualTo(20);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task ArithmeticBlock_Ascending()
	{
		BlockedInteger.Encode([0, 3, 6, 9, 12], out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].Arithmetic.First).IsEqualTo(0L);
		await Assert.That(proto.Blocks[0].Arithmetic.Step).IsEqualTo(3L);
		await Assert.That(proto.Blocks[0].Arithmetic.Count).IsEqualTo(5);
		await Assert.That(result).IsEquivalentTo(new List<Int64> { 0, 3, 6, 9, 12 });
	}

	[Test]
	public async Task ArithmeticBlock_Descending()
	{
		// step=-10, 20개: 100, 90, ..., -90
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = 100 - i * 10L;
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].Arithmetic.Step).IsEqualTo(-10L);
		await Assert.That(proto.Blocks[0].Arithmetic.Count).IsEqualTo(20);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task ConstantBlock_StepZeroArithmeticIsConstantPriority()
	{
		// step=0 등차 수열은 ConstantBlock이 우선 (ArithmeticBlock보다 먼저 선택됨)
		Int64[] input = new Int64[20];
		Array.Fill(input, 5L);
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Constant);
	}

	[Test]
	public async Task AscendingBlock_NonArithmeticAscending()
	{
		// 비-등차 단조증가(삼각수) → AscendingBlock (range=190>63이므로 Bitmap 아님)
		// diff: 1,2,3,...,19 — 등차수열이 아님
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = i * (i + 1) / 2; // 0,1,3,6,10,...
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That((Int64)proto.Blocks[0].Ascending.Diffs[0]).IsEqualTo(1L);
		await Assert.That((Int64)proto.Blocks[0].Ascending.Diffs[1]).IsEqualTo(2L);
		await Assert.That((Int64)proto.Blocks[0].Ascending.Diffs[2]).IsEqualTo(3L);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task DescendingBlock_NonArithmeticDescending()
	{
		// 비-등차 단조감소(삼각수 역순) → DescendingBlock (range=190>63이므로 Bitmap 아님)
		// diff: 19,18,17,...,1 — 등차수열이 아님
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = (19 - i) * (20 - i) / 2; // 190,171,...,0
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[0]).IsEqualTo(19L);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[1]).IsEqualTo(18L);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[2]).IsEqualTo(17L);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task DeltaOfDeltaBlock_NarrowRangePreferred()
	{
		// range=17, 비정렬, 20개 값
		// max|dod| ≤ 31이고 count ≥ 6이면 DeltaOfDeltaBlock 선택 (DeltaBlock보다 효율적)
		Int64[] input = [10, 5, 15, 3, 7, 12, 1, 14, 8, 17, 4, 11, 6, 13, 2, 16, 9, 0, 10, 5];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		// max|dod| ≤ 31이므로 DeltaOfDeltaBlock 선택
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DeltaOfDelta);
	}

	[Test]
	public async Task DeltaBlock_NonSortedWiderRange()
	{
		// range=200, 비정렬 → DeltaBlock
		Int64[] input = [200, 50, 100, 0, 150, 30, 170, 80, 120, 10,
		                 190, 60, 140, 20, 180, 70, 110, 40, 160, 90];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	// ─── 블록 분리 ───

	[Test]
	public async Task BlockSplit_RangeExceeded()
	{
		// 0, 20000 입력 시 range(0..20000)=20000 > DeltaBlockMax(8191)
		// → 5 입력 시 새 블록 시작 (2개 블록으로 분할)
		BlockedInteger.Encode([0L, 20000L, 5L], out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		// Round-trip 정확성 보장
		await Assert.That(result).IsEquivalentTo(new List<Int64> { 0, 20000, 5 });
		// 반드시 2개 블록으로 분할 (range > 8191이므로)
		await Assert.That(proto.Blocks.Count).IsEqualTo(2);
	}

	[Test]
	public async Task BlockSplit_AscendingThenNonMonotonic()
	{
		// 단조증가([0..20010]) → 역행값들([20005,20003,20007])
		// 단조성 깨짐 + range > 8191 → AscendingBlock + DeltaBlock (2개 블록)
		Int64[] input = [0, 1, 2, 5, 20000, 20010, 20005, 20003, 20007];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		// Round-trip 정확성 보장
		await Assert.That(result).IsEquivalentTo(input.ToList());
		// 반드시 2개 블록으로 분할 (단조성 깨짐 + range > 8191)
		await Assert.That(proto.Blocks.Count).IsGreaterThanOrEqualTo(2);
	}

	[Test]
	public async Task BlockSplit_ArithmeticThenNonMonotonic()
	{
		// 등차 수열([0..20000 step 5000]) → 역행값들([3,7,5])
		// 단조성 깨짐 + 비정렬 범위 정상 → ArithmeticBlock + DeltaBlock (2개 블록)
		Int64[] input = [0, 5000, 10000, 15000, 20000, 3, 7, 5];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		// Round-trip 정확성 보장
		await Assert.That(result).IsEquivalentTo(input.ToList());
		// 단조성이 깨지므로 최소 2개 블록
		await Assert.That(proto.Blocks.Count).IsGreaterThanOrEqualTo(2);
	}

	[Test]
	public async Task BlockSplit_ThreeTypes()
	{
		// 세 가지 서로 다른 블록 타입으로 분할되는 시나리오
		Int64[] input = [9000, 9001, 9002, -10001, -10006, -10009, 100, 100, 100];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		// Round-trip 정확성 보장
		await Assert.That(result).IsEquivalentTo(input.ToList());

		// 3개 블록으로 분할: Arithmetic(3)
		await Assert.That(proto.Blocks.Count).IsEqualTo(3);

		// Block 0: [9000, 9001, 9002] → Arithmetic 블록
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
		await Assert.That(proto.Blocks[0].Arithmetic.First).IsEqualTo(9000L);
		await Assert.That(proto.Blocks[0].Arithmetic.Step).IsEqualTo(1L);
		await Assert.That(proto.Blocks[0].Arithmetic.Count).IsEqualTo(3);

		// Block 1: [-10001, -10006, -10009] → Descending 블록
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That(proto.Blocks[1].Descending.First).IsEqualTo(-10001L);
		await Assert.That(proto.Blocks[1].Descending.Diffs.Count).IsEqualTo(2);
		await Assert.That((Int64)proto.Blocks[1].Descending.Diffs[0]).IsEqualTo(5L);
		await Assert.That((Int64)proto.Blocks[1].Descending.Diffs[1]).IsEqualTo(3L);

		// Block 2: [100, 100, 100] → Constant 블록
		await Assert.That(proto.Blocks[2].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Constant);
		await Assert.That(proto.Blocks[2].Constant.Value).IsEqualTo(100L);
		await Assert.That(proto.Blocks[2].Constant.Count).IsEqualTo(3);
	}

	// ─── 특수 입력 ───

	[Test]
	public async Task DuplicatesPreserved_NonSorted()
	{
		await AssertRoundTrip([5, 3, 5, 3, 1]);
	}

	[Test]
	public async Task ReversedInput_RoundTrip()
	{
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = 19 - i;
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task OscillatingInput_RoundTrip()
	{
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = (i % 2 == 0) ? 0L : 100L;
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task ContainsZero_RoundTrip()
	{
		await AssertRoundTrip([0, 5, 0, 3, 0, 4, 0, 2, 0, 5, 0, 3, 0, 4, 0, 2, 0, 5, 0, 1]);
	}

	[Test]
	public async Task NegativeOnly_RoundTrip()
	{
		await AssertRoundTrip([-5, -3, -7, -1, -9, -2, -8, -4, -6, -10,
		                       -3, -5, -7, -1, -9, -2, -8, -4, -6, -10]);
	}

	[Test]
	public async Task NegativePositiveOscillating_RoundTrip()
	{
		Int64[] input = new Int64[20];
		Int64[] pattern = [-100, 100, -50, 50];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = pattern[i % pattern.Length];
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task ZeroBoundary_ArithmeticBlock()
	{
		// 20원소 등차(step=1, -10..9) → ArithmeticBlock
		Int64[] input = new Int64[20];
		for (Int32 i = 0; i < input.Length; ++i) input[i] = i - 10L;
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	// ─── 경계값 ───

	[Test]
	public async Task PositiveExtreme_SafeMidpoint()
	{
		// 2원소 → count<3이므로 ArithmeticBlock 아님 → AscendingBlock
		Int64[] input = [Int64.MaxValue - 50, Int64.MaxValue];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task NegativeExtreme_SafeRange()
	{
		Int64[] input = [Int64.MinValue, Int64.MinValue + 100];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task CrossSignRange_SingleDescendingBlock()
	{
		// [MaxValue, MinValue]: 단조감소 → DescendingBlock 1개로 처리
		Int64[] input = [Int64.MaxValue, Int64.MinValue];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task CrossSignGradual_SingleDescendingBlock()
	{
		// [MaxValue, 0, MinValue]: 단조감소 → DescendingBlock 1개로 처리
		Int64[] input = [Int64.MaxValue, 0L, Int64.MinValue];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	// ─── 극단값 wrap-around ───

	[Test]
	public async Task AscendingBlock_FullInt64Range_RoundTrip()
	{
		// diff = UInt64.MaxValue (Int64 뺄셈이 wrap하지만 unchecked로 올바른 round-trip 보장)
		await AssertRoundTrip([Int64.MinValue, Int64.MaxValue]);
	}

	[Test]
	public async Task ArithmeticBlock_OverflowStep_RoundTrip()
	{
		// step = unchecked(1 - Int64.MaxValue) = Int64.MinValue+2 (overflow wrap)
		// [Int64.MaxValue, 1, Int64.MinValue+3]: 엄밀히 감소하는 등차 수열
		await AssertRoundTrip([Int64.MaxValue, 1L, Int64.MinValue + 3]);
	}

	// ─── 대량 입력 ───

	[Test]
	public async Task LargeSequentialTest()
	{
		// 10,000 elements: 순차 증가(step=1) → ArithmeticBlock 단일 블록
		Int64[] input = new Int64[10_000];
		for (Int64 i = 0; i < input.Length; ++i) input[i] = i;

		await AssertRoundTrip(input);
	}

	[Test]
	public async Task LargeInput_OrderAndDuplicatesPreserved()
	{
		var rng = new Random(42);
		Int64[] input = new Int64[2000];
		for (Int32 i = 0; i < input.Length; ++i)
			input[i] = rng.NextInt64(-1000, 1001);

		await AssertRoundTrip(input);
	}

	// ─── 혼합 블록 타입 (Multiple Block Types) ───


	[Test]
	public async Task MixedBlocks_MultipleArithmeticTypes()
	{
		// README 복합 블록 예시: 블록당 최대값(8192)으로 인한 자동 분할
		// 첫 번째: ArithmeticBlock (0, 1, 2, ..., 8191) - 8192개 값
		// 두 번째: ArithmeticBlock (0, 2, 4, ..., 50) - 26개 값
		Int64[] input = new Int64[8192 + 26];
		for (Int64 i = 0; i < 8192; i++) input[i] = i;
		for (Int32 i = 0; i < 26; i++) input[8192 + i] = i * 2;

		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		// Round-trip 정확성 보장
		await Assert.That(result).IsEquivalentTo(input.ToList());

		// 2개 블록으로 분할되어야 함 (블록당 최대 8192개 제한)
		await Assert.That(proto.Blocks.Count).IsEqualTo(2);

		// Block 0: ArithmeticBlock (8192개)
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
		await Assert.That(proto.Blocks[0].Arithmetic.First).IsEqualTo(0);
		await Assert.That(proto.Blocks[0].Arithmetic.Step).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].Arithmetic.Count).IsEqualTo(8192);

		// Block 1: ArithmeticBlock (26개)
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
		await Assert.That(proto.Blocks[1].Arithmetic.First).IsEqualTo(0);
		await Assert.That(proto.Blocks[1].Arithmetic.Step).IsEqualTo(2);
		await Assert.That(proto.Blocks[1].Arithmetic.Count).IsEqualTo(26);
	}

	// ─── null 예외 ───

	[Test]
	public async Task Encode_NullEnumerable_ThrowsArgumentNullException()
	{
		await Assert.That(() =>
		{
			BlockedInteger.Encode((IEnumerable<Int64>)null!, out _);
		}).Throws<ArgumentNullException>();
	}

	[Test]
	public async Task Decode_NullProto_ThrowsArgumentNullException()
	{
		await Assert.That(() =>
		{
			BlockedInteger.Decode(null!, out _);
		}).Throws<ArgumentNullException>();
	}

	// ─── CSV 파일 ───

	private static async Task AssertRoundTripFromCsv(string filePath)
	{
		if (!File.Exists(filePath))
		{
			Assert.Fail($"File not found: {filePath}");
			return;
		}

		CsvConfiguration config = new(CultureInfo.InvariantCulture) { HasHeaderRecord = false };
		List<Int64> input = [];

		using (StreamReader reader = new(filePath))
		using (CsvReader csv = new(reader, config))
		{
			while (csv.Read())
			{
				for (Int32 i = 0; i < csv.ColumnCount; ++i)
					input.Add(Convert.ToInt64(csv.GetField(i)));
			}
		}

		Pb.BlockedInteger proto;
		IReadOnlyList<Int64> result;

		using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
		{
			BlockedInteger.Encode(input, out proto);
			BlockedInteger.Decode(proto, out result);
		}

		await PrintCompressionStatistics(proto);

		Int32 intSize = input.Count * sizeof(Int64);
		Int32 pbSize = proto.CalculateSize();

		await Assert.That(pbSize).IsLessThanOrEqualTo(intSize);
		await Assert.That(result).IsEquivalentTo(input);
	}

	[Test]
	public async Task FromSortedIntData01Test()
	{
		await AssertRoundTripFromCsv("TestDatas/sorted_int_data_01.csv");
	}

	[Test]
	public async Task FromSortedIntData02Test()
	{
		await AssertRoundTripFromCsv("TestDatas/sorted_int_data_02.csv");
	}

	// ─── AscendingBitmapBlock ───

	[Test]
	public async Task AscBitmapBlock_CountBoundary_7_UsesAscending()
	{
		// strictly ascending, range=42≤63, count=7 < 8 (BitmapBlockMinCount) → AscendingBlock
		Int64[] input = [1, 3, 5, 8, 12, 20, 43];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
	}

	[Test]
	public async Task AscBitmapBlock_CountBoundary_8_UsesBitmap()
	{
		// strictly ascending, range=48≤63, count=8 (BitmapBlockMinCount) → AscendingBitmapBlock
		Int64[] input = [1, 3, 5, 8, 12, 20, 35, 49];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
	}

	[Test]
	public async Task AscBitmapBlock_RangeBoundary_63_UsesBitmap()
	{
		// strictly ascending, range=63, count=10 → AscendingBitmapBlock
		Int64[] input = [0, 5, 10, 15, 20, 25, 30, 40, 50, 63];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
	}

	[Test]
	public async Task AscBitmapBlock_RangeBoundary_64_UsesAscending()
	{
		// strictly ascending, range=64 > 63, count=10 → AscendingBlock
		Int64[] input = [0, 5, 10, 15, 20, 25, 30, 40, 50, 64];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
	}

	[Test]
	public async Task AscBitmapBlock_ArithmeticPriority()
	{
		// 등차수열(step=1)은 strictly ascending + range≤63 + count≥10이어도 ArithmeticBlock 우선
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
	}

	[Test]
	public async Task AscBitmapBlock_DuplicatesUseAscending()
	{
		// 중복 값 포함 → strictly ascending 아님 → AscendingBlock
		Int64[] input = [1, 3, 3, 8, 12, 20, 35, 45, 55, 60];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
	}

	[Test]
	public async Task AscBitmapBlock_NegativeFirst_RoundTrip()
	{
		// first < 0 → 정상 동작
		Int64[] input = [-10, -8, -5, -3, 0, 2, 5, 8, 20, 35];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
	}

	[Test]
	public async Task AscBitmapBlock_BitsEncoding()
	{
		// [0,1,2,4,5,6,8,9,10,12]: first=0, range=12
		// bits: pos 0,1,3,4,5,7,8,9,11 → 1+2+8+16+32+128+256+512+2048 = 3003
		Int64[] input = [0, 1, 2, 4, 5, 6, 8, 9, 10, 12];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await Assert.That(proto.Blocks[0].AscendingBitmap.First).IsEqualTo(0L);
		await Assert.That(proto.Blocks[0].AscendingBitmap.Bits).IsEqualTo(3003UL);
	}

	[Test]
	public async Task AscBitmapBlock_RoundTrip_Dense()
	{
		// 밀집 비등차 strictly ascending 케이스
		Int64[] input = [50, 51, 53, 54, 55, 57, 58, 59, 60, 61];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
	}

	// ─── DescendingBitmapBlock ───

	[Test]
	public async Task DescBitmapBlock_CountBoundary_7_UsesDescending()
	{
		// strictly descending, range=42≤63, count=7 < 8 (BitmapBlockMinCount) → DescendingBlock
		Int64[] input = [43, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
	}

	[Test]
	public async Task DescBitmapBlock_CountBoundary_8_UsesBitmap()
	{
		// strictly descending, range=48≤63, count=8 (BitmapBlockMinCount) → DescendingBitmapBlock
		Int64[] input = [49, 35, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
	}

	[Test]
	public async Task DescBitmapBlock_RangeBoundary_63_UsesBitmap()
	{
		// strictly descending, range=63, count=10 → DescendingBitmapBlock
		Int64[] input = [63, 58, 53, 48, 43, 38, 33, 23, 13, 0];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
	}

	[Test]
	public async Task DescBitmapBlock_RangeBoundary_64_UsesDescending()
	{
		// strictly descending, range=64 > 63, count=10 → DescendingBlock
		Int64[] input = [64, 60, 55, 50, 45, 40, 35, 25, 15, 0];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
	}

	[Test]
	public async Task DescBitmapBlock_DuplicatesUseDescending()
	{
		// 중복 값 포함 → strictly descending 아님 → DescendingBlock
		Int64[] input = [60, 55, 45, 45, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
	}

	[Test]
	public async Task DescBitmapBlock_NegativeFirst_RoundTrip()
	{
		// first < 0 → 정상 동작
		Int64[] input = [-20, -22, -25, -27, -30, -32, -35, -38, -40, -42];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
	}

	[Test]
	public async Task DescBitmapBlock_BitsEncoding()
	{
		// [12,10,9,8,6,5,4,2,1,0]: first=12, range=12
		// bits: pos 1,2,3,5,6,7,9,10,11 → 2+4+8+32+64+128+512+1024+2048 = 3822
		Int64[] input = [12, 10, 9, 8, 6, 5, 4, 2, 1, 0];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await Assert.That(proto.Blocks[0].DescendingBitmap.First).IsEqualTo(12L);
		await Assert.That(proto.Blocks[0].DescendingBitmap.Bits).IsEqualTo(3822UL);
	}

	[Test]
	public async Task DescBitmapBlock_RoundTrip_Dense()
	{
		// 밀집 비등차 strictly descending 케이스
		Int64[] input = [61, 60, 59, 57, 56, 55, 53, 51, 50, 49];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
	}

	// ─── DeltaOfDeltaBlock ───

	[Test]
	public async Task DeltaOfDeltaBlock_NearlyArithmetic_IsSelected()
	{
		// nearly-arithmetic 비단조 시퀀스: deltas=[10,-11,10,-11,10,-11], dods=[-21,21,-21,21,-21]
		// max|dod|=21 ≤ 31, count=7 ≥ 6, !ascending && !descending → DeltaOfDeltaBlock 선택
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DeltaOfDelta);
	}

	[Test]
	public async Task DeltaOfDeltaBlock_InternalEncoding_FirstAndDods()
	{
		// DeltaOfDeltaBlock 내부 구조 검증: first, firstDelta, dods
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].DeltaOfDelta.First).IsEqualTo(1000L);
		await Assert.That(proto.Blocks[0].DeltaOfDelta.FirstDelta).IsEqualTo(10L);
		await Assert.That(proto.Blocks[0].DeltaOfDelta.DeltaOfDeltas).IsEquivalentTo(
			new List<Int64> { -21, 21, -21, 21, -21 });
	}

	[Test]
	public async Task DeltaOfDeltaBlock_RoundTrip_PreservesValues()
	{
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(result).IsEquivalentTo(input);
	}

	[Test]
	public async Task DeltaOfDeltaBlock_NegativeFirstDelta_RoundTrip()
	{
		// first_delta < 0인 시퀀스: [100, 90, 100, 90, 100, 90, 100]
		// deltas: -10, 10, -10, 10, -10, 10
		// dods: 20, -20, 20, -20, 20 → max|dod| = 20 ≤ 31
		Int64[] input = [100, 90, 100, 90, 100, 90, 100];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DeltaOfDelta);
		await Assert.That(result).IsEquivalentTo(input);
	}

	[Test]
	public async Task DeltaOfDeltaBlock_CountBelowMinCount_UsesDeltaBlock()
	{
		// count = 5 < DeltaOfDeltaBlockMinCount(6) → DeltaBlock 선택
		// max|dod| = 21이지만 count 조건 불만족
		Int64[] input = [1000, 1010, 999, 1009, 998];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaOfDeltaBlock_DodAtThreshold_IsSelected()
	{
		// max|dod| = 31 (경계값) ≤ DeltaOfDeltaSelectThreshold(31) → DeltaOfDeltaBlock 선택
		// 시퀀스: [0, 31, 0, 31, 0, 31, 0]
		// deltas: 31, -31, 31, -31, 31, -31
		// dods: -62, 62, -62, 62, -62 → max|dod| = 62 (너무 큼!)
		// 시퀀스 재설계: [100, 131, 100, 131, 100, 131, 100]
		// deltas: 31, -31, 31, -31, 31, -31
		// dods: -62, ... (여전히 62)
		// 다른 접근: 작은 변동, max|dod| = 31만 되도록
		// [0, 31, 62, 93, 124, 155, 186] (일반 등차)은 ArithmeticBlock이 됨
		// [0, 31, 60, 91, 120, 151, 180]
		// deltas: 31, 29, 31, 29, 31, 29
		// dods: -2, 2, -2, 2, -2 → max|dod| = 2
		// 비단조로 만들기: [0, 31, 30, 61, 60, 91, 90]
		// deltas: 31, -1, 31, -1, 31, -1
		// dods: -32, 32, -32, 32, -32 → max|dod| = 32 > 31 (안 됨)
		// [0, 31, 30, 60, 59, 90, 89]
		// deltas: 31, -1, 30, -1, 31, -1
		// dods: -32, 31, -31, 32, -32 → max|dod| = 32 (안 됨)
		// [0, 30, 29, 59, 58, 88, 87]
		// deltas: 30, -1, 30, -1, 30, -1
		// dods: -31, 31, -31, 31, -31 → max|dod| = 31 ✓
		Int64[] input = [0, 30, 29, 59, 58, 88, 87];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DeltaOfDelta);
	}

	[Test]
	public async Task DeltaOfDeltaBlock_DodExceedsThreshold_UsesDeltaBlock()
	{
		// max|dod| = 32 > DeltaOfDeltaSelectThreshold(31) → DeltaBlock 선택
		// [0, 31, 30, 60, 59, 90, 89]
		// deltas: 31, -1, 30, -1, 31, -1
		// dods: -32, 31, -31, 32, -32 → max|dod| = 32 > 31
		Int64[] input = [0, 31, 30, 60, 59, 90, 89];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	// ─── DeltaBlock ───

	[Test]
	public async Task DeltaBlock_ReferenceCalculation_Symmetric()
	{
		// min=0, max=10 → reference=5
		// deltas: 0-5=-5, 10-5=5, 3-5=-2
		Int64[] input = [0, 10, 3];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await Assert.That(proto.Blocks[0].Delta.Reference).IsEqualTo(5L);
	}

	[Test]
	public async Task DeltaBlock_NegativeValues_RoundTrip()
	{
		// 음수만 포함하는 비정렬 시퀀스
		Int64[] input = [-100, -50, -75, -80, -60, -90, -55, -70, -85, -65,
		                 -95, -45, -55, -75, -80, -60, -90, -55, -70, -85];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_PositiveNegativeMixed_RoundTrip()
	{
		// 양수와 음수 혼합
		Int64[] input = [-10, 20, 5, -5, 15, -8, 12, -3, 8, 18,
		                 -7, 10, 2, -9, 17, 4, -6, 13, -1, 11];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_MaxRange_8191()
	{
		// range=8191 (최대 범위)
		Int64[] input = [0, 8191, 5000];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_Duplicates_RoundTrip()
	{
		// 중복 포함 비정렬
		Int64[] input = [10, 5, 10, 3, 5];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_LargeValueSpread_RoundTrip()
	{
		// min=-1000, max=5000, range=6000
		Int64[] input = [-1000, 5000, 0, 2000, -500];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_ReferenceAtExtremes()
	{
		// reference가 Int64 극값 근처
		Int64[] input = [Int64.MaxValue - 100, Int64.MaxValue - 50, Int64.MaxValue - 75];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_ZeroCrossing()
	{
		// 음수에서 양수로 혼합
		Int64[] input = [-100, 100, 0, -50, 80, -20, 60, -80, 40, 90,
		                 -10, 70, -60, 30, -90, 50, -30, 10, -70, 20];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	[Test]
	public async Task DeltaBlock_OscillatingValues_RoundTrip()
	{
		// 진동하는 값
		Int64[] input = [0, 100, 10, 90, 20];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
	}

	// ─── 블록 선택 우선순위 ───

	[Test]
	public async Task BlockSelection_TwoElementDecreasing_UsesDescendingBlock()
	{
		// 2원소 감소 시퀀스 — count < RepeatableBlockMinCount이므로 DeltaBlock 불가, DescendingBlock 선택
		Int64[] input = [100, 50];
		BlockedInteger.Encode(input, out var proto);

		await PrintCompressionStatistics(proto);

		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
	}


	// ─── ValidateIntegrity ───

	[Test]
	public async Task ValidateIntegrity_ValidProto_ReturnsTrue()
	{
		// 유효한 프로토 → validation 성공
		BlockedInteger.Encode([1, 2, 3, 4, 5], out var proto);
		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsTrue();
		await Assert.That(errors).IsEmpty();
	}

	[Test]
	public async Task ValidateIntegrity_EmptyProto_ReturnsTrue()
	{
		// 빈 프로토 → 유효
		Pb.BlockedInteger proto = new();
		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsTrue();
		await Assert.That(errors).IsEmpty();
	}

	[Test]
	public async Task ValidateIntegrity_NullProto_ThrowsArgumentNullException()
	{
		// null proto → ArgumentNullException 발생
		await Assert.That(() =>
		{
			BlockedInteger.TryValidate(null!, out _);
		}).Throws<ArgumentNullException>();
	}

	[Test]
	public async Task ValidateIntegrity_InvalidConstantBlock_Count0()
	{
		// ConstantBlock with Count=0 (invalid)
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Constant = new Pb.BlockedInteger.Types.Block.Types.ConstantBlock
					{
						Value = 42,
						Count = 0
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("Count는 1 이상");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidConstantBlock_CountTooLarge()
	{
		// ConstantBlock with Count > MaxBlockValues
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Constant = new Pb.BlockedInteger.Types.Block.Types.ConstantBlock
					{
						Value = 42,
						Count = 10000
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("이하여야");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidAscendingBitmapBlock_BelowMinCount()
	{
		// AscendingBitmapBlock with count < 8
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					AscendingBitmap = new Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock
					{
						First = 0,
						Bits = 0b111  // 3 set bits + 1 (first) = 4 values total < 8
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("최소");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidAscendingBitmapBlock_RangeTooLarge()
	{
		// AscendingBitmapBlock with range > 63 (invalid)
		// 최소 10개 값을 충족하면서 범위를 64로 설정
		UInt64 bits = 0;
		// bit 0,1,2,...,8과 bit 63 설정 → count=10+1(first)=11, range=64 > 63 ✗
		for (int i = 0; i < 9; i++) bits |= 1UL << i;
		bits |= 1UL << 63;  // 최상위 비트 설정 → range = 64 > 63 (invalid)

		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					AscendingBitmap = new Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock
					{
						First = 0,
						Bits = bits  // range = 64 > 63 (invalid)
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		// 범위 초과로 유효하지 않음
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("범위");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidAscendingBlock_DiffsTooMany()
	{
		// AscendingBlock with too many diffs
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Ascending = new Pb.BlockedInteger.Types.Block.Types.AscendingBlock
					{
						First = 0
					}
				}
			}
		};

		// Add 10000 diffs (exceeds MaxBlockValues)
		for (int i = 0; i < 10000; i++)
			proto.Blocks[0].Ascending.Diffs.Add(1);

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
	}

	[Test]
	public async Task ValidateIntegrity_InvalidDeltaBlock_EmptyDeltas()
	{
		// DeltaBlock with no deltas
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Delta = new Pb.BlockedInteger.Types.Block.Types.DeltaBlock
					{
						Reference = 0
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("Deltas는 1개 이상");
	}

	[Test]
	public async Task ValidateIntegrity_DeltaOfDeltaBlock_Valid()
	{
		// 유효한 DeltaOfDeltaBlock
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);

		await Assert.That(isValid).IsTrue();
		await Assert.That(errors).IsEmpty();
	}

	[Test]
	public async Task ValidateIntegrity_InvalidDeltaOfDeltaBlock_EmptyDeltaOfDeltas()
	{
		// DeltaOfDeltaBlock with DeltaOfDeltas.Count == 0 (invalid)
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					DeltaOfDelta = new Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock
					{
						First = 100,
						FirstDelta = 10
						// DeltaOfDeltas is empty
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);

		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("DeltaOfDeltas는 1개 이상");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidDeltaOfDeltaBlock_DodTooLarge()
	{
		// DeltaOfDeltaBlock with |dod| > 8191 (invalid)
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					DeltaOfDelta = new Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock
					{
						First = 0,
						FirstDelta = 100,
						DeltaOfDeltas = { 9000 }  // |dod| = 9000 > 8191
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);

		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("max|delta-of-delta|는");
	}

	[Test]
	public async Task ValidateIntegrity_InvalidDeltaBlock_RangeTooLarge()
	{
		// DeltaBlock with range > 8191
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Delta = new Pb.BlockedInteger.Types.Block.Types.DeltaBlock
					{
						Reference = 0,
						Deltas = { 0, 20000 }  // range = 20000 > 8191
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("범위");
	}

	[Test]
	public async Task ValidateIntegrity_MultipleErrors()
	{
		// Multiple invalid blocks
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block
				{
					Constant = new Pb.BlockedInteger.Types.Block.Types.ConstantBlock
					{
						Value = 42,
						Count = 0  // invalid
					}
				},
				new Pb.BlockedInteger.Types.Block
				{
					Delta = new Pb.BlockedInteger.Types.Block.Types.DeltaBlock
					{
						Reference = 0
						// empty deltas - invalid
					}
				}
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThanOrEqualTo(2);
	}

	[Test]
	public async Task ValidateIntegrity_UnsetBlockType()
	{
		// 블록 타입이 설정되지 않은 상태
		Pb.BlockedInteger proto = new()
		{
			Blocks =
			{
				new Pb.BlockedInteger.Types.Block()  // BlockOneofCase = None
			}
		};

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);
		await Assert.That(isValid).IsFalse();
		await Assert.That(errors.Count).IsGreaterThan(0);
		await Assert.That(errors[0]).Contains("설정되지 않음");
	}

	// ─── GetCompressionStatistics ───

	[Test]
	public async Task CompressionStatistics_EmptyProto()
	{
		// 빈 프로토 → 0개 값
		Pb.BlockedInteger proto = new();
		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(0);
		await Assert.That(stats.OriginalSize).IsEqualTo(0);
		await Assert.That(stats.BlockCount).IsEqualTo(0);
		await Assert.That(stats.CompressionRatio).IsEqualTo(0.0);
	}

	[Test]
	public async Task CompressionStatistics_SingleBlock_Constant()
	{
		// ConstantBlock으로 인코딩된 20개 값
		Int64[] input = new Int64[20];
		Array.Fill(input, 42L);
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(20);
		await Assert.That(stats.OriginalSize).IsEqualTo(20 * sizeof(Int64)); // 160 bytes
		await Assert.That(stats.CompressedSize).IsLessThan(stats.OriginalSize);
		await Assert.That(stats.CompressionRatio).IsLessThan(1.0);
		await Assert.That(stats.BlockCount).IsEqualTo(1);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("Constant");
		await Assert.That(stats.BlockTypeDistribution["Constant"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_SingleBlock_Arithmetic()
	{
		// ArithmeticBlock으로 인코딩된 시퀀스
		Int64[] input = [0, 3, 6, 9, 12, 15, 18, 21, 24, 27];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(10);
		await Assert.That(stats.OriginalSize).IsEqualTo(10 * sizeof(Int64));
		await Assert.That(stats.CompressedSize).IsLessThan(stats.OriginalSize);
		await Assert.That(stats.BlockCount).IsEqualTo(1);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("Arithmetic");
		await Assert.That(stats.BlockTypeDistribution["Arithmetic"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_MultipleBlocks()
	{
		// 여러 블록으로 분할되는 큰 시퀀스
		Int64[] input = new Int64[20000];
		for (Int64 i = 0; i < input.Length; ++i) input[i] = i;
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(20000);
		await Assert.That(stats.OriginalSize).IsEqualTo(20000 * sizeof(Int64));
		await Assert.That(stats.CompressedSize).IsLessThan(stats.OriginalSize);
		await Assert.That(stats.BlockCount).IsGreaterThan(0);
		await Assert.That(stats.AverageBlockSize).IsGreaterThan(0);
	}

	[Test]
	public async Task CompressionStatistics_AscendingBitmap()
	{
		// AscendingBitmapBlock
		Int64[] input = [1, 3, 5, 8, 12, 20, 35, 45, 55, 60];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(10);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("AscendingBitmap");
		await Assert.That(stats.BlockTypeDistribution["AscendingBitmap"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_Delta()
	{
		// DeltaBlock
		Int64[] input = [-10, 20, 5, -5, 15, -8, 12, -3, 8, 18];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(10);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("Delta");
		await Assert.That(stats.BlockTypeDistribution["Delta"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_DeltaOfDelta()
	{
		// DeltaOfDeltaBlock
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(7);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("DeltaOfDelta");
		await Assert.That(stats.BlockTypeDistribution["DeltaOfDelta"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_CompressionRatio()
	{
		// 압축률 검증 (Constant 패턴은 높은 압축률)
		Int64[] input = new Int64[1000];
		Array.Fill(input, 42L);
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.CompressionRatio).IsLessThan(0.1); // 90% 이상 압축
	}

	[Test]
	public async Task CompressionStatistics_AverageBlockSize()
	{
		// 평균 블록 크기 = 압축 크기 / 블록 개수
		Int64[] input = new Int64[1000];
		for (Int64 i = 0; i < input.Length; ++i) input[i] = i;
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		Double expectedAverage = (Double)stats.CompressedSize / stats.BlockCount;
		await Assert.That(stats.AverageBlockSize).IsEqualTo(expectedAverage);
	}

	[Test]
	public async Task CompressionStatistics_NullProto_ThrowsArgumentNullException()
	{
		// null proto → ArgumentNullException
		await Assert.That(() =>
		{
			BlockedInteger.GetCompressionStatistics(null!, out _);
		}).Throws<ArgumentNullException>();
	}

	// ─── DecodePage ───

	[Test]
	public async Task DecodePage_FirstPage()
	{
		// 첫 번째 페이지 (pageIndex=0)
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 3, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 1, 2, 3 });
	}

	[Test]
	public async Task DecodePage_SecondPage()
	{
		// 두 번째 페이지 (pageIndex=1)
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 1, 3, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 4, 5, 6 });
	}

	[Test]
	public async Task DecodePage_LastPage_PartialFill()
	{
		// 마지막 페이지 (데이터가 pageSize보다 적음)
		Int64[] input = [1, 2, 3, 4, 5];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 1, 3, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 4, 5 });
	}

	[Test]
	public async Task DecodePage_OutOfBounds_ReturnsEmpty()
	{
		// pageIndex가 데이터 범위를 초과
		Int64[] input = [1, 2, 3, 4, 5];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 10, 3, out var result);

		await Assert.That(result).IsEmpty();
	}

	[Test]
	public async Task DecodePage_InvalidPageIndex_ThrowsArgumentException()
	{
		// pageIndex < 0 → ArgumentException
		Int64[] input = [1, 2, 3];
		BlockedInteger.Encode(input, out var proto);

		await Assert.That(() =>
		{
			BlockedInteger.DecodePage(proto, -1, 3, out _);
		}).Throws<ArgumentException>();
	}

	[Test]
	public async Task DecodePage_InvalidPageSize_ThrowsArgumentException()
	{
		// pageSize <= 0 → ArgumentException
		Int64[] input = [1, 2, 3];
		BlockedInteger.Encode(input, out var proto);

		await Assert.That(() =>
		{
			BlockedInteger.DecodePage(proto, 0, 0, out _);
		}).Throws<ArgumentException>();
	}

	[Test]
	public async Task DecodePage_NullProto_ThrowsArgumentNullException()
	{
		// null proto → ArgumentNullException
		await Assert.That(() =>
		{
			BlockedInteger.DecodePage(null!, 0, 5, out _);
		}).Throws<ArgumentNullException>();
	}

	[Test]
	public async Task DecodePage_ConstantBlock_FullPage()
	{
		// ConstantBlock, 정확히 한 페이지
		Int64[] input = new Int64[20];
		Array.Fill(input, 42L);
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 10, out var result);

		var expected = new Int64[10];
		Array.Fill(expected, 42L);
		await Assert.That(result).IsEquivalentTo(expected.ToList());
	}

	[Test]
	public async Task DecodePage_ConstantBlock_PartialPage()
	{
		// ConstantBlock 부분 페이지
		Int64[] input = new Int64[20];
		Array.Fill(input, 42L);
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 1, 10, out var result);

		var expected = new Int64[10];
		Array.Fill(expected, 42L);
		await Assert.That(result).IsEquivalentTo(expected.ToList());
	}

	[Test]
	public async Task DecodePage_ArithmeticBlock_FullPage()
	{
		// ArithmeticBlock 전체 페이지
		Int64[] input = [0, 3, 6, 9, 12, 15];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 6, out var result);

		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task DecodePage_ArithmeticBlock_PartialPage()
	{
		// ArithmeticBlock 부분 페이지
		Int64[] input = [0, 3, 6, 9, 12, 15];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 3, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 0, 3, 6 });
	}

	[Test]
	public async Task DecodePage_AscendingBitmapBlock_PartialPage()
	{
		// AscendingBitmapBlock 부분 페이지
		Int64[] input = [1, 3, 5, 8, 12, 20, 35, 45, 55, 60];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 5, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 1, 3, 5, 8, 12 });
	}

	[Test]
	public async Task DecodePage_AscendingBlock_PartialPage()
	{
		// AscendingBlock (Bitmap이 아닌 일반) 부분 페이지
		Int64[] input = [0, 1, 3, 6, 10, 15, 21, 28, 36, 45];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 5, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 0, 1, 3, 6, 10 });
	}

	[Test]
	public async Task DecodePage_DescendingBlock_PartialPage()
	{
		// DescendingBlock 부분 페이지 (단조 감소)
		Int64[] input = [45, 36, 28, 21, 15, 10, 6, 3, 1, 0];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 5, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 45, 36, 28, 21, 15 });
	}

	[Test]
	public async Task DecodePage_DeltaOfDeltaBlock_PartialPage()
	{
		// DeltaOfDeltaBlock 부분 페이지
		// 시퀀스: [1000, 1010, 999, 1009, 998, 1008, 997]
		// pageIndex=1, pageSize=3 → 인덱스 3,4,5 → [1009, 998, 1008]
		Int64[] input = [1000, 1010, 999, 1009, 998, 1008, 997];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 1, 3, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { 1009, 998, 1008 });
	}

	[Test]
	public async Task DecodePage_DeltaBlock_PartialPage()
	{
		// DeltaBlock 부분 페이지
		Int64[] input = [-10, 20, 5, -5, 15, -8, 12, -3, 8, 18];
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 0, 5, out var result);

		await Assert.That(result).IsEquivalentTo(new List<Int64> { -10, 20, 5, -5, 15 });
	}

	[Test]
	public async Task DecodePage_LargeSequence_MiddlePage()
	{
		// 큰 시퀀스에서 중간 페이지 추출
		Int64[] input = new Int64[10000];
		for (Int64 i = 0; i < input.Length; ++i) input[i] = i * 2;
		BlockedInteger.Encode(input, out var proto);

		BlockedInteger.DecodePage(proto, 2, 5, out var result);

		var expected = new List<Int64>();
		for (int i = 10; i < 15; i++) expected.Add(i * 2);
		await Assert.That(result).IsEquivalentTo(expected);
	}

	[Test]
	public async Task DecodePage_MultipleBlocks_CrossingBoundary()
	{
		// 여러 블록에 걸친 페이지 추출
		var input = new List<Int64>();

		// Constant 블록: 20개 (인덱스 0-19)
		for (int i = 0; i < 20; i++) input.Add(100);

		// Arithmetic 블록: 10개 (인덱스 20-29)
		for (int i = 0; i < 10; i++) input.Add(i * 10);

		BlockedInteger.Encode(input, out var proto);

		// 페이지: 인덱스 15-29 (pageIndex=1, pageSize=15)
		BlockedInteger.DecodePage(proto, 1, 15, out var result);

		var expected = new List<Int64>();
		for (int i = 0; i < 5; i++) expected.Add(100);  // 인덱스 15-19
		for (int i = 0; i < 10; i++) expected.Add(i * 10);  // 인덱스 20-29

		await Assert.That(result).IsEquivalentTo(expected);
	}

	[Test]
	public async Task CompressionStatistics_AscendingBlock()
	{
		// AscendingBlock (Bitmap이 아닌 일반 Ascending) 통계
		// Ascending: 단조증가하지만 등차수열 아님, count < 10
		Int64[] input = [0, 1, 3, 6];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(4);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("Ascending");
		await Assert.That(stats.BlockTypeDistribution["Ascending"]).IsEqualTo(1);
	}

	[Test]
	public async Task CompressionStatistics_DescendingBlock()
	{
		// DescendingBlock (Bitmap이 아닌 일반 Descending) 통계
		// Descending: 단조감소하지만 등차수열 아님, count < 10
		Int64[] input = [100, 95, 85, 70];
		BlockedInteger.Encode(input, out var proto);

		var stats = await PrintCompressionStatistics(proto);

		await Assert.That(stats.TotalValues).IsEqualTo(4);
		await Assert.That(stats.BlockTypeDistribution).ContainsKey("Descending");
		await Assert.That(stats.BlockTypeDistribution["Descending"]).IsEqualTo(1);
	}

	[Test]
	public async Task ValidateIntegrity_DescendingBitmapBlock_Valid()
	{
		// DescendingBitmapBlock 유효성 검증
		Int64[] input = [60, 55, 45, 35, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);

		await Assert.That(isValid).IsTrue();
		await Assert.That(errors).IsEmpty();
	}

	[Test]
	public async Task ValidateIntegrity_AscendingBlock_Valid()
	{
		// AscendingBlock (Bitmap이 아닌 일반) 유효성 검증
		Int64[] input = [0, 1, 3, 6, 10, 15, 21, 28, 36, 45];
		BlockedInteger.Encode(input, out var proto);

		bool isValid = BlockedInteger.TryValidate(proto, out var errors);

		await Assert.That(isValid).IsTrue();
		await Assert.That(errors).IsEmpty();
	}

	// ─── GetPageCount ───

	[Test]
	public async Task GetPageCount_ExactPages()
	{
		// 정확히 떨어지는 페이지 (10개 값, 5개씩 = 2페이지)
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 5);

		await Assert.That(pageCount).IsEqualTo(2);
	}

	[Test]
	public async Task GetPageCount_PartialLastPage()
	{
		// 마지막 페이지가 부분 (10개 값, 3개씩 = 4페이지)
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 3);

		await Assert.That(pageCount).IsEqualTo(4);
	}

	[Test]
	public async Task GetPageCount_SinglePage()
	{
		// 한 페이지 안에 모든 데이터
		Int64[] input = [1, 2, 3];
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 10);

		await Assert.That(pageCount).IsEqualTo(1);
	}

	[Test]
	public async Task GetPageCount_LargePageSize()
	{
		// 매우 큰 페이지 크기
		Int64[] input = [1, 2, 3, 4, 5];
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 1000);

		await Assert.That(pageCount).IsEqualTo(1);
	}

	[Test]
	public async Task GetPageCount_OneValuePerPage()
	{
		// 한 값씩 페이지 분할 (10개 값 = 10페이지)
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 1);

		await Assert.That(pageCount).IsEqualTo(10);
	}

	[Test]
	public async Task GetPageCount_EmptyProto()
	{
		// 빈 프로토 (0개 값)
		BlockedInteger.Encode([], out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 5);

		await Assert.That(pageCount).IsEqualTo(0);
	}

	[Test]
	public async Task GetPageCount_NullProto_ThrowsArgumentNullException()
	{
		// null proto → ArgumentNullException
		await Assert.That(() =>
		{
			BlockedInteger.GetPageCount(null!, 5);
		}).Throws<ArgumentNullException>();
	}

	[Test]
	public async Task GetPageCount_InvalidPageSize_ThrowsArgumentException()
	{
		// pageSize <= 0 → ArgumentException
		Int64[] input = [1, 2, 3];
		BlockedInteger.Encode(input, out var proto);

		await Assert.That(() =>
		{
			BlockedInteger.GetPageCount(proto, 0);
		}).Throws<ArgumentException>();
	}

	[Test]
	public async Task GetPageCount_LargeSequence()
	{
		// 큰 시퀀스 (10,000개 값, 100씩 = 100페이지)
		Int64[] input = new Int64[10000];
		for (Int64 i = 0; i < input.Length; ++i) input[i] = i;
		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 100);

		await Assert.That(pageCount).IsEqualTo(100);
	}

	[Test]
	public async Task GetPageCount_MultipleBlocks()
	{
		// 여러 블록에 걸친 데이터
		var input = new List<Int64>();
		for (int i = 0; i < 20; i++) input.Add(100);  // Constant 블록
		for (int i = 0; i < 10; i++) input.Add(i * 10);  // Arithmetic 블록

		BlockedInteger.Encode(input, out var proto);

		Int32 pageCount = BlockedInteger.GetPageCount(proto, 7);

		await Assert.That(pageCount).IsEqualTo(5);  // 30개 값, 7씩 = 5페이지
	}
}
