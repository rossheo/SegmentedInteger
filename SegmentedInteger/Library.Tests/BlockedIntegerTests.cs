using CsvHelper;
using CsvHelper.Configuration;
using Library.Disposables;
using Library.SegmentedIntegers;
using System.Globalization;

namespace Library.Tests;

public class BlockedIntegerTests
{
	private static async Task AssertRoundTrip(Int64[] input)
	{
		Pb.BlockedInteger proto;
		IReadOnlyList<Int64> result;

		using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
		{
			BlockedInteger.Encode(input.AsSpan(), out proto);
			BlockedInteger.Decode(proto, out result);
		}

		await TestContext.Current!.OutputWriter.WriteLineAsync(
			$"IntSize: {input.Length * sizeof(Int64):N0}, pbSize: {proto.CalculateSize():N0}");

		await Assert.That(result).IsEquivalentTo(input);
	}

	// ─── 기본 라운드트립 ───

	[Test]
	public async Task EmptySpan_ReturnsEmptyList()
	{
		BlockedInteger.Encode(ReadOnlySpan<Int64>.Empty, out var proto);
		BlockedInteger.Decode(proto, out var result);
		await Assert.That(result).IsEmpty();
	}

	[Test]
	public async Task EmptyEnumerable_ReturnsEmptyList()
	{
		BlockedInteger.Encode(Enumerable.Empty<Int64>(), out var proto);
		BlockedInteger.Decode(proto, out var result);
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
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].Arithmetic.Step).IsEqualTo(-10L);
		await Assert.That(proto.Blocks[0].Arithmetic.Count).IsEqualTo(20);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task ArithmeticBlock_StepZeroIsConstant()
	{
		// step=0 등차 수열은 ConstantBlock이 우선
		Int64[] input = new Int64[20];
		Array.Fill(input, 5L);
		BlockedInteger.Encode(input, out var proto);
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
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[0]).IsEqualTo(19L);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[1]).IsEqualTo(18L);
		await Assert.That((Int64)proto.Blocks[0].Descending.Diffs[2]).IsEqualTo(17L);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task DeltaBlock_NonSortedNarrowRange()
	{
		// range=17, 비정렬 → DeltaBlock
		Int64[] input = [10, 5, 15, 3, 7, 12, 1, 14, 8, 17, 4, 11, 6, 13, 2, 16, 9, 0, 10, 5];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_NonSortedWiderRange()
	{
		// range=200, 비정렬 → DeltaBlock
		Int64[] input = [200, 50, 100, 0, 150, 30, 170, 80, 120, 10,
		                 190, 60, 140, 20, 180, 70, 110, 40, 160, 90];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	// ─── 블록 분리 ───

	[Test]
	public async Task BlockSplit_RangeExceeded()
	{
		// unsorted 값 5가 들어올 때 range(0..20000)=20000 > DeltaBlockMax → 블록 분리
		BlockedInteger.Encode([0L, 20000L, 5L], out var proto);
		BlockedInteger.Decode(proto, out var result);
		await Assert.That(proto.Blocks.Count).IsEqualTo(2);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(result).IsEquivalentTo(new List<Int64> { 0, 20000, 5 });
	}

	[Test]
	public async Task BlockSplit_AscendingThenMedium()
	{
		// 비-등차 단조증가 AscendingBlock([0..20010]) → 20005가 역행하며 range 초과 → DeltaBlock([20005,20003,20007])
		Int64[] input = [0, 1, 2, 5, 20000, 20010, 20005, 20003, 20007];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);
		await Assert.That(proto.Blocks.Count).IsEqualTo(2);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task BlockSplit_ArithmeticThenMedium()
	{
		// 등차 수열([0..20000 step 5000]) → 3이 역행하며 range 초과 → DeltaBlock([3,7,5])
		Int64[] input = [0, 5000, 10000, 15000, 20000, 3, 7, 5];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);
		await Assert.That(proto.Blocks.Count).IsEqualTo(2);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Arithmetic);
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await Assert.That(result).IsEquivalentTo(input.ToList());
	}

	[Test]
	public async Task BlockSplit_ThreeTypes_DescendingAscendingMedium()
	{
		// [5,5,5,-20000]: -20000이 내림차순 허용 → DescendingBlock
		// [-19990] 추가 시 내림차순 깨짐 + range 초과 → 블록 분리
		// [-19990,-19985,5]: 오름차순 → AscendingBlock
		// [5] 추가 시 range 초과 → 블록 분리
		// [0,3,-1]: 비정렬 소범위 → DeltaBlock
		Int64[] input = [5, 5, 5, -20000, -19990, -19985, 5, 0, 3, -1];
		BlockedInteger.Encode(input, out var proto);
		BlockedInteger.Decode(proto, out var result);
		await Assert.That(proto.Blocks.Count).IsEqualTo(3);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await Assert.That(proto.Blocks[1].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await Assert.That(proto.Blocks[2].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await Assert.That(result).IsEquivalentTo(input.ToList());
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

		Int32 intSize = input.Count * sizeof(Int64);
		Int32 pbSize = proto.CalculateSize();
		await TestContext.Current!.OutputWriter.WriteLineAsync(
			$"IntSize: {intSize:N0}, pbSize: {pbSize:N0}, ratio: {(double)pbSize / intSize:P1}");

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
	public async Task AscBitmapBlock_CountBoundary_9_UsesAscending()
	{
		// strictly ascending, range=54≤63, count=9 < 10 → AscendingBlock
		Int64[] input = [1, 3, 5, 8, 12, 20, 35, 45, 55];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_CountBoundary_10_UsesBitmap()
	{
		// strictly ascending, range=59≤63, count=10 → AscendingBitmapBlock
		Int64[] input = [1, 3, 5, 8, 12, 20, 35, 45, 55, 60];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_RangeBoundary_63_UsesBitmap()
	{
		// strictly ascending, range=63, count=10 → AscendingBitmapBlock
		Int64[] input = [0, 5, 10, 15, 20, 25, 30, 40, 50, 63];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_RangeBoundary_64_UsesAscending()
	{
		// strictly ascending, range=64 > 63, count=10 → AscendingBlock
		Int64[] input = [0, 5, 10, 15, 20, 25, 30, 40, 50, 64];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_ArithmeticPriority()
	{
		// 등차수열(step=1)은 strictly ascending + range≤63 + count≥10이어도 ArithmeticBlock 우선
		Int64[] input = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
		BlockedInteger.Encode(input, out var proto);
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
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_NegativeFirst_RoundTrip()
	{
		// first < 0 → 정상 동작
		Int64[] input = [-10, -8, -5, -3, 0, 2, 5, 8, 20, 35];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_BitsEncoding()
	{
		// [0,1,2,4,5,6,8,9,10,12]: first=0, range=12
		// bits: pos 0,1,3,4,5,7,8,9,11 → 1+2+8+16+32+128+256+512+2048 = 3003
		Int64[] input = [0, 1, 2, 4, 5, 6, 8, 9, 10, 12];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await Assert.That(proto.Blocks[0].AscendingBitmap.First).IsEqualTo(0L);
		await Assert.That(proto.Blocks[0].AscendingBitmap.Bits).IsEqualTo(3003UL);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task AscBitmapBlock_RoundTrip_Dense()
	{
		// 밀집 비등차 strictly ascending 케이스
		Int64[] input = [50, 51, 53, 54, 55, 57, 58, 59, 60, 61];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.AscendingBitmap);
		await AssertRoundTrip(input);
	}

	// ─── DescendingBitmapBlock ───

	[Test]
	public async Task DescBitmapBlock_CountBoundary_9_UsesDescending()
	{
		// strictly descending, range=54≤63, count=9 < 10 → DescendingBlock
		Int64[] input = [55, 45, 35, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_CountBoundary_10_UsesBitmap()
	{
		// strictly descending, range=59≤63, count=10 → DescendingBitmapBlock
		Int64[] input = [60, 55, 45, 35, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_RangeBoundary_63_UsesBitmap()
	{
		// strictly descending, range=63, count=10 → DescendingBitmapBlock
		Int64[] input = [63, 58, 53, 48, 43, 38, 33, 23, 13, 0];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_RangeBoundary_64_UsesDescending()
	{
		// strictly descending, range=64 > 63, count=10 → DescendingBlock
		Int64[] input = [64, 60, 55, 50, 45, 40, 35, 25, 15, 0];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_DuplicatesUseDescending()
	{
		// 중복 값 포함 → strictly descending 아님 → DescendingBlock
		Int64[] input = [60, 55, 45, 45, 20, 12, 8, 5, 3, 1];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks.Count).IsEqualTo(1);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_NegativeFirst_RoundTrip()
	{
		// first < 0 → 정상 동작
		Int64[] input = [-20, -22, -25, -27, -30, -32, -35, -38, -40, -42];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_BitsEncoding()
	{
		// [12,10,9,8,6,5,4,2,1,0]: first=12, range=12
		// bits: pos 1,2,3,5,6,7,9,10,11 → 2+4+8+32+64+128+512+1024+2048 = 3822
		Int64[] input = [12, 10, 9, 8, 6, 5, 4, 2, 1, 0];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await Assert.That(proto.Blocks[0].DescendingBitmap.First).IsEqualTo(12L);
		await Assert.That(proto.Blocks[0].DescendingBitmap.Bits).IsEqualTo(3822UL);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DescBitmapBlock_RoundTrip_Dense()
	{
		// 밀집 비등차 strictly descending 케이스
		Int64[] input = [61, 60, 59, 57, 56, 55, 53, 51, 50, 49];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.DescendingBitmap);
		await AssertRoundTrip(input);
	}

	// ─── DeltaBlock ───

	[Test]
	public async Task DeltaBlock_ReferenceCalculation_Symmetric()
	{
		// min=0, max=10 → reference=5
		// deltas: 0-5=-5, 10-5=5, 3-5=-2
		Int64[] input = [0, 10, 3];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await Assert.That(proto.Blocks[0].Delta.Reference).IsEqualTo(5L);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_NegativeValues_RoundTrip()
	{
		// 음수만 포함하는 비정렬 시퀀스
		Int64[] input = [-100, -50, -75, -80, -60, -90, -55, -70, -85, -65,
		                 -95, -45, -55, -75, -80, -60, -90, -55, -70, -85];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_PositiveNegativeMixed_RoundTrip()
	{
		// 양수와 음수 혼합
		Int64[] input = [-10, 20, 5, -5, 15, -8, 12, -3, 8, 18,
		                 -7, 10, 2, -9, 17, 4, -6, 13, -1, 11];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_MaxRange_16382()
	{
		// range=16382 (최대 범위)
		Int64[] input = [0, 16382, 5000];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_Duplicates_RoundTrip()
	{
		// 중복 포함 비정렬
		Int64[] input = [10, 5, 10, 3, 5];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_LargeValueSpread_RoundTrip()
	{
		// min=-1000, max=5000, range=6000
		Int64[] input = [-1000, 5000, 0, 2000, -500];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_ReferenceAtExtremes()
	{
		// reference가 Int64 극값 근처
		Int64[] input = [Int64.MaxValue - 100, Int64.MaxValue - 50, Int64.MaxValue - 75];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_ZeroCrossing()
	{
		// 음수에서 양수로 혼합
		Int64[] input = [-100, 100, 0, -50, 80, -20, 60, -80, 40, 90,
		                 -10, 70, -60, 30, -90, 50, -30, 10, -70, 20];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task DeltaBlock_OscillatingValues_RoundTrip()
	{
		// 진동하는 값
		Int64[] input = [0, 100, 10, 90, 20];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Delta);
		await AssertRoundTrip(input);
	}

	// ─── 블록 선택 우선순위 ───

	[Test]
	public async Task BlockSelection_TwoElementDecreasing_UsesDescendingBlock()
	{
		// 2원소 감소 시퀀스 — count < RepeatableBlockMinCount이므로 DeltaBlock 불가, DescendingBlock 선택
		Int64[] input = [100, 50];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Descending);
		await AssertRoundTrip(input);
	}

	[Test]
	public async Task BlockSelection_AllEqualValues_UsesConstantBlock()
	{
		// 모든 값 동일 → ConstantBlock이 DeltaBlock보다 우선
		Int64[] input = new Int64[20];
		BlockedInteger.Encode(input, out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Constant);
	}

	[Test]
	public async Task BlockSelection_SingleValue_UsesAscendingBlock()
	{
		// 단일 값 → AscendingBlock (diff 0개)
		BlockedInteger.Encode([42L], out var proto);
		await Assert.That(proto.Blocks[0].BlockOneofCase)
			.IsEqualTo(Pb.BlockedInteger.Types.Block.BlockOneofOneofCase.Ascending);
		await AssertRoundTrip([42L]);
	}
}
