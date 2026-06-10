using System.Buffers;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Library.SegmentedIntegers;

using PbArithmeticBlock = Pb.BlockedInteger.Types.Block.Types.ArithmeticBlock;
using PbAscendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock;
using PbAscendingBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBlock;
using PbBlock = Pb.BlockedInteger.Types.Block;
using PbBlockedInteger = Pb.BlockedInteger;
using PbConstantBlock = Pb.BlockedInteger.Types.Block.Types.ConstantBlock;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;
using PbDeltaOfDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock;
using PbDescendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBitmapBlock;
using PbDescendingBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBlock;

/// <summary>
/// 임의의 Int64 시퀀스를 패턴 감지 블록 방식으로 압축 (8 block types):
/// - ConstantBlock:         모든 값 동일 (count ≥ 3) → (value, count)
/// - ArithmeticBlock:       등차 수열 (count ≥ 3) → (first, step, count)
/// - AscendingBitmapBlock:  strictly ascending, range ≤ 63, count ≥ 8 → first + uint64 bits
/// - AscendingBlock:        단조증가(비내림차순) → first + repeated uint64 diffs (≤8191개)
/// - DescendingBitmapBlock: strictly descending, range ≤ 63, count ≥ 8 → first + uint64 bits
/// - DescendingBlock:       단조감소(비오름차순) → first + repeated uint64 diffs (≤8191개)
/// - DeltaOfDeltaBlock:     nearly-arithmetic (encoder: max|dod| ≤ 63, proto limit: ≤ 8,191, count ≥ 3)
///                          → first + first_delta + sint64 dods
/// - DeltaBlock:            range ≤ 8,191 → reference + sint64 deltas (≤2-byte zigzag)
/// <para>
/// 인코딩은 deterministic(입력 동일 → 출력 동일)이며 greedy 방식으로 동작합니다.
/// BlockAccumulator는 스트리밍 방식으로 최적 블록 타입을 선택하되 백트래킹을 하지 않으므로,
/// 약간의 조정으로 더 나은 압축률을 얻을 수 있는 경우도 있습니다.
/// Constant/Arithmetic 접두부(≥5개)가 있는 비단조 시퀀스는 해당 접두부를 먼저 분리하여 emit합니다.
/// </para>
/// <para>
/// 모든 public 메서드는 호출별로 상태를 갖지 않으므로(Encode는 호출마다 새 BlockAccumulator를 생성),
/// 동일 proto를 동시에 변경하지 않는 한 스레드로부터 안전하게 호출할 수 있습니다.
/// </para>
/// </summary>
public static class BlockedInteger
{
	private const Int64 DeltaBlockMax = (Int64)PbDeltaBlock.Types.RangeLimit.Max; // 8191
	private const Int64 DeltaOfDeltaBlockMax = (Int64)PbDeltaOfDeltaBlock.Types.DeltaLimit.Max; // 8191
	// 선택 조건: max|dod| ≤ 63 (varint 1바이트 범위), 아니면 DeltaBlock 사용
	private const Int64 DeltaOfDeltaSelectThreshold = 63;
	// proto 스펙상 repeated 필드의 합리적 상한; Ascending/Descending diff 저장 capacity
	private const Int32 MaxBlockValues = 8192;
	private const Int32 RepeatableBlockMinCount = 3;
	private const Int32 DeltaOfDeltaBlockMinCount = 3; // first + first_delta + 1개 dod 최소 (validator 최소 단위)
	private const Int32 PrefixSplitMinCount = 5; // prefix 분리가 이득인 최소 길이 (블록 태그 오버헤드 고려)
	private const Int32 BitmapBlockMinCount = 8;
	private const Int64 BitmapBlockRange = 63;
	private const Int32 UInt64BitWidth = 64;

	/// <summary>
	/// 임의의 Int64 시퀀스를 블록 구조로 인코딩합니다.
	/// </summary>
	/// <returns>인코딩된 블록 구조</returns>
	public static PbBlockedInteger Encode(ReadOnlySpan<Int64> values)
	{
		PbBlockedInteger proto = new();
		if (values.Length == 0) return proto;

		using BlockAccumulator acc = new();
		foreach (Int64 value in values)
		{
			acc.Feed(proto, value);
		}
		acc.Flush(proto);
		return proto;
	}

	/// <summary>
	/// 임의의 Int64 시퀀스를 블록 구조로 인코딩합니다.
	/// </summary>
	/// <returns>인코딩된 블록 구조</returns>
	/// <exception cref="ArgumentNullException">values가 null인 경우</exception>
	public static PbBlockedInteger Encode(IEnumerable<Int64> values)
	{
		ArgumentNullException.ThrowIfNull(values);
		PbBlockedInteger proto = new();

		using BlockAccumulator acc = new();
		foreach (Int64 value in values)
		{
			acc.Feed(proto, value);
		}
		acc.Flush(proto);
		return proto;
	}

	/// <summary>
	/// 블록 구조를 Int64 시퀀스로 디코딩합니다. 순서와 중복을 보존합니다.
	/// </summary>
	/// <remarks>
	/// 신뢰된 입력 전용. 각 블록의 내부 invariant(Deltas/DeltaOfDeltas 비어있지 않음 등)를
	/// 검증하지 않습니다. 신뢰할 수 없는 외부 입력은 먼저 <c>Validators</c>로 검증하세요.
	/// </remarks>
	/// <returns>디코딩된 Int64 시퀀스</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static IReadOnlyList<Int64> Decode(PbBlockedInteger proto)
	{
		ArgumentNullException.ThrowIfNull(proto);

		// 2-pass: 첫 순회에서 totalCount를 계산해 List를 정확히 pre-allocate하고,
		// 두 번째 순회에서 디코딩한다.
		Int64 totalCount = Decoders.GetTotalValueCount(proto);
		if (totalCount > Int32.MaxValue)
			throw new InvalidOperationException(
				$"총 값 개수({totalCount})가 List<Int64> 한계({Int32.MaxValue})를 초과");

		List<Int64> result = new((Int32)totalCount);

		foreach (PbBlock block in proto.Blocks)
		{
			switch (block.BlockOneofCase)
			{
				case PbBlock.BlockOneofOneofCase.Constant:
					Decoders.DecodeConstant(block.Constant, result);
					break;
				case PbBlock.BlockOneofOneofCase.Arithmetic:
					Decoders.DecodeArithmetic(block.Arithmetic, result);
					break;
				case PbBlock.BlockOneofOneofCase.AscendingBitmap:
					Decoders.DecodeAscendingBitmap(block.AscendingBitmap, result);
					break;
				case PbBlock.BlockOneofOneofCase.Ascending:
					Decoders.DecodeAscending(block.Ascending, result);
					break;
				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					Decoders.DecodeDescendingBitmap(block.DescendingBitmap, result);
					break;
				case PbBlock.BlockOneofOneofCase.Descending:
					Decoders.DecodeDescending(block.Descending, result);
					break;
				case PbBlock.BlockOneofOneofCase.DeltaOfDelta:
					Decoders.DecodeDeltaOfDelta(block.DeltaOfDelta, result);
					break;
				case PbBlock.BlockOneofOneofCase.Delta:
					Decoders.DecodeDelta(block.Delta, result);
					break;
				default:
					throw new InvalidOperationException($"알 수 없는 블록 타입: {block.BlockOneofCase}");
			}
		}
		return result;
	}

	/// <summary>
	/// 주어진 페이지 크기로 전체 데이터를 나눌 때 필요한 페이지 개수를 반환합니다.
	/// </summary>
	/// <param name="proto">분석할 프로토콜 버퍼</param>
	/// <param name="pageSize">페이지 크기 (값의 개수)</param>
	/// <returns>필요한 페이지 개수 (데이터가 없으면 0)</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentOutOfRangeException">pageSize &lt;= 0인 경우</exception>
	public static Int32 GetPageCount(PbBlockedInteger proto, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageSize <= 0)
			throw new ArgumentOutOfRangeException(nameof(pageSize),
				$"pageSize({pageSize})는 0보다 커야 함");

		Int64 totalValueCount = Decoders.GetTotalValueCount(proto);

		if (totalValueCount == 0)
			return 0;

		Int64 pageCount = (totalValueCount + pageSize - 1) / pageSize;
		return checked((Int32)pageCount);
	}

	/// <summary>
	/// 지정된 페이지의 값들을 디코딩합니다.
	/// </summary>
	/// <param name="proto">디코딩할 프로토콜 버퍼</param>
	/// <param name="pageIndex">0-based 페이지 번호</param>
	/// <param name="pageSize">페이지 크기 (값의 개수)</param>
	/// <returns>해당 페이지의 디코딩된 값 목록 (범위를 벗어나면 빈 목록)</returns>
	/// <remarks>
	/// pageIndex 범위를 벗어난 경우 빈 결과를 반환합니다.
	/// 신뢰된 입력 전용. 각 블록의 내부 invariant를 검증하지 않습니다.
	/// 신뢰할 수 없는 외부 입력은 먼저 <c>Validators</c>로 검증하세요.
	/// </remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentOutOfRangeException">pageIndex &lt; 0 또는 pageSize &lt;= 0인 경우</exception>
	public static IReadOnlyList<Int64> DecodePage(PbBlockedInteger proto,
		Int32 pageIndex, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageIndex < 0)
			throw new ArgumentOutOfRangeException(nameof(pageIndex),
				$"pageIndex({pageIndex})는 0 이상이어야 함");
		if (pageSize <= 0)
			throw new ArgumentOutOfRangeException(nameof(pageSize),
				$"pageSize({pageSize})는 0보다 커야 함");

		Int64 startLong = checked((Int64)pageIndex * pageSize);
		Int64 endLong = startLong + pageSize;
		if (startLong > Int32.MaxValue)
		{
			return [];
		}
		Int32 startIndex = checked((Int32)startLong);
		Int32 endIndex = endLong > Int32.MaxValue ? Int32.MaxValue : checked((Int32)endLong);

		// capacity는 pageSize로 둔다. 전체 블록을 스캔해 정확한 크기를 구하면
		// 아래 루프의 블록 단위 early-exit 이점을 잃어 페이지네이션이 매번 full scan이 된다.
		// over-allocation은 호출자가 고른 pageSize로 bounded이므로 그대로 수용한다.
		List<Int64> result = new(pageSize);
		// currentIndex/blockEndIndex는 전체 시퀀스 누적 위치이므로 Int32 wrap을 막기 위해 Int64로 누적한다.
		Int64 currentIndex = 0;

		foreach (PbBlock block in proto.Blocks)
		{
			Int32 blockValueCount = Decoders.GetBlockValueCount(block);
			Int64 blockEndIndex = currentIndex + blockValueCount;

			if (blockEndIndex > startIndex && currentIndex < endIndex)
			{
				// 오프셋은 0..blockValueCount 범위이므로 Int32 캐스트가 안전하다.
				Int32 blockStartOffset = (Int32)Math.Max(0L, startIndex - currentIndex);
				Int32 blockEndOffset = (Int32)Math.Min(blockValueCount, endIndex - currentIndex);

				Decoders.DecodeBlockPage(block, blockStartOffset, blockEndOffset, result);
			}

			currentIndex = blockEndIndex;

			if (currentIndex >= endIndex)
				break;
		}

		return result;
	}

	/// <summary>
	/// 블록 구조의 무결성을 검증합니다.
	/// </summary>
	/// <param name="proto">검증할 프로토콜 버퍼</param>
	/// <param name="errors">발견된 에러 메시지 목록</param>
	/// <returns>유효하면 true, 그렇지 않으면 false</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static bool TryValidate(PbBlockedInteger proto, out List<string> errors)
	{
		ArgumentNullException.ThrowIfNull(proto);
		errors = [];

		for (Int32 blockIndex = 0; blockIndex < proto.Blocks.Count; ++blockIndex)
		{
			PbBlock block = proto.Blocks[blockIndex];
			Validators.ValidateBlock(block, blockIndex, errors);
		}

		// 전체 값 개수가 List<Int64> 한계를 넘으면 Decode가 예외를 던지므로 여기서 미리 보고한다.
		// (개별 블록은 유효해도 합계가 초과할 수 있다)
		Int64 totalCount = Decoders.GetTotalValueCount(proto);
		if (totalCount > Int32.MaxValue)
		{
			errors.Add($"전체 값 개수({totalCount})가 디코딩 한계({Int32.MaxValue})를 초과");
		}

		return errors.Count == 0;
	}

	/// <summary>
	/// 압축된 데이터의 통계 정보를 계산합니다.
	/// </summary>
	/// <param name="proto">분석할 프로토콜 버퍼</param>
	/// <returns>압축 통계 정보</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static CompressionStatistics GetCompressionStatistics(PbBlockedInteger proto)
	{
		ArgumentNullException.ThrowIfNull(proto);

		CompressionStatistics statistics = new()
		{
			OriginalSize = 0,
			CompressedSize = proto.CalculateSize(),
			BlockCount = proto.Blocks.Count,
			TotalValues = 0
		};

		foreach (PbBlock block in proto.Blocks)
		{
			StatisticsHelper.AddBlockStatistics(block, statistics);
		}

		statistics.OriginalSize = statistics.TotalValues * sizeof(Int64);
		statistics.CalculateDerivedValues();
		return statistics;
	}

	private static class Encoders
	{
		internal static PbBlock EncodeConstant(ReadOnlySpan<Int64> buffer) =>
			new()
			{
				Constant = new PbConstantBlock
				{
					Value = buffer[0],
					Count = buffer.Length
				}
			};

		internal static PbBlock EncodeArithmetic(ReadOnlySpan<Int64> buffer) =>
			new()
			{
				Arithmetic = new PbArithmeticBlock
				{
					First = buffer[0],
					Step = unchecked(buffer[1] - buffer[0]),
					Count = buffer.Length
				}
			};

		internal static PbBlock EncodeAscendingBitmap(ReadOnlySpan<Int64> buffer)
		{
			Int64 first = buffer[0];
			return new()
			{
				AscendingBitmap = new PbAscendingBitmapBlock
				{
					First = first,
					Bits = BuildBitmapBitsCore(buffer, first, ascending: true)
				}
			};
		}

		internal static PbBlock EncodeAscending(ReadOnlySpan<Int64> buffer)
		{
			PbAscendingBlock block = new() { First = buffer[0] };
			block.Diffs.Capacity = buffer.Length - 1; // diff 개수를 미리 알 수 있어 내부 재할당을 피한다.
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				block.Diffs.Add(unchecked((UInt64)(buffer[i] - buffer[i - 1])));
			}
			return new() { Ascending = block };
		}

		internal static PbBlock EncodeDescendingBitmap(ReadOnlySpan<Int64> buffer)
		{
			Int64 first = buffer[0];
			return new()
			{
				DescendingBitmap = new PbDescendingBitmapBlock
				{
					First = first,
					Bits = BuildBitmapBitsCore(buffer, first, ascending: false)
				}
			};
		}

		internal static PbBlock EncodeDescending(ReadOnlySpan<Int64> buffer)
		{
			PbDescendingBlock block = new() { First = buffer[0] };
			block.Diffs.Capacity = buffer.Length - 1; // diff 개수를 미리 알 수 있어 내부 재할당을 피한다.
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				block.Diffs.Add(unchecked((UInt64)(buffer[i - 1] - buffer[i])));
			}
			return new() { Descending = block };
		}

		internal static PbBlock EncodeDeltaOfDelta(ReadOnlySpan<Int64> buffer)
		{
			PbDeltaOfDeltaBlock block = new() { First = buffer[0] };
			if (buffer.Length >= 2)
			{
				block.FirstDelta = unchecked(buffer[1] - buffer[0]);
				Int64 prevDelta = block.FirstDelta;
				for (Int32 i = 2; i < buffer.Length; i++)
				{
					Int64 delta = unchecked(buffer[i] - buffer[i - 1]);
					block.DeltaOfDeltas.Add(unchecked(delta - prevDelta));
					prevDelta = delta;
				}
			}
			return new() { DeltaOfDelta = block };
		}

		internal static PbBlock EncodeDelta(ReadOnlySpan<Int64> buffer, Int64 min, Int64 max)
		{
			Int64 reference = min + (max - min) / 2;
			PbDeltaBlock block = new() { Reference = reference };
			foreach (Int64 value in buffer)
			{
				block.Deltas.Add(value - reference);
			}
			return new() { Delta = block };
		}

		// buffer[1..n]의 각 값에 대해 비트 위치를 계산한다.
		// ascending: bitPos = value - first - 1 (예: [0,5,10] → bits 4, 9)
		// descending: bitPos = first - value - 1 (예: [12,10,8] → bits 1, 3)
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static UInt64 BuildBitmapBitsCore(ReadOnlySpan<Int64> buffer, Int64 first, bool ascending)
		{
			UInt64 bits = 0UL;
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				Int32 bitPos = ascending
					? (Int32)(buffer[i] - first - 1)
					: (Int32)(first - buffer[i] - 1);
				Debug.Assert((UInt32)bitPos < BitmapBlockRange,
					$"bitPos {bitPos}가 BitmapBlockRange({BitmapBlockRange})를 초과 — 호출자 선택 조건 위반");
				bits |= 1UL << bitPos;
			}
			return bits;
		}
	}

	internal static class Decoders
	{
		// 모든 블록의 값 개수 합계. 블록 합이 Int32 범위를 넘을 수 있으므로 Int64로 누적한다.
		public static Int64 GetTotalValueCount(PbBlockedInteger proto)
		{
			Int64 total = 0;
			foreach (PbBlock block in proto.Blocks)
			{
				total += GetBlockValueCount(block);
			}
			return total;
		}

		public static Int32 GetBlockValueCount(PbBlock block)
		{
			switch (block.BlockOneofCase)
			{
				case PbBlock.BlockOneofOneofCase.Constant:
					return block.Constant?.Count ?? 0;

				case PbBlock.BlockOneofOneofCase.Arithmetic:
					return block.Arithmetic?.Count ?? 0;

				case PbBlock.BlockOneofOneofCase.AscendingBitmap:
					if (block.AscendingBitmap == null) return 0;
					return BitOperations.PopCount(block.AscendingBitmap.Bits) + 1;

				case PbBlock.BlockOneofOneofCase.Ascending:
					if (block.Ascending == null) return 0;
					return block.Ascending.Diffs.Count + 1;

				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					if (block.DescendingBitmap == null) return 0;
					return BitOperations.PopCount(block.DescendingBitmap.Bits) + 1;

				case PbBlock.BlockOneofOneofCase.Descending:
					if (block.Descending == null) return 0;
					return block.Descending.Diffs.Count + 1;

				case PbBlock.BlockOneofOneofCase.DeltaOfDelta:
					if (block.DeltaOfDelta == null) return 0;
					return block.DeltaOfDelta.DeltaOfDeltas.Count + 2;

				case PbBlock.BlockOneofOneofCase.Delta:
					return block.Delta?.Deltas.Count ?? 0;

				default:
					return 0;
			}
		}

		public static void DecodeBlockPage(PbBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			switch (block.BlockOneofCase)
			{
				case PbBlock.BlockOneofOneofCase.Constant:
					DecodeConstantPage(block.Constant, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Arithmetic:
					DecodeArithmeticPage(block.Arithmetic, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.AscendingBitmap:
					DecodeBitmapCorePage(block.AscendingBitmap.First, block.AscendingBitmap.Bits,
						1L, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Ascending:
					DecodeMonotonicCorePage(block.Ascending.First, block.Ascending.Diffs,
						1L, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					DecodeBitmapCorePage(block.DescendingBitmap.First, block.DescendingBitmap.Bits,
						-1L, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Descending:
					DecodeMonotonicCorePage(block.Descending.First, block.Descending.Diffs,
						-1L, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.DeltaOfDelta:
					DecodeDeltaOfDeltaPage(block.DeltaOfDelta, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Delta:
					DecodeDeltaPage(block.Delta, startOffset, endOffset, output);
					break;

				default:
					throw new InvalidOperationException($"알 수 없는 블록 타입: {block.BlockOneofCase}");
			}
		}

		private static void DecodeConstantPage(PbConstantBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 actualEnd = Math.Min(endOffset, block.Count);
			Int32 written = actualEnd - startOffset;
			if (written <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + written);
			CollectionsMarshal.AsSpan(output).Slice(start, written).Fill(block.Value);
		}

		private static void DecodeArithmeticPage(PbArithmeticBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 actualEnd = Math.Min(endOffset, block.Count);
			Int32 written = actualEnd - startOffset;
			if (written <= 0) return;

			Int64 firstInPage = unchecked(block.First + (Int64)startOffset * block.Step);
			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + written);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, written);

			FillArithmetic(dest, firstInPage, block.Step);
		}

		// sign = 1L: ascending (+bitIndex), sign = -1L: descending (-bitIndex)
		private static void DecodeBitmapCorePage(Int64 first, UInt64 bits, Int64 sign,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = BitOperations.PopCount(bits) + 1;
			if (startOffset >= totalCount) return;

			Int32 currentPos = 0;

			if (startOffset == 0 && endOffset > 0)
			{
				output.Add(first);
				currentPos = 1;
			}
			else if (startOffset > 0)
			{
				currentPos = 1;
			}

			Int32 bitIndex = 0;

			while (bits != 0 && currentPos < endOffset)
			{
				Int32 trailingZeros = BitOperations.TrailingZeroCount(bits);
				Int32 shift = trailingZeros + 1;
				bitIndex += shift;

				if (currentPos >= startOffset)
				{
					output.Add(unchecked(first + sign * bitIndex));
				}

				currentPos++;
				if (shift >= UInt64BitWidth) break;
				bits >>= shift;
			}
		}

		// sign = 1L: ascending (+diff), sign = -1L: descending (-diff)
		private static void DecodeMonotonicCorePage(Int64 first, IList<UInt64> diffs, Int64 sign,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			if (startOffset == 0 && endOffset > 0)
			{
				output.Add(first);
			}

			Int64 current = first;
			Int32 skipCount = Math.Max(0, startOffset - 1);
			for (Int32 i = 0; i < skipCount && i < diffs.Count; ++i)
			{
				current = unchecked(current + sign * (Int64)diffs[i]);
			}

			// diffs[i]가 만드는 값의 출력 위치는 i+1 (인덱스 0은 first). endOffset(exclusive)과 비교한다.
			for (Int32 i = skipCount; i < diffs.Count && i + 1 < endOffset; ++i)
			{
				current = unchecked(current + sign * (Int64)diffs[i]);
				output.Add(current);
			}
		}

		private static void DecodeDeltaOfDeltaPage(PbDeltaOfDeltaBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = block.DeltaOfDeltas.Count + 2;
			if (startOffset >= totalCount) return;

			if (startOffset == 0 && endOffset > 0)
			{
				output.Add(block.First);
			}

			if (startOffset <= 1 && endOffset > 1)
			{
				output.Add(unchecked(block.First + block.FirstDelta));
			}

			Int64 current = unchecked(block.First + block.FirstDelta);
			Int64 prevDelta = block.FirstDelta;

			Int32 skipCount = Math.Max(0, startOffset - 2);
			for (Int32 i = 0; i < skipCount && i < block.DeltaOfDeltas.Count; ++i)
			{
				Int64 dod = block.DeltaOfDeltas[i];
				prevDelta = unchecked(prevDelta + dod);
				current = unchecked(current + prevDelta);
			}

			// DeltaOfDeltas[i]가 만드는 값의 출력 위치는 i+2 (인덱스 0,1은 first, first+firstDelta).
			for (Int32 i = skipCount; i < block.DeltaOfDeltas.Count && i + 2 < endOffset; ++i)
			{
				Int64 dod = block.DeltaOfDeltas[i];
				prevDelta = unchecked(prevDelta + dod);
				current = unchecked(current + prevDelta);
				output.Add(current);
			}
		}

		private static void DecodeDeltaPage(PbDeltaBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			// Constant/Arithmetic Page와 동일하게 [startOffset, actualEnd) 범위를 클램프한다.
			// Delta는 각 값이 reference+delta[i]로 인덱스가 곧 위치이므로 first 별도 처리가 없다.
			Int32 actualEnd = Math.Min(endOffset, block.Deltas.Count);
			for (Int32 i = startOffset; i < actualEnd; ++i)
			{
				output.Add(unchecked(block.Reference + block.Deltas[i]));
			}
		}

		public static void DecodeConstant(PbConstantBlock block, List<Int64> output)
		{
			Int32 count = block.Count;
			if (count <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			CollectionsMarshal.AsSpan(output).Slice(start, count).Fill(block.Value);
		}

		public static void DecodeArithmetic(PbArithmeticBlock block, List<Int64> output)
		{
			Int64 first = block.First;
			Int64 step = block.Step;
			Int32 count = block.Count;
			if (count <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			FillArithmetic(dest, first, step);
		}

		private static void FillArithmetic(Span<Int64> dest, Int64 first, Int64 step)
		{
			Int32 count = dest.Length;
			Int32 vIdx = 0;

			if (Vector.IsHardwareAccelerated && count >= Vector<Int64>.Count)
			{
				Int32 width = Vector<Int64>.Count;
				Span<Int64> offsetTmp = stackalloc Int64[width];
				for (Int32 k = 0; k < width; ++k)
				{
					offsetTmp[k] = unchecked(k * step);
				}

				Vector<Int64> laneOffsets = new(offsetTmp);
				Vector<Int64> strideVec = new(unchecked((Int64)width * step));
				Vector<Int64> baseVec = new(first);
				unchecked { baseVec += laneOffsets; }

				Int32 limit = count - width;
				for (; vIdx <= limit; vIdx += width)
				{
					baseVec.CopyTo(dest.Slice(vIdx, width));
					unchecked { baseVec += strideVec; }
				}
			}

			Int64 current = unchecked(first + (Int64)vIdx * step);
			for (Int32 i = vIdx; i < count; ++i)
			{
				dest[i] = current;
				current = unchecked(current + step);
			}
		}

		// sign = 1L: first + bit + 1 (ascending), sign = -1L: first - bit - 1 (descending)
		private static void DecodeBitmapCore(Int64 first, UInt64 bits, Int64 sign, List<Int64> output)
		{
			output.Add(first);
			while (bits != 0)
			{
				Int32 bit = BitOperations.TrailingZeroCount(bits);
				output.Add(unchecked(first + sign * (bit + 1)));
				bits &= bits - 1;
			}
		}

		public static void DecodeAscendingBitmap(PbAscendingBitmapBlock block, List<Int64> output)
			=> DecodeBitmapCore(block.First, block.Bits, 1L, output);

		public static void DecodeDescendingBitmap(PbDescendingBitmapBlock block, List<Int64> output)
			=> DecodeBitmapCore(block.First, block.Bits, -1L, output);

		public static void DecodeAscending(PbAscendingBlock block, List<Int64> output)
		{
			Int32 count = block.Diffs.Count + 1;
			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			Int64 current = block.First;
			dest[0] = current;
			for (Int32 i = 0; i < block.Diffs.Count; ++i)
			{
				current = unchecked(current + (Int64)block.Diffs[i]);
				dest[i + 1] = current;
			}
		}

		public static void DecodeDescending(PbDescendingBlock block, List<Int64> output)
		{
			Int32 count = block.Diffs.Count + 1;
			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			Int64 current = block.First;
			dest[0] = current;
			for (Int32 i = 0; i < block.Diffs.Count; ++i)
			{
				current = unchecked(current - (Int64)block.Diffs[i]);
				dest[i + 1] = current;
			}
		}

		public static void DecodeDelta(PbDeltaBlock block, List<Int64> output)
		{
			Debug.Assert(block.Deltas.Count > 0,
				"DeltaBlock.Deltas must not be empty; encoder never produces this state.");

			Int32 count = block.Deltas.Count;
			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			Int64 reference = block.Reference;
			for (Int32 i = 0; i < count; ++i)
			{
				dest[i] = unchecked(reference + block.Deltas[i]);
			}
		}

		public static void DecodeDeltaOfDelta(PbDeltaOfDeltaBlock block, List<Int64> output)
		{
			Debug.Assert(block.DeltaOfDeltas.Count >= 1,
				"DeltaOfDeltaBlock.DeltaOfDeltas must not be empty; validator and encoder enforce this.");

			Int32 count = block.DeltaOfDeltas.Count + 2;
			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			dest[0] = block.First;
			Int64 current = unchecked(block.First + block.FirstDelta);
			dest[1] = current;
			Int64 prevDelta = block.FirstDelta;
			for (Int32 i = 0; i < block.DeltaOfDeltas.Count; ++i)
			{
				Int64 delta = unchecked(prevDelta + block.DeltaOfDeltas[i]);
				current = unchecked(current + delta);
				dest[i + 2] = current;
				prevDelta = delta;
			}
		}

	}

	private static class Validators
	{
		internal static void ValidateBlock(PbBlock block, Int32 blockIndex, List<string> errors)
		{
			switch (block.BlockOneofCase)
			{
				case PbBlock.BlockOneofOneofCase.Constant:
					ValidateConstantBlock(block.Constant, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.Arithmetic:
					ValidateArithmeticBlock(block.Arithmetic, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.AscendingBitmap:
					ValidateAscendingBitmapBlock(block.AscendingBitmap, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.Ascending:
					ValidateAscendingBlock(block.Ascending, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					ValidateDescendingBitmapBlock(block.DescendingBitmap, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.Descending:
					ValidateDescendingBlock(block.Descending, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.DeltaOfDelta:
					ValidateDeltaOfDeltaBlock(block.DeltaOfDelta, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.Delta:
					ValidateDeltaBlock(block.Delta, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.None:
					errors.Add($"Block[{blockIndex}]: 블록 타입이 설정되지 않음");
					break;
				default:
					errors.Add($"Block[{blockIndex}]: 알 수 없는 블록 타입 {block.BlockOneofCase}");
					break;
			}
		}

		// Ascending/DescendingBitmapBlock의 공통 검증 로직
		private static void ValidateBitmapBlockCore(UInt64 bits, Int32 blockIndex, String label,
			List<String> errors)
		{
			Int32 setBitCount = BitOperations.PopCount(bits) + 1;
			if (setBitCount < BitmapBlockMinCount)
			{
				errors.Add($"Block[{blockIndex}] ({label}): 최소 {BitmapBlockMinCount}개 값 필요" +
					$" (현재: {setBitCount})");
			}

			if (bits > 0)
			{
				// highestBitPosition은 설정된 최상위 비트의 위치 (0-62)
				// rangeSpan = highestBitPosition + 1은 필요한 범위 (1-63)
				Int32 highestBitPosition = 63 - BitOperations.LeadingZeroCount(bits);
				Int32 rangeSpan = highestBitPosition + 1;

				if (rangeSpan > BitmapBlockRange)
				{
					errors.Add($"Block[{blockIndex}] ({label}): 범위는 {BitmapBlockRange} 이하여야 함" +
						$" (현재: {rangeSpan})");
				}
			}
		}

		// Ascending/DescendingBlock의 공통 검증 로직
		private static void ValidateMonotonicBlockCore(Int32 diffsCount, Int32 blockIndex, String label,
			List<String> errors)
		{
			Int32 totalCount = diffsCount + 1;
			if (totalCount > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] ({label}): 최대 {MaxBlockValues}개 값 허용" +
					$" (현재: {totalCount})");
			}
		}

		private static void ValidateConstantBlock(PbConstantBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (Constant): null");
				return;
			}

			// validator는 encoder가 생성하는 것보다 넓은 집합을 수용한다.
			// encoder는 Constant를 RepeatableBlockMinCount(3) 이상에서만 emit하지만,
			// Count 1~2짜리 블록도 디코딩 가능하므로 외부 도구가 만든 proto를 받아들이기 위해 허용한다.
			if (block.Count < 1)
			{
				errors.Add($"Block[{blockIndex}] (Constant): Count는 1 이상이어야 함 (현재: {block.Count})");
			}

			if (block.Count > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (Constant): Count는 {MaxBlockValues} 이하여야 함" +
					$" (현재: {block.Count})");
			}
		}

		private static void ValidateArithmeticBlock(PbArithmeticBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (Arithmetic): null");
				return;
			}

			// validator는 encoder가 생성하는 것보다 넓은 집합을 수용한다.
			// encoder는 Arithmetic을 RepeatableBlockMinCount(3) 이상에서만 emit하지만,
			// Count 1~2짜리 블록도 디코딩 가능하므로 외부 도구가 만든 proto를 받아들이기 위해 허용한다.
			if (block.Count < 1)
			{
				errors.Add($"Block[{blockIndex}] (Arithmetic): Count는 1 이상이어야 함 (현재: {block.Count})");
			}

			if (block.Count > MaxBlockValues)
			{
				errors.Add(
					$"Block[{blockIndex}] (Arithmetic): Count는 {MaxBlockValues} 이하여야 함" +
					$" (현재: {block.Count})");
			}
		}

		private static void ValidateAscendingBitmapBlock(PbAscendingBitmapBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null) { errors.Add($"Block[{blockIndex}] (AscendingBitmap): null"); return; }
			ValidateBitmapBlockCore(block.Bits, blockIndex, "AscendingBitmap", errors);
		}

		private static void ValidateAscendingBlock(PbAscendingBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null) { errors.Add($"Block[{blockIndex}] (Ascending): null"); return; }
			ValidateMonotonicBlockCore(block.Diffs.Count, blockIndex, "Ascending", errors);
		}

		private static void ValidateDescendingBitmapBlock(PbDescendingBitmapBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null) { errors.Add($"Block[{blockIndex}] (DescendingBitmap): null"); return; }
			ValidateBitmapBlockCore(block.Bits, blockIndex, "DescendingBitmap", errors);
		}

		private static void ValidateDescendingBlock(PbDescendingBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null) { errors.Add($"Block[{blockIndex}] (Descending): null"); return; }
			ValidateMonotonicBlockCore(block.Diffs.Count, blockIndex, "Descending", errors);
		}

		private static void ValidateDeltaOfDeltaBlock(PbDeltaOfDeltaBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (DeltaOfDelta): null");
				return;
			}

			if (block.DeltaOfDeltas.Count < 1)
			{
				errors.Add($"Block[{blockIndex}] (DeltaOfDelta): DeltaOfDeltas는 1개 이상이어야 함" +
					$" (현재: {block.DeltaOfDeltas.Count})");
				return;
			}

			Int32 totalCount = block.DeltaOfDeltas.Count + 2;
			if (totalCount > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (DeltaOfDelta): 총 값 개수는 {MaxBlockValues} 이하여야 함" +
					$" (현재: {totalCount})");
			}

			// DeltaOfDeltaBlockMax(8191)는 proto 스펙 상한이며,
			// encoder는 DeltaOfDeltaSelectThreshold(63)까지 생성.
			// 64~8191 구간은 외부 도구가 생성한 proto를 수용하기 위해 validator에서 허용됨.
			foreach (Int64 dod in block.DeltaOfDeltas)
			{
				Int64 absDod = dod >= 0 ? dod : -dod;
				if (absDod > DeltaOfDeltaBlockMax)
				{
					errors.Add($"Block[{blockIndex}] (DeltaOfDelta): max|delta-of-delta|는" +
						$" {DeltaOfDeltaBlockMax} 이하여야 함" +
						$" (현재: {absDod})");
					break;
				}
			}
		}

		private static void ValidateDeltaBlock(PbDeltaBlock block, Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (Delta): null");
				return;
			}

			if (block.Deltas.Count < 1)
			{
				errors.Add($"Block[{blockIndex}] (Delta): Deltas는 1개 이상이어야 함" +
					$" (현재: {block.Deltas.Count})");
				return;
			}

			if (block.Deltas.Count > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (Delta): Deltas는 {MaxBlockValues}개 이하여야 함" +
					$" (현재: {block.Deltas.Count})");
			}

			Int64 min = Int64.MaxValue;
			Int64 max = Int64.MinValue;

			foreach (Int64 delta in block.Deltas)
			{
				Int64 value = unchecked(block.Reference + delta);
				if (value < min) min = value;
				if (value > max) max = value;
			}

			if (unchecked((UInt64)(max - min)) > (UInt64)DeltaBlockMax)
			{
				errors.Add($"Block[{blockIndex}] (Delta): 범위는 {DeltaBlockMax} 이하여야 함" +
					$" (현재: {max - min})");
			}
		}

	}

	/// <summary>
	/// Int64 시퀀스 압축의 통계 정보.
	/// </summary>
	public sealed class CompressionStatistics
	{
		/// <summary>원본 값의 개수.</summary>
		public Int64 TotalValues { get; set; }

		/// <summary>원본 크기 (바이트).</summary>
		public Int64 OriginalSize { get; set; }

		/// <summary>압축 크기 (바이트).</summary>
		public Int64 CompressedSize { get; set; }

		/// <summary>
		/// 압축률 (이론상 0.0 이상, 1.0은 무압축, 0.0에 가까울수록 높은 압축률).
		/// 소규모 입력에서 프로토버프 오버헤드로 인해 1.0을 초과할 수 있습니다.
		/// </summary>
		public Double CompressionRatio { get; private set; }

		/// <summary>블록 개수.</summary>
		public Int32 BlockCount { get; set; }

		/// <summary>평균 블록 크기 (바이트).</summary>
		public Double AverageBlockSize { get; private set; }

		/// <summary>블록 타입별 분포.</summary>
		public Dictionary<String, Int32> BlockTypeDistribution { get; set; } = [];

		public void CalculateDerivedValues()
		{
			CompressionRatio = OriginalSize > 0 ? (Double)CompressedSize / OriginalSize : 0.0;

			AverageBlockSize = BlockCount > 0 ? (Double)CompressedSize / BlockCount : 0.0;
		}
	}

	private static class StatisticsHelper
	{
		internal static void AddBlockStatistics(PbBlock block, CompressionStatistics statistics)
		{
			String blockType = block.BlockOneofCase.ToString();

			statistics.BlockTypeDistribution.TryAdd(blockType, 0);
			statistics.BlockTypeDistribution[blockType]++;
			statistics.TotalValues += Decoders.GetBlockValueCount(block);
		}
	}

	private sealed class BlockAccumulator : IDisposable
	{
		[Flags]
		private enum SequenceFlags : byte
		{
			Constant = 1,
			Arithmetic = 2,
			Ascending = 4,
			Descending = 8,
			StrictlyAscending = 16,
			StrictlyDescending = 32,
			All = Constant | Arithmetic | Ascending | Descending | StrictlyAscending | StrictlyDescending
		}

		// ArrayPool에서 빌린 버퍼. Rent는 MaxBlockValues 이상 크기를 줄 수 있으나
		// 인덱스는 _bufferCount(< MaxBlockValues)로만 관리하므로 정확한 길이에 의존하지 않는다.
		private Int64[] _buffer = ArrayPool<Int64>.Shared.Rent(MaxBlockValues);
		private Int32 _bufferCount;
		private Int64 _min;
		private Int64 _max;
		private Int64 _prev;
		private Int64 _arithmeticStep;  // Arithmetic 플래그가 true인 동안 기대되는 연속 차이값
		private Int64 _prevDelta;
		private UInt64 _maxAbsDod;
		private SequenceFlags _flags;
		private Int32 _constantPrefixCount;   // Constant가 false로 전이될 때의 buffer 길이
		private Int32 _arithmeticPrefixCount; // Arithmetic이 false로 전이될 때의 buffer 길이

		public BlockAccumulator() => Reset();

		public void Dispose()
		{
			// null 가드로 double-dispose에서도 한 번만 반환한다. clearArray는 불필요(다음 Rent 후 _bufferCount=0부터 덮어씀).
			if (_buffer is not null)
			{
				ArrayPool<Int64>.Shared.Return(_buffer);
				_buffer = null!;
			}
		}

		private void Reset()
		{
			_bufferCount = 0;
			_min = Int64.MaxValue;
			_max = Int64.MinValue;
			_flags = SequenceFlags.All;
			_arithmeticStep = 0;
			_prevDelta = 0;
			_maxAbsDod = 0;
			_constantPrefixCount = 0;
			_arithmeticPrefixCount = 0;
			_prev = 0;
		}

		public bool TryAdd(Int64 value)
		{
			if (_bufferCount >= MaxBlockValues) return false;

			Int64 newMin = Math.Min(_min, value);
			Int64 newMax = Math.Max(_max, value);

			if (_bufferCount > 0)
			{
				Int64 prospectiveDelta = unchecked(value - _prev);
				UInt64 prospectiveMaxAbsDod = ComputeProspectiveMaxAbsDod(prospectiveDelta);
				// unchecked 뺄셈은 극값 근처에서 wrap할 수 있으므로
				// 단조성 판정은 원래 값을 직접 비교한다.
				Int32 cmp = value < _prev ? -1 : value > _prev ? 1 : 0;
				UInt64 newRange = unchecked((UInt64)(newMax - newMin));

				// 단조성이 유지되는 한 range 제약 없음(Ascending/DescendingBlock은 diff만 저장).
				// 단조성이 이미 깨졌거나 이번 값으로 깨지면 Delta/DoD 범위 내여야 한다.
				bool monotonicityHolds =
					(_flags & (SequenceFlags.Ascending | SequenceFlags.Descending)) != 0
					&& WillRemainMonotonic(cmp);
				if (!monotonicityHolds && !IsWithinBlockRange(newRange, prospectiveMaxAbsDod))
				{
					return false;
				}

				UpdateMonotonicity(cmp);
				UpdateArithmetic(prospectiveDelta);
				_maxAbsDod = prospectiveMaxAbsDod;
				_prevDelta = prospectiveDelta;
			}

			_prev = value;
			_min = newMin;
			_max = newMax;
			_buffer[_bufferCount++] = value;
			return true;
		}

		private UInt64 ComputeProspectiveMaxAbsDod(Int64 prospectiveDelta)
		{
			if (_bufferCount < 2) return _maxAbsDod;
			Int64 dod = unchecked(prospectiveDelta - _prevDelta);
			// dod == Int64.MinValue일 때 -dod는 unchecked에서 Int64.MinValue로 wrap하지만,
			// (UInt64) 캐스트가 비트 패턴 0x8000...0을 2^63으로 해석하여 |dod|와 정확히 일치함.
			UInt64 absDod = unchecked(dod >= 0 ? (UInt64)dod : (UInt64)(-dod));
			return absDod > _maxAbsDod ? absDod : _maxAbsDod;
		}

		private bool WillRemainMonotonic(Int32 cmp)
		{
			bool nextAscending  = (_flags & SequenceFlags.Ascending)  != 0 && cmp >= 0;
			bool nextDescending = (_flags & SequenceFlags.Descending) != 0 && cmp <= 0;
			return nextAscending || nextDescending;
		}

		private static bool IsWithinBlockRange(UInt64 range, UInt64 maxAbsDod) =>
			range <= (UInt64)DeltaBlockMax || maxAbsDod <= (UInt64)DeltaOfDeltaSelectThreshold;

		private void UpdateMonotonicity(Int32 cmp)
		{
			if (cmp < 0)  _flags &= ~(SequenceFlags.Ascending | SequenceFlags.StrictlyAscending);
			if (cmp > 0)  _flags &= ~(SequenceFlags.Descending | SequenceFlags.StrictlyDescending);
			if (cmp == 0) _flags &= ~(SequenceFlags.StrictlyAscending | SequenceFlags.StrictlyDescending);
			if (cmp != 0)
			{
				if ((_flags & SequenceFlags.Constant) != 0 && _constantPrefixCount == 0)
				{
					_constantPrefixCount = _bufferCount;
				}
				_flags &= ~SequenceFlags.Constant;
			}
		}

		private void UpdateArithmetic(Int64 diff)
		{
			if ((_flags & SequenceFlags.Arithmetic) == 0) return;
			// _bufferCount는 이 호출 시점에 아직 증가 전이므로 1이면 두 번째 원소를 처리 중이다.
			if (_bufferCount == 1)
			{
				_arithmeticStep = diff;
			}
			else if (diff != _arithmeticStep)
			{
				if (_arithmeticPrefixCount == 0)
				{
					_arithmeticPrefixCount = _bufferCount;
				}
				_flags &= ~SequenceFlags.Arithmetic;
			}
		}

		public void Feed(PbBlockedInteger proto, Int64 value)
		{
			if (!TryAdd(value))
			{
				Flush(proto);
				bool added = TryAdd(value);
				Debug.Assert(added, "TryAdd failed after flush — logic invariant violated.");
			}
		}

		public void Flush(PbBlockedInteger proto)
		{
			// Prefix split 후 suffix를 재분석해야 할 수 있으므로 루프로 처리.
			// 매 반복은 prefix를 emit하거나 전체 버퍼를 emit하고 종료.
			while (_bufferCount > 0)
			{
				ReadOnlySpan<Int64> bufferSpan = new(_buffer, 0, _bufferCount);

				// Constant prefix 분리: 데이터가 비단조가 되었을 때만 적용.
				// 단조(ascending/descending) 상태라면 ascending/bitmap 블록이 더 유리할 수 있음.
				if (_constantPrefixCount >= PrefixSplitMinCount
					&& (_flags & (SequenceFlags.Ascending | SequenceFlags.Descending)) == 0)
				{
					proto.Blocks.Add(Encoders.EncodeConstant(bufferSpan.Slice(0, _constantPrefixCount)));
					ReFeed(bufferSpan.Slice(_constantPrefixCount));
					continue;
				}

				// Arithmetic prefix 분리: 비단조 데이터에서만 적용.
				if (_arithmeticPrefixCount >= PrefixSplitMinCount
					&& (_flags & (SequenceFlags.Constant
						| SequenceFlags.Ascending | SequenceFlags.Descending)) == 0)
				{
					proto.Blocks.Add(Encoders.EncodeArithmetic(bufferSpan.Slice(0, _arithmeticPrefixCount)));
					ReFeed(bufferSpan.Slice(_arithmeticPrefixCount));
					continue;
				}

				SelectAndEmitBlock(proto, bufferSpan);
				Reset();
				return;
			}
		}

		// suffix는 _buffer를 가리키는 span이므로 Reset() 후 TryAdd가 낮은 인덱스에
		// 쓰고 높은 인덱스에서 읽는다. prefixCount >= PrefixSplitMinCount(5) > 0이므로
		// 읽기 인덱스가 항상 쓰기 인덱스보다 앞에 있어 aliasing이 없다.
		// suffix는 원본 버퍼의 부분집합이므로 range/maxAbsDod 조건을 반드시 만족한다.
		private void ReFeed(ReadOnlySpan<Int64> suffix)
		{
			Reset();
			foreach (Int64 value in suffix)
			{
				bool added = TryAdd(value);
				Debug.Assert(added, "ReFeed invariant violated: suffix는 원본 버퍼의 부분집합이므로" +
					" TryAdd가 반드시 성공해야 함");
			}
		}

		private void SelectAndEmitBlock(PbBlockedInteger proto, ReadOnlySpan<Int64> bufferSpan)
		{
			// _max - _min이 Int64 범위를 초과할 수 있으므로 UInt64로 비교한다.
			// unchecked 캐스트는 비트 패턴을 그대로 유지해 올바른 UInt64 거리를 만든다.
			UInt64 valueRange = unchecked((UInt64)(_max - _min));

			if ((_flags & SequenceFlags.Constant) != 0
				&& _bufferCount >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeConstant(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Arithmetic) != 0
				&& _bufferCount >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeArithmetic(bufferSpan));
			}
			else if ((_flags & SequenceFlags.StrictlyAscending) != 0
				&& _bufferCount >= BitmapBlockMinCount
				&& valueRange <= (UInt64)BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeAscendingBitmap(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Ascending) != 0)
			{
				proto.Blocks.Add(Encoders.EncodeAscending(bufferSpan));
			}
			else if ((_flags & SequenceFlags.StrictlyDescending) != 0
				&& _bufferCount >= BitmapBlockMinCount
				&& valueRange <= (UInt64)BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeDescendingBitmap(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Descending) != 0)
			{
				proto.Blocks.Add(Encoders.EncodeDescending(bufferSpan));
			}
			else if (_maxAbsDod <= (UInt64)DeltaOfDeltaSelectThreshold
				&& _bufferCount >= DeltaOfDeltaBlockMinCount)
			{
				// 비단조이고 delta-of-delta가 매우 작음 → DeltaOfDeltaBlock (DeltaBlock보다 더 효율적)
				proto.Blocks.Add(Encoders.EncodeDeltaOfDelta(bufferSpan));
			}
			else
			{
				// TryAdd 불변 조건: 비단조 구간에서 range > 8191이면
				// 블록 경계 (Flush 시점에서 range ≤ 8191 또는 DoD 조건 만족 보장)
				proto.Blocks.Add(Encoders.EncodeDelta(bufferSpan, _min, _max));
			}
		}
	}
}
