using System.Buffers.Binary;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Library.SegmentedIntegers;

using PbBlock = Pb.BlockedInteger.Types.Block;
using PbBlockedInteger = Pb.BlockedInteger;
using PbConstantBlock = Pb.BlockedInteger.Types.Block.Types.ConstantBlock;
using PbArithmeticBlock = Pb.BlockedInteger.Types.Block.Types.ArithmeticBlock;
using PbAscendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock;
using PbAscendingBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBlock;
using PbDescendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBitmapBlock;
using PbDescendingBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBlock;
using PbDeltaOfDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;
using PbBitPackedBlock = Pb.BlockedInteger.Types.Block.Types.BitPackedBlock;

/// <summary>
/// 임의의 Int64 시퀀스를 패턴 감지 블록 방식으로 압축 (9 block types):
/// - ConstantBlock:         모든 값 동일 (count ≥ 3) → (value, count)
/// - ArithmeticBlock:       등차 수열 (count ≥ 3) → (first, step, count)
/// - AscendingBitmapBlock:  strictly ascending, range ≤ 63, count ≥ 10 → first + uint64 bits
/// - AscendingBlock:        단조증가(비내림차순) → first + repeated uint64 diffs (≤8191개)
/// - DescendingBitmapBlock: strictly descending, range ≤ 63, count ≥ 10 → first + uint64 bits
/// - DescendingBlock:       단조감소(비오름차순) → first + repeated uint64 diffs (≤8191개)
/// - DeltaOfDeltaBlock:     nearly-arithmetic (encoder: max|dod| ≤ 31, proto limit: ≤ 8,191) → first + first_delta + sint64 dods
/// - DeltaBlock:            range ≤ 16,382 → reference + sint64 deltas (≤2-byte zigzag)
/// - BitPackedBlock:        16,382 < range ≤ 1M → min_value + bit_width + packed bits
/// <para>
/// 인코딩은 deterministic(입력 동일 → 출력 동일)이며 greedy 방식으로 동작합니다.
/// BlockAccumulator는 스트리밍 방식으로 최적 블록 타입을 선택하되 백트래킹을 하지 않으므로,
/// 약간의 조정으로 더 나은 압축률을 얻을 수 있는 경우도 있습니다.
/// </para>
/// </summary>
public static class BlockedInteger
{
	private const Int64 DeltaBlockMax = (Int64)PbDeltaBlock.Types.RangeLimit.Max; // 16382
	private const Int64 DeltaOfDeltaBlockMax = (Int64)PbDeltaOfDeltaBlock.Types.DeltaLimit.Max; // 8191
	private const Int64 DeltaOfDeltaSelectThreshold = 31; // 선택 조건: max|dod| ≤ 31 (매우 작은 변동), 아니면 DeltaBlock 사용
	private const Int64 BitPackedBlockMax = 1_000_000L; // range > 16382인 비단조 데이터, 최대 1M까지 지원
	private const Int32 MaxBlockValues = 8192; // proto 스펙상 repeated 필드의 합리적 상한; Ascending/Descending diff 저장 capacity
	private const Int32 RepeatableBlockMinCount = 3;
	private const Int32 DeltaOfDeltaBlockMinCount = 6; // first + first_delta + 4개 dod 값 이상일 때 효율적
	private const Int32 BitmapBlockMinCount = 10;
	private const Int64 BitmapBlockRange = 63;

	/// <summary>
	/// 임의의 Int64 시퀀스를 블록 구조로 인코딩합니다.
	/// </summary>
	public static void Encode(ReadOnlySpan<Int64> values, out PbBlockedInteger proto)
	{
		proto = new();
		if (values.Length == 0) return;

		BlockAccumulator acc = new();
		foreach (Int64 value in values)
		{
			acc.Feed(proto, value);
		}
		acc.Flush(proto);
	}

	/// <summary>
	/// 임의의 Int64 시퀀스를 블록 구조로 인코딩합니다.
	/// </summary>
	/// <exception cref="ArgumentNullException">values가 null인 경우</exception>
	public static void Encode(IEnumerable<Int64> values, out PbBlockedInteger proto)
	{
		ArgumentNullException.ThrowIfNull(values);
		proto = new();

		BlockAccumulator acc = new();
		foreach (Int64 value in values)
		{
			acc.Feed(proto, value);
		}
		acc.Flush(proto);
	}

	/// <summary>
	/// 블록 구조를 Int64 시퀀스로 디코딩합니다. 순서와 중복을 보존합니다.
	/// </summary>
	/// <remarks>신뢰된 입력 전용. 외부 proto는 Count 범위 등을 검증하지 않습니다.</remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static void Decode(PbBlockedInteger proto, out IReadOnlyList<Int64> integers)
	{
		ArgumentNullException.ThrowIfNull(proto);
		Int32 totalCount = 0;
		foreach (PbBlock block in proto.Blocks)
		{
			totalCount += Decoders.GetBlockValueCount(block);
		}

		List<Int64> result = new(totalCount);

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
				case PbBlock.BlockOneofOneofCase.BitPacked:
					Decoders.DecodeBitPacked(block.BitPacked, result);
					break;
				default:
					throw new InvalidOperationException($"Unknown block type: {block.BlockOneofCase}");
			}
		}
		integers = result;
	}

	/// <summary>
	/// 주어진 페이지 크기로 전체 데이터를 나눌 때 필요한 페이지 개수를 반환합니다.
	/// </summary>
	/// <param name="proto">분석할 프로토콜 버퍼</param>
	/// <param name="pageSize">페이지 크기 (값의 개수)</param>
	/// <returns>필요한 페이지 개수 (데이터가 없으면 0)</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentException">pageSize &lt;= 0인 경우</exception>
	public static Int32 GetPageCount(PbBlockedInteger proto, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageSize <= 0)
			throw new ArgumentException(
				$"pageSize ({pageSize}) must be > 0", nameof(pageSize));

		Int32 totalValueCount = 0;
		foreach (PbBlock block in proto.Blocks)
		{
			totalValueCount += Decoders.GetBlockValueCount(block);
		}

		if (totalValueCount == 0)
			return 0;

		return (totalValueCount + pageSize - 1) / pageSize;
	}

	/// <summary>
	/// 지정된 페이지의 값들을 디코딩합니다.
	/// </summary>
	/// <param name="proto">디코딩할 프로토콜 버퍼</param>
	/// <param name="pageIndex">0-based 페이지 번호</param>
	/// <param name="pageSize">페이지 크기 (값의 개수)</param>
	/// <param name="integers">디코딩된 값 목록</param>
	/// <remarks>
	/// pageIndex 범위를 벗어난 경우 빈 결과를 반환합니다.
	/// Trusted input only.
	/// </remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentException">pageIndex &lt; 0 또는 pageSize &lt;= 0인 경우</exception>
	public static void DecodePage(PbBlockedInteger proto,
		Int32 pageIndex, Int32 pageSize, out IReadOnlyList<Int64> integers)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageIndex < 0)
			throw new ArgumentException(
				$"pageIndex ({pageIndex}) must be >= 0", nameof(pageIndex));
		if (pageSize <= 0)
			throw new ArgumentException(
				$"pageSize ({pageSize}) must be > 0", nameof(pageSize));

		Int64 startLong = checked((Int64)pageIndex * pageSize);
		Int64 endLong = startLong + pageSize;
		if (startLong > Int32.MaxValue)
		{
			integers = [];
			return;
		}
		Int32 startIndex = checked((Int32)startLong);
		Int32 endIndex = endLong > Int32.MaxValue ? Int32.MaxValue : checked((Int32)endLong);

		List<Int64> result = new(pageSize);
		Int32 currentIndex = 0;

		foreach (PbBlock block in proto.Blocks)
		{
			Int32 blockValueCount = Decoders.GetBlockValueCount(block);
			Int32 blockEndIndex = currentIndex + blockValueCount;

			if (blockEndIndex > startIndex && currentIndex < endIndex)
			{
				Int32 blockStartOffset = Math.Max(0, startIndex - currentIndex);
				Int32 blockEndOffset = Math.Min(blockValueCount, endIndex - currentIndex);

				Decoders.DecodeBlockPage(block, blockStartOffset, blockEndOffset, result);
			}

			currentIndex = blockEndIndex;

			if (currentIndex >= endIndex)
				break;
		}

		integers = result;
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

		return errors.Count == 0;
	}

	/// <summary>
	/// 압축된 데이터의 통계 정보를 계산합니다.
	/// </summary>
	/// <param name="proto">분석할 프로토콜 버퍼</param>
	/// <param name="statistics">통계 정보</param>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static void GetCompressionStatistics(PbBlockedInteger proto, out CompressionStatistics statistics)
	{
		ArgumentNullException.ThrowIfNull(proto);

		statistics = new()
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
	}

	private static class Encoders
	{
		public static PbBlock EncodeConstant(ReadOnlySpan<Int64> buffer) =>
			new()
			{
				Constant = new PbConstantBlock
				{
					Value = buffer[0],
					Count = buffer.Length
				}
			};

		public static PbBlock EncodeArithmetic(ReadOnlySpan<Int64> buffer) =>
			new()
			{
				Arithmetic = new PbArithmeticBlock
				{
					First = buffer[0],
					Step = unchecked(buffer[1] - buffer[0]),
					Count = buffer.Length
				}
			};

		public static PbBlock EncodeAscendingBitmap(ReadOnlySpan<Int64> buffer)
		{
			Int64 first = buffer[0];
			return new()
			{
				AscendingBitmap = new PbAscendingBitmapBlock
				{
					First = first,
					Bits = BuildAscendingBitmapBits(buffer, first)
				}
			};
		}

		public static PbBlock EncodeAscending(ReadOnlySpan<Int64> buffer)
		{
			PbAscendingBlock block = new() { First = buffer[0] };
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				block.Diffs.Add(unchecked((UInt64)(buffer[i] - buffer[i - 1])));
			}
			return new() { Ascending = block };
		}

		public static PbBlock EncodeDescendingBitmap(ReadOnlySpan<Int64> buffer)
		{
			Int64 first = buffer[0];
			return new()
			{
				DescendingBitmap = new PbDescendingBitmapBlock
				{
					First = first,
					Bits = BuildDescendingBitmapBits(buffer, first)
				}
			};
		}

		public static PbBlock EncodeDescending(ReadOnlySpan<Int64> buffer)
		{
			PbDescendingBlock block = new() { First = buffer[0] };
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				block.Diffs.Add(unchecked((UInt64)(buffer[i - 1] - buffer[i])));
			}
			return new() { Descending = block };
		}

		public static PbBlock EncodeDeltaOfDelta(ReadOnlySpan<Int64> buffer)
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

		public static PbBlock EncodeDelta(ReadOnlySpan<Int64> buffer, Int64 min, Int64 max)
		{
			Int64 reference = min + (max - min) / 2;
			PbDeltaBlock block = new() { Reference = reference };
			foreach (Int64 value in buffer)
			{
				block.Deltas.Add(value - reference);
			}
			return new() { Delta = block };
		}

		public static PbBlock EncodeBitPacked(ReadOnlySpan<Int64> buffer, Int64 min, Int64 max)
		{
			UInt64 range = unchecked((UInt64)(max - min));
			Int32 bitWidth = range == 0 ? 1 : (BitOperations.Log2(range) + 1);
			byte[] packed = PackBits(buffer, min, bitWidth);
			return new() { BitPacked = new PbBitPackedBlock
			{
				MinValue = min,
				BitWidth = (UInt32)bitWidth,
				Count = (UInt32)buffer.Length,
				PackedData = Google.Protobuf.UnsafeByteOperations.UnsafeWrap(packed)
			}};
		}

		private static byte[] PackBits(ReadOnlySpan<Int64> buffer, Int64 min, Int32 bitWidth)
		{
			Int32 totalBytes = (buffer.Length * bitWidth + 7) / 8;
			byte[] result = new byte[totalBytes];
			Int32 bitIndex = 0;

			foreach (Int64 value in buffer)
			{
				UInt64 offset = unchecked((UInt64)(value - min));
				for (Int32 b = 0; b < bitWidth; b++)
				{
					if ((offset & (1UL << b)) != 0)
					{
						result[bitIndex >> 3] |= (byte)(1 << (bitIndex & 7));
					}
					bitIndex++;
				}
			}
			return result;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static UInt64 BuildAscendingBitmapBits(ReadOnlySpan<Int64> buffer, Int64 first)
		{
			// buffer[1..n]의 각 값에 대해, (value - first - 1) 위치의 비트 설정
			// 예: buffer = [0, 5, 10], first = 0
			//     → bits |= 1 << 4, 1 << 9  (positions 4, 9)
			UInt64 bits = 0UL;
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				Int32 bitPos = (Int32)(buffer[i] - first - 1);
				bits |= 1UL << bitPos;
			}
			return bits;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static UInt64 BuildDescendingBitmapBits(ReadOnlySpan<Int64> buffer, Int64 first)
		{
			// buffer[1..n]의 각 값에 대해, (first - value - 1) 위치의 비트 설정
			// 예: buffer = [12, 10, 8], first = 12
			//     → bits |= 1 << 1, 1 << 3  (positions 1, 3)
			UInt64 bits = 0UL;
			for (Int32 i = 1; i < buffer.Length; ++i)
			{
				Int32 bitPos = (Int32)(first - buffer[i] - 1);
				bits |= 1UL << bitPos;
			}
			return bits;
		}
	}

	internal static class Decoders
	{
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

				case PbBlock.BlockOneofOneofCase.BitPacked:
					return (Int32)(block.BitPacked?.Count ?? 0);

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
					DecodeAscendingBitmapPage(block.AscendingBitmap, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Ascending:
					DecodeAscendingPage(block.Ascending, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					DecodeDescendingBitmapPage(block.DescendingBitmap, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Descending:
					DecodeDescendingPage(block.Descending, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.DeltaOfDelta:
					DecodeDeltaOfDeltaPage(block.DeltaOfDelta, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.Delta:
					DecodeDeltaPage(block.Delta, startOffset, endOffset, output);
					break;

				case PbBlock.BlockOneofOneofCase.BitPacked:
					DecodeBitPackedPage(block.BitPacked, startOffset, endOffset, output);
					break;

				default:
					throw new InvalidOperationException($"Unknown block type: {block.BlockOneofCase}");
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

		private static void DecodeAscendingBitmapPage(PbAscendingBitmapBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = BitOperations.PopCount(block.Bits) + 1;
			if (startOffset >= totalCount) return;

			Int64 first = block.First;
			Int32 currentPos = 0;

			// 첫 값 (인덱스 0)
			if (startOffset == 0 && endOffset > 0)
			{
				output.Add(first);
				currentPos = 1;
			}
			else if (startOffset > 0)
			{
				currentPos = 1;
			}

			UInt64 bits = block.Bits;
			Int32 bitIndex = 0;

			while (bits != 0 && currentPos < endOffset)
			{
				Int32 trailingZeros = BitOperations.TrailingZeroCount(bits);
				bitIndex += trailingZeros + 1;

				if (currentPos >= startOffset)
				{
					output.Add(first + bitIndex);
				}

				currentPos++;
				bits >>= trailingZeros + 1;
			}
		}

		private static void DecodeAscendingPage(PbAscendingBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			if (startOffset == 0)
			{
				output.Add(block.First);
			}

			Int64 current = block.First;
			Int32 skipCount = Math.Max(0, startOffset - 1);
			for (Int32 i = 0; i < skipCount && i < block.Diffs.Count; ++i)
			{
				current = unchecked(current + (Int64)block.Diffs[i]);
			}

			for (Int32 i = skipCount; i < block.Diffs.Count && i + 1 < endOffset; ++i)
			{
				current = unchecked(current + (Int64)block.Diffs[i]);
				output.Add(current);
			}
		}

		private static void DecodeDescendingBitmapPage(PbDescendingBitmapBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = BitOperations.PopCount(block.Bits) + 1;
			if (startOffset >= totalCount) return;

			Int64 first = block.First;
			Int32 currentPos = 0;

			// 첫 값 (인덱스 0)
			if (startOffset == 0 && endOffset > 0)
			{
				output.Add(first);
				currentPos = 1;
			}
			else if (startOffset > 0)
			{
				currentPos = 1;
			}

			UInt64 bits = block.Bits;
			Int32 bitIndex = 0;

			while (bits != 0 && currentPos < endOffset)
			{
				Int32 trailingZeros = BitOperations.TrailingZeroCount(bits);
				bitIndex += trailingZeros + 1;

				if (currentPos >= startOffset)
				{
					output.Add(first - bitIndex);
				}

				currentPos++;
				bits >>= trailingZeros + 1;
			}
		}

		private static void DecodeDescendingPage(PbDescendingBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			if (startOffset == 0)
			{
				output.Add(block.First);
			}

			Int64 current = block.First;
			Int32 skipCount = Math.Max(0, startOffset - 1);
			for (Int32 i = 0; i < skipCount && i < block.Diffs.Count; ++i)
			{
				current = unchecked(current - (Int64)block.Diffs[i]);
			}

			for (Int32 i = skipCount; i < block.Diffs.Count && i + 1 < endOffset; ++i)
			{
				current = unchecked(current - (Int64)block.Diffs[i]);
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
			for (Int32 i = startOffset; i < endOffset && i < block.Deltas.Count; ++i)
			{
				output.Add(unchecked(block.Reference + block.Deltas[i]));
			}
		}

		private static void DecodeBitPackedPage(PbBitPackedBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			ReadOnlySpan<byte> data = block.PackedData.Span;
			Int32 bitWidth = (Int32)block.BitWidth;
			Int32 count = (Int32)block.Count;
			Int64 min = block.MinValue;

			Int32 actualEnd = Math.Min(endOffset, count);
			if (startOffset >= actualEnd) return;

			Int32 written = actualEnd - startOffset;

			if (written == 0 || bitWidth > 57)
			{
				Int32 byteIndex = startOffset * bitWidth >> 3;
				Int32 bitInByte = (startOffset * bitWidth) & 7;
				byte currentByte = data[byteIndex];

				for (Int32 i = startOffset; i < actualEnd; i++)
				{
					UInt64 value = 0;
					for (Int32 b = 0; b < bitWidth; b++)
					{
						if ((currentByte & (1 << bitInByte)) != 0)
						{
							value |= 1UL << b;
						}
						bitInByte++;
						if (bitInByte == 8)
						{
							bitInByte = 0;
							byteIndex++;
							if (byteIndex < data.Length)
								currentByte = data[byteIndex];
						}
					}

					output.Add(unchecked(min + (Int64)value));
				}
				return;
			}

			UInt64 mask = (1UL << bitWidth) - 1;
			Int32 outputStart = output.Count;
			CollectionsMarshal.SetCount(output, outputStart + written);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(outputStart, written);

			Int32 startBitOffset = startOffset * bitWidth;
			Int32 safeEnd =
				startOffset + ComputeSafeCount(data.Length - (startBitOffset >> 3), written, bitWidth);

			// 빠른 경로: 64비트 슬라이딩 윈도우
			for (Int32 i = startOffset; i < safeEnd; ++i)
			{
				Int32 bitOffset = i * bitWidth;
				UInt64 raw =
					System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(bitOffset >> 3));
				dest[i - startOffset] = unchecked(min + (Int64)((raw >> (bitOffset & 7)) & mask));
			}

			// tail: 기존 스칼라
			Int32 bitIdx = safeEnd * bitWidth;
			for (Int32 i = safeEnd; i < actualEnd; i++)
			{
				UInt64 value = 0;
				for (Int32 b = 0; b < bitWidth; b++)
				{
					if ((data[bitIdx >> 3] & (1 << (bitIdx & 7))) != 0)
						value |= 1UL << b;
					bitIdx++;
				}
				dest[i - startOffset] = unchecked(min + (Int64)value);
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
				for (Int32 k = 0; k < width; k++)
					offsetTmp[k] = unchecked(k * step);

				var laneOffsets = new Vector<Int64>(offsetTmp);
				var strideVec = new Vector<Int64>(unchecked((Int64)width * step));
				var baseVec = new Vector<Int64>(first);
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

		public static void DecodeAscendingBitmap(PbAscendingBitmapBlock block, List<Int64> output)
		{
			Int64 first = block.First;
			output.Add(first);
			UInt64 bits = block.Bits;
			while (bits != 0)
			{
				Int32 bit = BitOperations.TrailingZeroCount(bits);
				output.Add(first + bit + 1);
				bits &= bits - 1;
			}
		}

		public static void DecodeAscending(PbAscendingBlock block, List<Int64> output)
		{
			Int64 current = block.First;
			output.Add(current);
			foreach (UInt64 diff in block.Diffs)
			{
				current = unchecked(current + (Int64)diff);
				output.Add(current);
			}
		}

		public static void DecodeDescendingBitmap(PbDescendingBitmapBlock block, List<Int64> output)
		{
			Int64 first = block.First;
			output.Add(first);
			UInt64 bits = block.Bits;
			while (bits != 0)
			{
				Int32 bit = BitOperations.TrailingZeroCount(bits);
				output.Add(first - bit - 1);
				bits &= bits - 1;
			}
		}

		public static void DecodeDescending(PbDescendingBlock block, List<Int64> output)
		{
			Int64 current = block.First;
			output.Add(current);
			foreach (UInt64 diff in block.Diffs)
			{
				current = unchecked(current - (Int64)diff);
				output.Add(current);
			}
		}

		public static void DecodeDelta(PbDeltaBlock block, List<Int64> output)
		{
			Debug.Assert(block.Deltas.Count > 0,
				"DeltaBlock.Deltas must not be empty; encoder never produces this state.");

			foreach (Int64 delta in block.Deltas)
			{
				output.Add(unchecked(block.Reference + delta));
			}
		}

		public static void DecodeDeltaOfDelta(PbDeltaOfDeltaBlock block, List<Int64> output)
		{
			output.Add(block.First);
			Debug.Assert(block.DeltaOfDeltas.Count >= 1,
				"DeltaOfDeltaBlock.DeltaOfDeltas must not be empty; validator and encoder enforce this.");

			Int64 current = unchecked(block.First + block.FirstDelta);
			output.Add(current);
			Int64 prevDelta = block.FirstDelta;
			foreach (Int64 dod in block.DeltaOfDeltas)
			{
				Int64 delta = unchecked(prevDelta + dod);
				current = unchecked(current + delta);
				output.Add(current);
				prevDelta = delta;
			}
		}

		public static void DecodeBitPacked(PbBitPackedBlock block, List<Int64> output)
		{
			UnpackBits(block, output);
		}

		private static void UnpackBits(PbBitPackedBlock block, List<Int64> output)
		{
			ReadOnlySpan<byte> data = block.PackedData.Span;
			Int32 bitWidth = (Int32)block.BitWidth;
			Int32 count = (Int32)block.Count;
			Int64 min = block.MinValue;

			if (count == 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + count);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, count);

			if (bitWidth > 57)
			{
				// Fallback: 스칼라만 사용
				Int32 bitIdx = 0;
				for (Int32 i = 0; i < count; i++)
				{
					UInt64 value = 0;
					for (Int32 b = 0; b < bitWidth; b++)
					{
						if ((data[bitIdx >> 3] & (1 << (bitIdx & 7))) != 0)
							value |= 1UL << b;
						bitIdx++;
					}
					dest[i] = unchecked(min + (Int64)value);
				}
				return;
			}

			UInt64 mask = (1UL << bitWidth) - 1;
			Int32 safeCount = ComputeSafeCount(data.Length, count, bitWidth);

			// 빠른 경로: 64비트 슬라이딩 윈도우
			for (Int32 i = 0; i < safeCount; i++)
			{
				Int32 bitOffset = i * bitWidth;
				UInt64 raw = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(bitOffset >> 3));
				dest[i] = unchecked(min + (Int64)((raw >> (bitOffset & 7)) & mask));
			}

			// tail: 스칼라
			Int32 bitIdx2 = safeCount * bitWidth;
			for (Int32 i = safeCount; i < count; i++)
			{
				UInt64 value = 0;
				for (Int32 b = 0; b < bitWidth; b++)
				{
					if ((data[bitIdx2 >> 3] & (1 << (bitIdx2 & 7))) != 0)
						value |= 1UL << b;
					bitIdx2++;
				}
				dest[i] = unchecked(min + (Int64)value);
			}
		}

		private static Int32 ComputeSafeCount(Int32 dataLength, Int32 count, Int32 bitWidth)
		{
			if (dataLength < 8) return 0;
			Int64 safeBitLimit = (Int64)(dataLength - 8) * 8 + 7;
			Int32 safe = (Int32)(safeBitLimit / bitWidth) + 1;
			return Math.Min(count, Math.Max(0, safe));
		}
	}

	private static class Validators
	{
		public static void ValidateBlock(PbBlock block, Int32 blockIndex, List<string> errors)
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
				case PbBlock.BlockOneofOneofCase.BitPacked:
					ValidateBitPackedBlock(block.BitPacked, blockIndex, errors);
					break;
				case PbBlock.BlockOneofOneofCase.None:
					errors.Add($"Block[{blockIndex}]: 블록 타입이 설정되지 않음");
					break;
				default:
					errors.Add($"Block[{blockIndex}]: 알 수 없는 블록 타입 {block.BlockOneofCase}");
					break;
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
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (AscendingBitmap): null");
				return;
			}

			Int32 setBitCount = BitOperations.PopCount(block.Bits) + 1;
			if (setBitCount < BitmapBlockMinCount)
			{
				errors.Add($"Block[{blockIndex}] (AscendingBitmap): 최소 {BitmapBlockMinCount}개 값 필요" +
					$" (현재: {setBitCount})");
			}

			if (block.Bits > 0)
			{
				// highestBitPosition은 설정된 최상위 비트의 위치 (0-62)
				// rangeSpan = highestBitPosition + 1은 필요한 범위 (1-63)
				Int32 highestBitPosition = 63 - BitOperations.LeadingZeroCount(block.Bits);
				Int32 rangeSpan = highestBitPosition + 1;

				if (rangeSpan > BitmapBlockRange)
				{
					errors.Add(
						$"Block[{blockIndex}] (AscendingBitmap): 범위는 {BitmapBlockRange} 이하여야 함" +
						$" (현재: {rangeSpan})");
				}
			}
		}

		private static void ValidateAscendingBlock(PbAscendingBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (Ascending): null");
				return;
			}

			Int32 totalCount = block.Diffs.Count + 1;
			if (totalCount > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (Ascending): 최대 {MaxBlockValues}개 값 허용" +
					$" (현재: {totalCount})");
			}
		}

		private static void ValidateDescendingBitmapBlock(PbDescendingBitmapBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (DescendingBitmap): null");
				return;
			}

			Int32 setBitCount = BitOperations.PopCount(block.Bits) + 1;
			if (setBitCount < BitmapBlockMinCount)
			{
				errors.Add($"Block[{blockIndex}] (DescendingBitmap): 최소 {BitmapBlockMinCount}개 값 필요" +
					$" (현재: {setBitCount})");
			}

			if (block.Bits > 0)
			{
				// highestBitPosition은 설정된 최상위 비트의 위치 (0-62)
				// rangeSpan = highestBitPosition + 1은 필요한 범위 (1-63)
				Int32 highestBitPosition = 63 - BitOperations.LeadingZeroCount(block.Bits);
				Int32 rangeSpan = highestBitPosition + 1;

				if (rangeSpan > BitmapBlockRange)
				{
					errors.Add($"Block[{blockIndex}] (DescendingBitmap): 범위는 {BitmapBlockRange} 이하여야 함" +
						$" (현재: {rangeSpan})");
				}
			}
		}

		private static void ValidateDescendingBlock(PbDescendingBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (Descending): null");
				return;
			}

			Int32 totalCount = block.Diffs.Count + 1;
			if (totalCount > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (Descending): 최대 {MaxBlockValues}개 값 허용" +
					$" (현재: {totalCount})");
			}
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

			// DeltaOfDeltaBlockMax(8191)는 proto 스펙 상한이며, encoder는 DeltaOfDeltaSelectThreshold(31)만 생성.
			// 31~8191 구간은 외부 도구가 생성한 proto를 수용하기 위해 validator에서 허용됨.
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

		private static void ValidateBitPackedBlock(PbBitPackedBlock block,
			Int32 blockIndex, List<string> errors)
		{
			if (block is null)
			{
				errors.Add($"Block[{blockIndex}] (BitPacked): null");
				return;
			}

			if (block.Count < 1)
			{
				errors.Add($"Block[{blockIndex}] (BitPacked): Count는 1 이상이어야 함" +
					$" (현재: {block.Count})");
				return;
			}

			if (block.Count > MaxBlockValues)
			{
				errors.Add($"Block[{blockIndex}] (BitPacked): Count는 {MaxBlockValues} 이하여야 함" +
					$" (현재: {block.Count})");
			}

			// 인코더는 Log2(BitPackedBlockMax=1_000_000) + 1 = 20 이하의 BitWidth만 생성하지만,
			// 프로토버프 확장성과 하위 호환성을 위해 상한을 32로 유지합니다.
			if (block.BitWidth < 1 || block.BitWidth > 32)
			{
				errors.Add($"Block[{blockIndex}] (BitPacked): BitWidth는 1~32 사이여야 함" +
					$" (현재: {block.BitWidth})");
				return;
			}

			Int32 expectedBytes = ((Int32)block.Count * (Int32)block.BitWidth + 7) / 8;
			Int32 actualBytes = block.PackedData.Length;
			if (actualBytes != expectedBytes)
			{
				errors.Add($"Block[{blockIndex}] (BitPacked): PackedData 길이는 {expectedBytes}여야 함" +
					$" (현재: {actualBytes})");
			}
		}
	}

	/// <summary>
	/// Int64 시퀀스 압축의 통계 정보.
	/// </summary>
	public sealed class CompressionStatistics
	{
		/// <summary>원본 값의 개수.</summary>
		public Int32 TotalValues { get; set; }

		/// <summary>원본 크기 (바이트).</summary>
		public Int32 OriginalSize { get; set; }

		/// <summary>압축 크기 (바이트).</summary>
		public Int32 CompressedSize { get; set; }

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
			if (OriginalSize > 0)
			{
				CompressionRatio = (Double)CompressedSize / OriginalSize;
			}
			else
			{
				CompressionRatio = 0.0;
			}

			if (BlockCount > 0)
			{
				AverageBlockSize = (Double)CompressedSize / BlockCount;
			}
			else
			{
				AverageBlockSize = 0.0;
			}
		}
	}

	private static class StatisticsHelper
	{
		public static void AddBlockStatistics(PbBlock block, CompressionStatistics statistics)
		{
			String blockType = block.BlockOneofCase.ToString();

			statistics.BlockTypeDistribution.TryAdd(blockType, 0);
			statistics.BlockTypeDistribution[blockType]++;
			statistics.TotalValues += Decoders.GetBlockValueCount(block);
		}
	}

	private sealed class BlockAccumulator
	{
		private readonly List<Int64> _buffer = new(MaxBlockValues);
		private Int64 _min;
		private Int64 _max;
		private Int64 _prev;
		private Int64 _prevDiff;
		private Int64 _prevDelta;
		private UInt64 _maxAbsDod;
		private bool _isAscending;
		private bool _isDescending;
		private bool _isConstant;
		private bool _isArithmetic;
		private bool _isStrictlyAscending;
		private bool _isStrictlyDescending;

		public BlockAccumulator() => Reset();

		private void Reset()
		{
			_buffer.Clear();
			_min = Int64.MaxValue;
			_max = Int64.MinValue;
			_isAscending = true;
			_isDescending = true;
			_isConstant = true;
			_isArithmetic = true;
			_isStrictlyAscending = true;
			_isStrictlyDescending = true;
			_prevDiff = 0;
			_prevDelta = 0;
			_maxAbsDod = 0;
			_prev = 0; // 방어적 초기화. _prev 읽기는 _buffer.Count > 0 조건 하에서만 발생하지만 명시적 초기화가 코드 보수성 향상
		}

		public bool TryAdd(Int64 value)
		{
			if (_buffer.Count >= MaxBlockValues) return false;

			Int64 newMin = Math.Min(_min, value);
			Int64 newMax = Math.Max(_max, value);
			UInt64 newRange = unchecked((UInt64)(newMax - newMin));

			// prospective DoD 계산
			Int64 prospectiveDelta = _buffer.Count > 0 ? unchecked(value - _prev) : 0;
			UInt64 prospectiveMaxAbsDod = _maxAbsDod;
			if (_buffer.Count >= 2)
			{
				Int64 dod = unchecked(prospectiveDelta - _prevDelta);
				UInt64 absDod = dod >= 0 ? (UInt64)dod : (UInt64)(-dod);
				if (absDod > prospectiveMaxAbsDod) prospectiveMaxAbsDod = absDod;
			}

			// Ascending/DescendingBlock은 diff만 저장하므로 range 제약이 불필요.
			// 어느 한 방향이라도 정렬이 유지되는 동안 range 검사를 생략하고,
			// 양 방향 모두 깨질 때만 range 검사(DeltaBlock/DoD/BitPacked 블록 경계).
			if (_buffer.Count > 0)
			{
				bool nextAscending = _isAscending && value >= _prev;
				bool nextDescending = _isDescending && value <= _prev;
				if (!nextAscending && !nextDescending)
				{
					bool deltaOk = newRange <= (UInt64)DeltaBlockMax;
					bool dodOk = prospectiveMaxAbsDod <= (UInt64)DeltaOfDeltaSelectThreshold;
					bool bitPackOk = newRange <= (UInt64)BitPackedBlockMax;
					if (!deltaOk && !dodOk && !bitPackOk)
					{
						return false;
					}
				}

				if (value < _prev) _isAscending = false;
				if (value > _prev) _isDescending = false;
				if (value != _prev) _isConstant = false;
				if (value <= _prev) _isStrictlyAscending = false;
				if (value >= _prev) _isStrictlyDescending = false;

				if (_isArithmetic)
				{
					Int64 diff = unchecked(value - _prev);
					if (_buffer.Count == 1)
					{
						_prevDiff = diff;
					}
					else if (diff != _prevDiff)
					{
						_isArithmetic = false;
					}
				}

				// prospective 값 커밋
				_maxAbsDod = prospectiveMaxAbsDod;
				_prevDelta = prospectiveDelta;
			}

			_prev = value;
			_min = newMin;
			_max = newMax;
			_buffer.Add(value);
			return true;
		}

		public void Feed(PbBlockedInteger proto, Int64 value)
		{
			if (!TryAdd(value))
			{
				Flush(proto);
				TryAdd(value);
			}
		}

		public void Flush(PbBlockedInteger proto)
		{
			if (_buffer.Count == 0) return;

			ReadOnlySpan<Int64> bufferSpan = CollectionsMarshal.AsSpan(_buffer);

			if (_isConstant && _buffer.Count >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeConstant(bufferSpan));
			}
			else if (_isArithmetic && _buffer.Count >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeArithmetic(bufferSpan));
			}
			else if (_isStrictlyAscending
				&& _buffer.Count >= BitmapBlockMinCount
				&& (_max - _min) <= BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeAscendingBitmap(bufferSpan));
			}
			else if (_isAscending)
			{
				proto.Blocks.Add(Encoders.EncodeAscending(bufferSpan));
			}
			else if (_isStrictlyDescending
				&& _buffer.Count >= BitmapBlockMinCount
				&& (_max - _min) <= BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeDescendingBitmap(bufferSpan));
			}
			else if (_isDescending)
			{
				proto.Blocks.Add(Encoders.EncodeDescending(bufferSpan));
			}
			else if (_maxAbsDod <= (UInt64)DeltaOfDeltaSelectThreshold
				&& _buffer.Count >= DeltaOfDeltaBlockMinCount)
			{
				// 비단조이고 delta-of-delta가 매우 작음 → DeltaOfDeltaBlock (DeltaBlock보다 더 효율적)
				proto.Blocks.Add(Encoders.EncodeDeltaOfDelta(bufferSpan));
			}
			else if (unchecked((UInt64)(_max - _min)) <= (UInt64)DeltaBlockMax)
			{
				proto.Blocks.Add(Encoders.EncodeDelta(bufferSpan, _min, _max));
			}
			else
			{
				// range > 16382 → BitPackedBlock
				proto.Blocks.Add(Encoders.EncodeBitPacked(bufferSpan, _min, _max));
			}

			Reset();
		}
	}
}
