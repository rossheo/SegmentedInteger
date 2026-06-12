using System.Numerics;
using System.Runtime.InteropServices;
using Google.Protobuf.Collections;

namespace Library.SegmentedIntegers;

using PbArithmeticBlock = Pb.BlockedInteger.Types.Block.Types.ArithmeticBlock;
using PbBlock = Pb.BlockedInteger.Types.Block;
using PbBlockedInteger = Pb.BlockedInteger;
using PbConstantBlock = Pb.BlockedInteger.Types.Block.Types.ConstantBlock;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;
using PbDeltaOfDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock;

public static partial class BlockedInteger
{
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

		// 전체 블록 디코딩은 [0, count) 페이지 디코딩의 특수 경우로 위임한다.
		// 페이지 디코더가 동일한 SetCount + Span 기법을 사용하므로 성능 손실 없이 중복 구현을 없앤다.
		public static void DecodeBlock(PbBlock block, List<Int64> output) =>
			DecodeBlockPage(block, 0, GetBlockValueCount(block), output);

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
		// diffs는 인터페이스 대신 RepeatedField 구체 타입으로 받아 루프 내 가상 호출을 피한다.
		private static void DecodeMonotonicCorePage(Int64 first, RepeatedField<UInt64> diffs, Int64 sign,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = diffs.Count + 1;
			Int32 actualEnd = Math.Min(endOffset, totalCount);
			Int32 written = actualEnd - startOffset;
			if (written <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + written);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, written);

			Int32 destIndex = 0;
			if (startOffset == 0)
			{
				dest[destIndex++] = first;
			}

			Int64 current = first;
			Int32 skipCount = Math.Max(0, startOffset - 1);
			for (Int32 i = 0; i < skipCount; ++i)
			{
				current = unchecked(current + sign * (Int64)diffs[i]);
			}

			// written > 0이면 startOffset < totalCount이므로 i는 diffs.Count를 넘지 않는다
			// (최대 i = actualEnd - 2 ≤ diffs.Count - 1).
			for (Int32 i = skipCount; destIndex < written; ++i)
			{
				current = unchecked(current + sign * (Int64)diffs[i]);
				dest[destIndex++] = current;
			}
		}

		private static void DecodeDeltaOfDeltaPage(PbDeltaOfDeltaBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			Int32 totalCount = block.DeltaOfDeltas.Count + 2;
			Int32 actualEnd = Math.Min(endOffset, totalCount);
			Int32 written = actualEnd - startOffset;
			if (written <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + written);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, written);

			Int32 destIndex = 0;
			if (startOffset == 0)
			{
				dest[destIndex++] = block.First;
			}

			Int64 current = unchecked(block.First + block.FirstDelta);
			if (startOffset <= 1 && destIndex < written)
			{
				dest[destIndex++] = current;
			}

			Int64 prevDelta = block.FirstDelta;

			Int32 skipCount = Math.Max(0, startOffset - 2);
			for (Int32 i = 0; i < skipCount; ++i)
			{
				Int64 dod = block.DeltaOfDeltas[i];
				prevDelta = unchecked(prevDelta + dod);
				current = unchecked(current + prevDelta);
			}

			// DeltaOfDeltas[i]가 만드는 값의 출력 위치는 i+2 (인덱스 0,1은 first, first+firstDelta).
			// written > 0이면 startOffset < totalCount이므로 i는 DeltaOfDeltas.Count를 넘지 않는다
			// (최대 i = actualEnd - 3 ≤ DeltaOfDeltas.Count - 1).
			for (Int32 i = skipCount; destIndex < written; ++i)
			{
				Int64 dod = block.DeltaOfDeltas[i];
				prevDelta = unchecked(prevDelta + dod);
				current = unchecked(current + prevDelta);
				dest[destIndex++] = current;
			}
		}

		private static void DecodeDeltaPage(PbDeltaBlock block,
			Int32 startOffset, Int32 endOffset, List<Int64> output)
		{
			// Constant/Arithmetic Page와 동일하게 [startOffset, actualEnd) 범위를 클램프한다.
			// Delta는 각 값이 reference+delta[i]로 인덱스가 곧 위치이므로 first 별도 처리가 없다.
			Int32 actualEnd = Math.Min(endOffset, block.Deltas.Count);
			Int32 written = actualEnd - startOffset;
			if (written <= 0) return;

			Int32 start = output.Count;
			CollectionsMarshal.SetCount(output, start + written);
			Span<Int64> dest = CollectionsMarshal.AsSpan(output).Slice(start, written);

			Int64 reference = block.Reference;
			for (Int32 i = 0; i < written; ++i)
			{
				dest[i] = unchecked(reference + block.Deltas[startOffset + i]);
			}
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
	}
}
