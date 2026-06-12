using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Library.SegmentedIntegers;

using PbArithmeticBlock = Pb.BlockedInteger.Types.Block.Types.ArithmeticBlock;
using PbAscendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock;
using PbAscendingBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBlock;
using PbBlock = Pb.BlockedInteger.Types.Block;
using PbConstantBlock = Pb.BlockedInteger.Types.Block.Types.ConstantBlock;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;
using PbDeltaOfDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock;
using PbDescendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBitmapBlock;
using PbDescendingBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBlock;

public static partial class BlockedInteger
{
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
			// 호출 전제: max - min ≤ DeltaBlockMax(8191) — TryAdd가 보장.
			// 따라서 아래 연산들은 overflow가 불가능하다 (delta ∈ [-4096, 4095]).
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
}
