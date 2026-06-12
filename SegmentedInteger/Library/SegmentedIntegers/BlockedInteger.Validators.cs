using System.Numerics;

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

		// Ascending/DescendingBitmapBlock의 공통 검증 로직.
		// BitmapBlockMinCount(8)는 encoder의 블록 선택 휴리스틱일 뿐 디코딩 가능성과 무관하므로
		// 검증하지 않는다 (Constant/Arithmetic의 Count 1~2 허용과 동일한 정책).
		// 범위(BitmapBlockRange)는 포맷 스펙이므로 강제한다.
		// ascending: 디코딩 값이 first + offset, descending: first - offset (offset = 1..rangeSpan)
		private static void ValidateBitmapBlockCore(UInt64 bits, Int64 first, bool ascending,
			Int32 blockIndex, string label, List<string> errors)
		{
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
				// first ± rangeSpan이 Int64 범위를 벗어나면 디코딩 값이 wrap된다.
				// encoder는 실존하는 Int64 값에서만 블록을 만들므로 이 상태를 생성할 수 없다.
				else if (ascending
					? first > Int64.MaxValue - rangeSpan
					: first < Int64.MinValue + rangeSpan)
				{
					errors.Add($"Block[{blockIndex}] ({label}): First({first}) ± 범위({rangeSpan})가" +
						$" Int64 범위를 벗어남");
				}
			}
		}

		// Ascending/DescendingBlock의 공통 검증 로직
		private static void ValidateMonotonicBlockCore(Int32 diffsCount, Int32 blockIndex, string label,
			List<string> errors)
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
			ValidateBitmapBlockCore(block.Bits, block.First, ascending: true,
				blockIndex, "AscendingBitmap", errors);
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
			ValidateBitmapBlockCore(block.Bits, block.First, ascending: false,
				blockIndex, "DescendingBitmap", errors);
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

			// DeltaOfDeltas가 비어 있으면 first + firstDelta 두 값(count 2)을 표현한다.
			// encoder는 count ≥ 3에서만 emit하지만 count 2도 디코딩 가능하므로
			// 외부 도구가 만든 proto를 받아들이기 위해 허용한다.
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
				// dod == Int64.MinValue일 때 -dod는 Int64로 표현 불가(wrap하면 다시 MinValue).
				// ComputeProspectiveMaxAbsDod와 동일하게 UInt64 캐스트로 2^63까지 정확히 표현한다.
				UInt64 absDod = unchecked(dod >= 0 ? (UInt64)dod : (UInt64)(-dod));
				if (absDod > (UInt64)DeltaOfDeltaBlockMax)
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
}
