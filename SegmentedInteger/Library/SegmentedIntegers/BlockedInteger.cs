using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Library.SegmentedIntegers;

using PbBlockedInteger = Pb.BlockedInteger;
using PbBlock = Pb.BlockedInteger.Types.Block;
using PbConstantBlock = Pb.BlockedInteger.Types.Block.Types.ConstantBlock;
using PbArithmeticBlock = Pb.BlockedInteger.Types.Block.Types.ArithmeticBlock;
using PbAscendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBitmapBlock;
using PbAscendingBlock = Pb.BlockedInteger.Types.Block.Types.AscendingBlock;
using PbDescendingBitmapBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBitmapBlock;
using PbDescendingBlock = Pb.BlockedInteger.Types.Block.Types.DescendingBlock;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;

/// <summary>
/// 임의의 Int64 시퀀스를 패턴 감지 블록 방식으로 압축:
/// - ConstantBlock:         모든 값 동일 (count ≥ 3) → (value, count)
/// - ArithmeticBlock:       등차 수열 (count ≥ 3) → (first, step, count)
/// - AscendingBitmapBlock:  strictly ascending, range ≤ 63, count ≥ 10 → first + uint64 bits
/// - AscendingBlock:        단조증가(비내림차순) → first + repeated uint64 diffs (≤8191개)
/// - DescendingBitmapBlock: strictly descending, range ≤ 63, count ≥ 10 → first + uint64 bits
/// - DescendingBlock:       단조감소(비오름차순) → first + repeated uint64 diffs (≤8191개)
/// - DeltaBlock:            range ≤ 16,382 → midpoint + sint64 deltas (≤2-byte zigzag)
/// <para>
/// 이 인코딩은 deterministic — 같은 입력은 항상 같은 byte 시퀀스를 생성합니다.
/// 블록 타입 dispatcher 우선순위·분기 조건을 변경하면 byte 호환성이 깨집니다.
/// </para>
/// </summary>
public static class BlockedInteger
{
	private const Int64 DeltaBlockMax = (Int64)PbDeltaBlock.Types.RangeLimit.Max; // 16382
	private const Int32 MaxBlockValues = 8192;
	private const Int32 RepeatableBlockMinCount = 3;
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
	/// 블록 구조의 무결성을 검증합니다.
	/// </summary>
	/// <param name="proto">검증할 프로토콜 버퍼</param>
	/// <param name="errors">발견된 에러 메시지 목록 (null인 경우 에러 메시지 생성 안 함)</param>
	/// <returns>유효하면 true, 그렇지 않으면 false</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static bool TryValidate(PbBlockedInteger proto, out List<string> errors)
	{
		ArgumentNullException.ThrowIfNull(proto);
		errors = [];

		for (Int32 blockIndex = 0; blockIndex < proto.Blocks.Count; ++blockIndex)
		{
			PbBlock block = proto.Blocks[blockIndex];
			ValidateBlock(block, blockIndex, errors);
		}

		return errors.Count == 0;
	}

	/// <summary>
	/// 블록 구조를 Int64 시퀀스로 디코딩합니다. 순서와 중복을 보존합니다.
	/// </summary>
	/// <remarks>신뢰된 입력 전용. 외부 proto는 Count 범위 등을 검증하지 않습니다.</remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static void Decode(PbBlockedInteger proto, out IReadOnlyList<Int64> integers)
	{
		ArgumentNullException.ThrowIfNull(proto);
		List<Int64> result = [];
		foreach (PbBlock block in proto.Blocks)
		{
			switch (block.BlockOneofCase)
			{
				case PbBlock.BlockOneofOneofCase.Constant:
					DecodeConstant(block.Constant, result);
					break;
				case PbBlock.BlockOneofOneofCase.Arithmetic:
					DecodeArithmetic(block.Arithmetic, result);
					break;
				case PbBlock.BlockOneofOneofCase.AscendingBitmap:
					DecodeAscendingBitmap(block.AscendingBitmap, result);
					break;
				case PbBlock.BlockOneofOneofCase.Ascending:
					DecodeAscending(block.Ascending, result);
					break;
				case PbBlock.BlockOneofOneofCase.DescendingBitmap:
					DecodeDescendingBitmap(block.DescendingBitmap, result);
					break;
				case PbBlock.BlockOneofOneofCase.Descending:
					DecodeDescending(block.Descending, result);
					break;
				case PbBlock.BlockOneofOneofCase.Delta:
					DecodeDelta(block.Delta, result);
					break;
				default:
					throw new InvalidOperationException($"Unknown block type: {block.BlockOneofCase}");
			}
		}
		integers = result;
	}

	private static PbBlock EncodeConstant(List<Int64> buffer) =>
		new()
		{
			Constant = new PbConstantBlock
			{
				Value = buffer[0],
				Count = buffer.Count
			}
		};

	private static PbBlock EncodeArithmetic(List<Int64> buffer) =>
		new()
		{
			Arithmetic = new PbArithmeticBlock
			{
				First = buffer[0],
				Step = unchecked(buffer[1] - buffer[0]),
				Count = buffer.Count
			}
		};

	private static PbBlock EncodeAscendingBitmap(List<Int64> buffer)
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

	private static PbBlock EncodeAscending(List<Int64> buffer)
	{
		PbAscendingBlock block = new() { First = buffer[0] };
		for (Int32 i = 1; i < buffer.Count; ++i)
		{
			block.Diffs.Add(unchecked((UInt64)(buffer[i] - buffer[i - 1])));
		}
		return new() { Ascending = block };
	}

	private static PbBlock EncodeDescendingBitmap(List<Int64> buffer)
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

	private static PbBlock EncodeDescending(List<Int64> buffer)
	{
		PbDescendingBlock block = new() { First = buffer[0] };
		for (Int32 i = 1; i < buffer.Count; ++i)
		{
			block.Diffs.Add(unchecked((UInt64)(buffer[i - 1] - buffer[i])));
		}
		return new() { Descending = block };
	}

	private static PbBlock EncodeDelta(List<Int64> buffer, Int64 min, Int64 max)
	{
		Int64 reference = min + (max - min) / 2;
		PbDeltaBlock block = new() { Reference = reference };
		foreach (Int64 value in buffer)
		{
			block.Deltas.Add(value - reference);
		}
		return new() { Delta = block };
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static UInt64 BuildAscendingBitmapBits(List<Int64> buffer, Int64 first)
	{
		UInt64 bits = 0UL;
		for (Int32 i = 1; i < buffer.Count; ++i)
		{
			bits |= 1UL << (Int32)(buffer[i] - first - 1);
		}
		return bits;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static UInt64 BuildDescendingBitmapBits(List<Int64> buffer, Int64 first)
	{
		UInt64 bits = 0UL;
		for (Int32 i = 1; i < buffer.Count; ++i)
		{
			bits |= 1UL << (Int32)(first - buffer[i] - 1);
		}
		return bits;
	}

	private static void DecodeConstant(PbConstantBlock block, List<Int64> output)
	{
		for (Int32 i = 0; i < block.Count; ++i)
		{
			output.Add(block.Value);
		}
	}

	private static void DecodeArithmetic(PbArithmeticBlock block, List<Int64> output)
	{
		Int64 current = block.First;
		for (Int32 i = 0; i < block.Count; ++i)
		{
			output.Add(current);
			current = unchecked(current + block.Step);
		}
	}

	private static void DecodeAscendingBitmap(PbAscendingBitmapBlock block, List<Int64> output)
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

	private static void DecodeAscending(PbAscendingBlock block, List<Int64> output)
	{
		Int64 current = block.First;
		output.Add(current);
		foreach (UInt64 diff in block.Diffs)
		{
			current = unchecked(current + (Int64)diff);
			output.Add(current);
		}
	}

	private static void DecodeDescendingBitmap(PbDescendingBitmapBlock block, List<Int64> output)
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

	private static void DecodeDescending(PbDescendingBlock block, List<Int64> output)
	{
		Int64 current = block.First;
		output.Add(current);
		foreach (UInt64 diff in block.Diffs)
		{
			current = unchecked(current - (Int64)diff);
			output.Add(current);
		}
	}

	private static void ValidateBlock(PbBlock block, Int32 blockIndex, List<string> errors)
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

	private static void ValidateConstantBlock(PbConstantBlock block, Int32 blockIndex, List<string> errors)
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
			errors.Add($"Block[{blockIndex}] (Constant): Count는 {MaxBlockValues} 이하여야 함 (현재: {block.Count})");
		}
	}

	private static void ValidateArithmeticBlock(PbArithmeticBlock block, Int32 blockIndex, List<string> errors)
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
			errors.Add($"Block[{blockIndex}] (Arithmetic): Count는 {MaxBlockValues} 이하여야 함 (현재: {block.Count})");
		}
	}

	private static void ValidateAscendingBitmapBlock(PbAscendingBitmapBlock block, Int32 blockIndex, List<string> errors)
	{
		if (block is null)
		{
			errors.Add($"Block[{blockIndex}] (AscendingBitmap): null");
			return;
		}

		Int32 setBitCount = BitOperations.PopCount(block.Bits) + 1;
		if (setBitCount < BitmapBlockMinCount)
		{
			errors.Add($"Block[{blockIndex}] (AscendingBitmap): 최소 {BitmapBlockMinCount}개 값 필요 (현재: {setBitCount})");
		}

		if (block.Bits > 0)
		{
			Int32 highestBit = 63 - BitOperations.LeadingZeroCount(block.Bits);
			if (highestBit >= BitmapBlockRange)
			{
				errors.Add($"Block[{blockIndex}] (AscendingBitmap): 범위는 {BitmapBlockRange} 이하여야 함 (현재 최대: {highestBit + 1})");
			}
		}
	}

	private static void ValidateAscendingBlock(PbAscendingBlock block, Int32 blockIndex, List<string> errors)
	{
		if (block is null)
		{
			errors.Add($"Block[{blockIndex}] (Ascending): null");
			return;
		}

		Int32 totalCount = block.Diffs.Count + 1;
		if (totalCount < 1)
		{
			errors.Add($"Block[{blockIndex}] (Ascending): 최소 1개 값 필요 (현재: {totalCount})");
		}

		if (totalCount > MaxBlockValues)
		{
			errors.Add($"Block[{blockIndex}] (Ascending): 최대 {MaxBlockValues}개 값 허용 (현재: {totalCount})");
		}

		if (block.Diffs.Count > MaxBlockValues)
		{
			errors.Add($"Block[{blockIndex}] (Ascending): Diffs는 {MaxBlockValues}개 이하여야 함 (현재: {block.Diffs.Count})");
		}
	}

	private static void ValidateDescendingBitmapBlock(PbDescendingBitmapBlock block, Int32 blockIndex, List<string> errors)
	{
		if (block is null)
		{
			errors.Add($"Block[{blockIndex}] (DescendingBitmap): null");
			return;
		}

		Int32 setBitCount = BitOperations.PopCount(block.Bits) + 1;
		if (setBitCount < BitmapBlockMinCount)
		{
			errors.Add($"Block[{blockIndex}] (DescendingBitmap): 최소 {BitmapBlockMinCount}개 값 필요 (현재: {setBitCount})");
		}

		if (block.Bits > 0)
		{
			Int32 highestBit = 63 - BitOperations.LeadingZeroCount(block.Bits);
			if (highestBit >= BitmapBlockRange)
			{
				errors.Add($"Block[{blockIndex}] (DescendingBitmap): 범위는 {BitmapBlockRange} 이하여야 함 (현재 최대: {highestBit + 1})");
			}
		}
	}

	private static void ValidateDescendingBlock(PbDescendingBlock block, Int32 blockIndex, List<string> errors)
	{
		if (block is null)
		{
			errors.Add($"Block[{blockIndex}] (Descending): null");
			return;
		}

		Int32 totalCount = block.Diffs.Count + 1;
		if (totalCount < 1)
		{
			errors.Add($"Block[{blockIndex}] (Descending): 최소 1개 값 필요 (현재: {totalCount})");
		}

		if (totalCount > MaxBlockValues)
		{
			errors.Add($"Block[{blockIndex}] (Descending): 최대 {MaxBlockValues}개 값 허용 (현재: {totalCount})");
		}

		if (block.Diffs.Count > MaxBlockValues)
		{
			errors.Add($"Block[{blockIndex}] (Descending): Diffs는 {MaxBlockValues}개 이하여야 함 (현재: {block.Diffs.Count})");
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
			errors.Add($"Block[{blockIndex}] (Delta): Deltas는 1개 이상이어야 함 (현재: {block.Deltas.Count})");
			return;
		}

		if (block.Deltas.Count > MaxBlockValues)
		{
			errors.Add($"Block[{blockIndex}] (Delta): Deltas는 {MaxBlockValues}개 이하여야 함 (현재: {block.Deltas.Count})");
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
			errors.Add($"Block[{blockIndex}] (Delta): 범위는 {DeltaBlockMax} 이하여야 함 (현재: {max - min})");
		}
	}

	private static void DecodeDelta(PbDeltaBlock block, List<Int64> output)
	{
		Debug.Assert(block.Deltas.Count > 0,
			"DeltaBlock.Deltas must not be empty; encoder never produces this state.");

		foreach (Int64 delta in block.Deltas)
		{
			output.Add(block.Reference + delta);
		}
	}

	private sealed class BlockAccumulator
	{
		private readonly List<Int64> _buffer = [];
		private Int64 _min;
		private Int64 _max;
		private Int64 _prev;
		private Int64 _prevDiff;
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
			// _prev는 의도적으로 초기화하지 않음.
			// TryAdd에서 _prev를 읽는 모든 코드는 _buffer.Count > 0 가드 내부에 있고,
			// 첫 TryAdd 호출은 이 가드를 건너뛴 뒤 가드 바깥의 _prev = value로 쓴다.
		}

		public bool TryAdd(Int64 value)
		{
			if (_buffer.Count >= MaxBlockValues) return false;

			Int64 newMin = Math.Min(_min, value);
			Int64 newMax = Math.Max(_max, value);

			// Ascending/DescendingBlock은 diff만 저장하므로 range 제약이 불필요.
			// 어느 한 방향이라도 정렬이 유지되는 동안 range 검사를 생략하고,
			// 양 방향 모두 깨질 때만 range 검사(delta 블록 경계).
			if (_buffer.Count > 0)
			{
				bool nextAscending = _isAscending && value >= _prev;
				bool nextDescending = _isDescending && value <= _prev;
				if (!nextAscending && !nextDescending &&
					unchecked((UInt64)(newMax - newMin)) > (UInt64)DeltaBlockMax)
				{
					return false;
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

			if (_isConstant && _buffer.Count >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(EncodeConstant(_buffer));
			}
			else if (_isArithmetic && _buffer.Count >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(EncodeArithmetic(_buffer));
			}
			else if (_isStrictlyAscending
				&& _buffer.Count >= BitmapBlockMinCount
				&& (_max - _min) <= BitmapBlockRange)
			{
				proto.Blocks.Add(EncodeAscendingBitmap(_buffer));
			}
			else if (_isAscending)
			{
				proto.Blocks.Add(EncodeAscending(_buffer));
			}
			else if (_isStrictlyDescending
				&& _buffer.Count >= BitmapBlockMinCount
				&& (_max - _min) <= BitmapBlockRange)
			{
				proto.Blocks.Add(EncodeDescendingBitmap(_buffer));
			}
			else if (_isDescending)
			{
				proto.Blocks.Add(EncodeDescending(_buffer));
			}
			else
			{
				proto.Blocks.Add(EncodeDelta(_buffer, _min, _max));
			}

			Reset();
		}
	}
}
