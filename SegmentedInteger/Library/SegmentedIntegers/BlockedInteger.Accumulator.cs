using System.Buffers;
using System.Diagnostics;

namespace Library.SegmentedIntegers;

using PbBlockedInteger = Pb.BlockedInteger;

public static partial class BlockedInteger
{
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
		private Int32 _trailingRunLength;     // 연속 delta가 동일한 trailing run의 값 개수 (suffix 분리용)

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
			_trailingRunLength = 0;
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
				// 연속 delta가 이전과 같으면 trailing 등차 run이 이어진다(동일값 run은 step 0인 특수 경우).
				// 다르면 직전 값과 현재 값이 새 run의 시작이므로 2로 리셋.
				_trailingRunLength = _bufferCount >= 2 && prospectiveDelta == _prevDelta
					? _trailingRunLength + 1
					: 2;
				_maxAbsDod = prospectiveMaxAbsDod;
				_prevDelta = prospectiveDelta;
			}
			else
			{
				_trailingRunLength = 1;
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

			// Suffix 분리: 비단조(Delta/DoD) 구간에 등차/동일값 run이 형성되면,
			// run이 임계 길이에 도달한 시점에 head를 먼저 emit하고 run을 새 버퍼로 옮긴다.
			// run은 새 버퍼에서 Constant/Arithmetic 후보로 자연스럽게 이어진다.
			// (== 비교: run은 단조 구간이므로 한 run당 정확히 한 번만 트리거된다)
			if (_trailingRunLength == PrefixSplitMinCount
				&& _bufferCount > _trailingRunLength
				&& (_flags & (SequenceFlags.Ascending | SequenceFlags.Descending)) == 0)
			{
				SplitTrailingRun(proto);
			}
		}

		// head(buffer 앞부분)를 현재 누적 통계로 emit하고 trailing run을 ReFeed한다.
		// 비단조 모드에서만 호출되므로 head는 SelectAndEmitBlock의 DoD/Delta 분기로 떨어진다.
		// _min/_max/_maxAbsDod는 run을 포함한 전체 버퍼 기준이지만 head 값들은 그 부분집합이므로
		// (range ≤ 8191 ∨ maxAbsDod ≤ 63 불변 조건 포함) 항상 유효한 블록이 만들어진다.
		private void SplitTrailingRun(PbBlockedInteger proto)
		{
			ReadOnlySpan<Int64> bufferSpan = new(_buffer, 0, _bufferCount);
			Int32 headCount = _bufferCount - _trailingRunLength;

			SelectAndEmitBlock(proto, bufferSpan.Slice(0, headCount));
			ReFeed(bufferSpan.Slice(headCount));
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
		// 쓰고 높은 인덱스에서 읽는다. 호출자(Flush의 prefix 분리, SplitTrailingRun)가
		// suffix 시작 오프셋 ≥ 1을 보장하므로 읽기 인덱스가 항상 쓰기 인덱스보다 앞에 있어
		// aliasing이 없다.
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

		// bufferSpan은 전체 버퍼이거나(Flush 경로) head 부분(SplitTrailingRun 경로)일 수 있으므로
		// 개수 판정은 _bufferCount가 아닌 bufferSpan.Length를 사용한다.
		private void SelectAndEmitBlock(PbBlockedInteger proto, ReadOnlySpan<Int64> bufferSpan)
		{
			// _max - _min이 Int64 범위를 초과할 수 있으므로 UInt64로 비교한다.
			// unchecked 캐스트는 비트 패턴을 그대로 유지해 올바른 UInt64 거리를 만든다.
			UInt64 valueRange = unchecked((UInt64)(_max - _min));

			if ((_flags & SequenceFlags.Constant) != 0
				&& bufferSpan.Length >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeConstant(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Arithmetic) != 0
				&& bufferSpan.Length >= RepeatableBlockMinCount)
			{
				proto.Blocks.Add(Encoders.EncodeArithmetic(bufferSpan));
			}
			else if ((_flags & SequenceFlags.StrictlyAscending) != 0
				&& bufferSpan.Length >= BitmapBlockMinCount
				&& valueRange <= (UInt64)BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeAscendingBitmap(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Ascending) != 0)
			{
				proto.Blocks.Add(Encoders.EncodeAscending(bufferSpan));
			}
			else if ((_flags & SequenceFlags.StrictlyDescending) != 0
				&& bufferSpan.Length >= BitmapBlockMinCount
				&& valueRange <= (UInt64)BitmapBlockRange)
			{
				proto.Blocks.Add(Encoders.EncodeDescendingBitmap(bufferSpan));
			}
			else if ((_flags & SequenceFlags.Descending) != 0)
			{
				proto.Blocks.Add(Encoders.EncodeDescending(bufferSpan));
			}
			else if (bufferSpan.Length <= 2)
			{
				// SplitTrailingRun의 head는 1~2개까지 짧아질 수 있고, 이때 flags는
				// 전체 버퍼 기준이라 위의 단조 분기에 걸리지 않는다. 1~2개 값은 그 자체로
				// 항상 단조이므로 Ascending/Descending으로 emit한다. Delta로 보내면
				// 극값 wrap aliasing 케이스에서 range > 8191인 블록이 만들어져
				// TryValidate가 실패할 수 있고, 정상 케이스에서도 Asc/Desc 쪽이 더 작다.
				proto.Blocks.Add(bufferSpan.Length == 2 && bufferSpan[0] > bufferSpan[1]
					? Encoders.EncodeDescending(bufferSpan)
					: Encoders.EncodeAscending(bufferSpan));
			}
			else if (_maxAbsDod <= (UInt64)DeltaOfDeltaSelectThreshold
				&& bufferSpan.Length >= DeltaOfDeltaBlockMinCount)
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
