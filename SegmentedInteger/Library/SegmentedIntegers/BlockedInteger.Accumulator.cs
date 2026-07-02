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

			// Suffix 분리: 누적 구간에 등차/동일값 run이 형성되면,
			// run이 임계 길이에 도달한 시점에 head를 먼저 emit하고 run을 새 버퍼로 옮긴다.
			// run은 새 버퍼에서 Constant/Arithmetic 후보로 자연스럽게 이어진다.
			// 임계: 비단조 구간은 PrefixSplitMinCount(5). 단조 구간은 run을 분리하지 않아도
			// diff당 1~2바이트로 인코딩되므로 MonotonicRunSplitMinCount(16)부터 이득이다.
			// (== 비교: run 내 delta는 모두 같아 run 도중 단조성 전이가 일어날 수 없고
			//  임계도 바뀌지 않으므로, 한 run당 정확히 한 번만 트리거된다)
			Int32 splitThreshold =
				(_flags & (SequenceFlags.Ascending | SequenceFlags.Descending)) == 0
					? PrefixSplitMinCount
					: MonotonicRunSplitMinCount;
			if (_trailingRunLength == splitThreshold && _bufferCount > _trailingRunLength)
			{
				SplitTrailingRun(proto);
			}
		}

		// buffer를 head와 trailing run으로 나눈다. head는 Flush와 동일한 prefix-split
		// 경로로 emit하여 head 내부의 Constant/Arithmetic prefix(예: 직전 plateau)도
		// 같은 기준으로 분리되게 하고, run은 복사 후 ReFeed하여 새 버퍼에서
		// Constant/Arithmetic 후보로 잇는다.
		// _flags/_min/_max/_maxAbsDod는 run을 포함한 전체 버퍼 기준의 보수적 근사지만
		// head는 그 부분집합이므로 (range ≤ 8191 ∨ maxAbsDod ≤ 63 불변 조건 포함)
		// 항상 유효한 블록이 만들어진다.
		private void SplitTrailingRun(PbBlockedInteger proto)
		{
			Int32 headCount = _bufferCount - _trailingRunLength;

			// run 길이는 트리거 시점의 임계값으로 고정되므로 stackalloc이 안전하다.
			Debug.Assert(_trailingRunLength <= MonotonicRunSplitMinCount,
				"run 길이가 분리 임계를 초과 — Feed 트리거 조건 위반");
			Span<Int64> run = stackalloc Int64[MonotonicRunSplitMinCount];
			run = run.Slice(0, _trailingRunLength);
			new ReadOnlySpan<Int64>(_buffer, headCount, _trailingRunLength).CopyTo(run);

			// head만 남기고 Flush한다. run의 첫 값이 head의 Constant/Arithmetic prefix를
			// 한 칸 연장한 경우 prefix 카운트가 headCount를 1 초과할 수 있으므로 클램프한다
			// (prefix 성립 구간의 부분집합인 head 앞부분도 당연히 성립하므로 안전).
			_bufferCount = headCount;
			_constantPrefixCount = Math.Min(_constantPrefixCount, headCount);
			_arithmeticPrefixCount = Math.Min(_arithmeticPrefixCount, headCount);
			Flush(proto);

			ReFeed(run);
		}

		public void Flush(PbBlockedInteger proto)
		{
			// Prefix split 후 suffix를 재분석해야 할 수 있으므로 루프로 처리.
			// 매 반복은 prefix를 emit하거나 전체 버퍼를 emit하고 종료.
			while (_bufferCount > 0)
			{
				ReadOnlySpan<Int64> bufferSpan = new(_buffer, 0, _bufferCount);

				// Prefix 분리 임계: 비단조 구간은 PrefixSplitMinCount(5).
				// 단조 구간은 prefix를 분리하지 않아도 diff당 1~2바이트로 인코딩되므로
				// 블록 고정 비용을 확실히 상회하는 MonotonicRunSplitMinCount(16)부터 이득이다.
				Int32 prefixSplitThreshold =
					(_flags & (SequenceFlags.Ascending | SequenceFlags.Descending)) == 0
						? PrefixSplitMinCount
						: MonotonicRunSplitMinCount;

				// Constant prefix 분리 (전체가 Constant면 prefix 카운트가 0이므로 자연 제외).
				if (_constantPrefixCount >= prefixSplitThreshold)
				{
					proto.Blocks.Add(Encoders.EncodeConstant(bufferSpan.Slice(0, _constantPrefixCount)));
					ReFeed(bufferSpan.Slice(_constantPrefixCount));
					continue;
				}

				// Arithmetic prefix 분리. 전체 버퍼가 Bitmap 후보(strictly 단조 + range ≤ 63)라면
				// 단일 Bitmap 블록이 분리보다 작으므로 분리하지 않는다.
				bool bitmapEligible =
					(_flags & (SequenceFlags.StrictlyAscending | SequenceFlags.StrictlyDescending)) != 0
					&& _bufferCount >= BitmapBlockMinCount
					&& unchecked((UInt64)(_max - _min)) <= (UInt64)BitmapBlockRange;
				if (_arithmeticPrefixCount >= prefixSplitThreshold
					&& (_flags & SequenceFlags.Constant) == 0
					&& !bitmapEligible)
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
