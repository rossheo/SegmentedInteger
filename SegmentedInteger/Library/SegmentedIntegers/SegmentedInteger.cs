using Google.Protobuf;
using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Library.SegmentedIntegers;

using PbSegment = Pb.SegmentedInteger.Types.Segment;
using PbSegment2M = Pb.SegmentedInteger.Types.Segment.Types.Segment2M;
using PbSegment64 = Pb.SegmentedInteger.Types.Segment.Types.Segment64;
using PbSegmented = Pb.SegmentedInteger;

/// <summary>
/// 정렬된 비음수 Int64 시퀀스를 두 가지 세그먼트 방식으로 압축:
/// - Segment64: 연속된 64개 미만 범위를 비트맵으로 저장
/// - Segment2M: 최대 2,000,000 범위의 증분을 리스트로 저장
/// </summary>
public static class SegmentedInteger
{
	private const Int32 BitsPerByte = 8;
	private const Int32 PoolThreshold = 1024;

	private static readonly Int64 Segment64Max = (Int64)PbSegment64.Types.SegmentLimit.Max;
	private static readonly Int64 Segment2MMax = (Int64)PbSegment2M.Types.SegmentLimit.Max;

	private enum Mode { None, S64, S2M }

	/// <summary>
	/// SortedSet을 세그먼트 구조로 인코딩합니다.
	/// </summary>
	/// <param name="sorted">인코딩할 정렬된 집합 (중복 없음, 비음수)</param>
	/// <param name="proto">출력 세그먼트 구조</param>
	/// <exception cref="ArgumentNullException">sorted가 null인 경우</exception>
	public static void Encode(SortedSet<Int64> sorted, out PbSegmented proto)
	{
		ArgumentNullException.ThrowIfNull(sorted);

		if (sorted.Count == 0)
		{
			proto = new();
			return;
		}

		Int64[]? rented = sorted.Count >= PoolThreshold
			? ArrayPool<Int64>.Shared.Rent(sorted.Count)
			: null;

		Int64[] buffer = rented ?? new Int64[sorted.Count];

		try
		{
			sorted.CopyTo(buffer, 0);
			Encode(buffer.AsSpan(0, sorted.Count), out proto, useSortValidation: false);
		}
		finally
		{
			if (rented is not null)
			{
				ArrayPool<Int64>.Shared.Return(rented, clearArray: false);
			}
		}
	}

	/// <summary>
	/// 정렬된 배열을 세그먼트 구조로 인코딩합니다.
	/// </summary>
	/// <param name="sorted">인코딩할 정렬된 배열</param>
	/// <param name="proto">출력 세그먼트 구조</param>
	/// <exception cref="ArgumentException">값이 음수이거나 엄격한 오름차순이 아닌 경우</exception>
	public static void Encode(ReadOnlySpan<Int64> sorted, out PbSegmented proto,
		bool useSortValidation = true)
	{
		proto = new();
		if (sorted.Length == 0)
		{
			return;
		}

		ValidateNonNegative(sorted);

		if (useSortValidation)
		{
			ValidateSorted(sorted);
		}

		EncodeCore(sorted, proto);
	}

	/// <summary>
	/// 세그먼트 구조를 SortedSet으로 디코딩합니다.
	/// </summary>
	/// <param name="proto">디코딩할 세그먼트 구조</param>
	/// <param name="integers">출력 정렬된 집합</param>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static void Decode(PbSegmented proto, out SortedSet<Int64> integers)
	{
		ArgumentNullException.ThrowIfNull(proto);
		integers = [];

		foreach (PbSegment segment in proto.Segments)
		{
			if (segment.SegmentsOneofCase == PbSegment.SegmentsOneofOneofCase.Segment64)
			{
				Decode64(segment.Segment64, integers);
			}
			else if (segment.SegmentsOneofCase == PbSegment.SegmentsOneofOneofCase.Segment2M)
			{
				Decode2M(segment.Segment2M, integers);
			}
		}
	}

	private static void ValidateNonNegative(ReadOnlySpan<Int64> sorted)
	{
		Int64 prev = sorted[0];
		if (prev < 0)
		{
			throw new ArgumentException("Values must be non-negative.", nameof(sorted));
		}
	}

	private static void ValidateSorted(ReadOnlySpan<Int64> sorted)
	{
		Int64 prev = sorted[0];
		for (Int32 i = 1; i < sorted.Length; ++i)
		{
			Int64 current = sorted[i];
			if (current <= prev)
			{
				throw new ArgumentException("Input must be strictly ascending.", nameof(sorted));
			}

			prev = current;
		}
	}

	private static void EncodeCore(ReadOnlySpan<Int64> sorted, PbSegmented proto)
	{
		if (sorted.Length == 1)
		{
			// 단일 요소: S2M으로 추가
			proto.Segments.Add(new PbSegment
			{
				Segment2M = new PbSegment2M
				{
					Start = sorted[0]
				}
			});

			return;
		}

		EncodingContext context = default;
		Int32 lastIdx = sorted.Length - 1;

		for (Int32 idx = 0; idx < lastIdx; ++idx)
		{
			ProcessValue(ref context, sorted[idx], sorted[idx + 1], proto);
		}

		if (context.Mode != Mode.S2M)
		{
			context.FlushActive(proto);
			context.Mode = Mode.S2M;
			context.Initialize(sorted[lastIdx]);
		}
		else
		{
			context.AddValue(sorted[lastIdx], proto);
		}

		context.FlushActive(proto);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void ProcessValue(
		ref EncodingContext context,
		Int64 current,
		Int64 next,
		PbSegmented proto)
	{
		Mode desired = (next - current) < Segment64Max ? Mode.S64 : Mode.S2M;

		if (context.Mode != desired)
		{
			context.FlushActive(proto);
			context.Mode = desired;
			context.Initialize(current);
		}
		else
		{
			context.AddValue(current, proto);
		}

		if (context.WillOverflow(next))
		{
			context.FlushActive(proto);
			context.Mode = Mode.None;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void Decode64(PbSegment64 segment, SortedSet<Int64> set)
	{
		Int64 start = segment.Start;

		if (segment.Filled)
		{
			for (Int64 i = 0; i < Segment64Max; ++i)
			{
				set.Add(start + i);
			}

			return;
		}

		set.Add(start);

		if (segment.BitIncrements.Length > 0)
		{
			for (Int32 i = 0; i < segment.BitIncrements.Length; ++i)
			{
				Int32 value = segment.BitIncrements[i];
				if (value == 0)
				{
					continue;
				}

				Int64 baseValue = start + (i * BitsPerByte);

				while (value != 0)
				{
					Int32 trailingZeros = BitOperations.TrailingZeroCount(value);
					set.Add(baseValue + trailingZeros + 1);
					value &= value - 1;
				}
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void Decode2M(PbSegment2M segment, SortedSet<Int64> set)
	{
		Int64 start = segment.Start;
		set.Add(start);

		foreach (Int64 increment in segment.Increments)
		{
			set.Add(start + increment);
		}
	}

	private struct EncodingContext
	{
		public Mode Mode;
		private Segment64Builder _s64;
		private Segment2MBuilder _s2m;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Initialize(Int64 start)
		{
			if (Mode == Mode.S64) _s64.Init(start);
			else if (Mode == Mode.S2M) _s2m.Init(start);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AddValue(Int64 value, PbSegmented output)
		{
			if (Mode == Mode.S64) _s64.Add(value, output);
			else if (Mode == Mode.S2M) _s2m.Add(value, output);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> Mode switch
			{
				Mode.S64 => _s64.WillOverflow(nextValue),
				Mode.S2M => _s2m.WillOverflow(nextValue),
				_ => false
			};

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void FlushActive(PbSegmented output)
		{
			if (Mode == Mode.S64) _s64.Flush(output);
			else if (Mode == Mode.S2M) _s2m.Flush(output);

			Mode = Mode.None;
		}
	}

	private struct Segment64Builder
	{
		public bool Active;
		public Int64 Start;
		private UInt64 _bits;
		private Int32 _count;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Init(Int64 start)
		{
			Active = true;
			Start = start;
			_bits = 0UL;
			_count = 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(Int64 value, PbSegmented output)
		{
			Int64 increment = value - Start;
			if (increment == 0)
			{
				return;
			}

			if (increment >= Segment64Max)
			{
				Flush(output);
				Init(value);

				return;
			}

			_bits |= 1UL << ((Int32)increment - 1);

			if (++_count == Segment64Max - 1)
			{
				Flush(output);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> Active && (nextValue - Start) >= Segment64Max;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Flush(PbSegmented output)
		{
			if (!Active)
			{
				return;
			}

			PbSegment64 segment = new()
			{
				Start = Start
			};

			if (_count > 0)
			{
				if (_count == Segment64Max - 1)
				{
					segment.Filled = true;
				}
				else
				{
					Int32 highest = BitOperations.Log2(_bits);
					Int32 byteLen = (highest / BitsPerByte) + 1;

					Span<byte> buffer = stackalloc byte[8];
					BinaryPrimitives.WriteUInt64LittleEndian(buffer, _bits);

					segment.BitIncrements = ByteString.CopyFrom(buffer[..byteLen]);
				}
			}

			output.Segments.Add(new PbSegment
			{
				Segment64 = segment
			});

			Active = false;
		}
	}

	private struct Segment2MBuilder
	{
		public bool Active;
		public Int64 Start;
		public PbSegment2M Proto;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Init(Int64 start)
		{
			Active = true;
			Start = start;
			Proto = new PbSegment2M
			{
				Start = start
			};
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(Int64 value, PbSegmented output)
		{
			Int64 increment = value - Start;
			if (increment == 0)
			{
				return;
			}

			if (increment >= Segment2MMax)
			{
				Flush(output);
				Init(value);

				return;
			}

			Proto.Increments.Add(increment);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> Active && (nextValue - Start) >= Segment2MMax;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Flush(PbSegmented output)
		{
			if (!Active)
			{
				return;
			}

			output.Segments.Add(new PbSegment
			{
				Segment2M = Proto
			});

			Active = false;
			Proto = null!; // GC가 이전 객체를 수집 가능
		}
	}
}
