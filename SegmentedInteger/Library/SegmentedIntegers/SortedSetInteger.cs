using Google.Protobuf;
using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Library.SegmentedIntegers;

using PbChunk = Pb.SortedSetInteger.Types.Chunk;
using PbBitmapChunk = Pb.SortedSetInteger.Types.Chunk.Types.BitmapChunk;
using PbIncrementChunk = Pb.SortedSetInteger.Types.Chunk.Types.IncrementChunk;
using PbSortedSetInteger = Pb.SortedSetInteger;

/// <summary>
/// 정렬된 비음수 Int64 시퀀스를 두 가지 청크 방식으로 압축:
/// - BitmapChunk:    범위 &lt;64인 값들을 64비트 비트맵으로 저장
/// - IncrementChunk: 최대 2,000,000 범위의 증분을 리스트로 저장
/// <para>
/// 이 인코딩은 deterministic — 같은 입력은 항상 같은 byte 시퀀스를 생성합니다.
/// 청크 타입 결정 로직을 변경하면 byte 호환성이 깨집니다.
/// </para>
/// </summary>
public static class SortedSetInteger
{
	private const Int32 BitsPerByte = 8;
	private const Int32 BitmapBufferSize = sizeof(UInt64);
	private const Int32 PoolThreshold = 1024;
	private const Int32 BitmapChunkMaxIncrements = 63; // = (Int32)BitmapChunkMax - 1

	private const Int64 BitmapChunkMax = (Int64)PbBitmapChunk.Types.ChunkLimit.Max;
	private const Int64 IncrementChunkMax = (Int64)PbIncrementChunk.Types.ChunkLimit.Max;

	private enum Mode { None, Bitmap, Increment }

	/// <summary>
	/// SortedSet을 청크 구조로 인코딩합니다.
	/// </summary>
	/// <param name="sorted">인코딩할 정렬된 집합 (중복 없음, 비음수)</param>
	/// <param name="proto">출력 청크 구조</param>
	/// <exception cref="ArgumentNullException">sorted가 null인 경우</exception>
	public static void Encode(SortedSet<Int64> sorted, out PbSortedSetInteger proto)
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
	/// 정렬된 배열을 청크 구조로 인코딩합니다.
	/// </summary>
	/// <param name="sorted">인코딩할 정렬된 배열</param>
	/// <param name="proto">출력 청크 구조</param>
	/// <param name="useSortValidation">
	/// false로 설정 시 입력이 엄격한 오름차순이고 비음수임을 호출자가 보장해야 합니다.
	/// </param>
	/// <exception cref="ArgumentException">값이 음수이거나 엄격한 오름차순이 아닌 경우</exception>
	public static void Encode(ReadOnlySpan<Int64> sorted, out PbSortedSetInteger proto,
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
	/// 청크 구조를 SortedSet으로 디코딩합니다.
	/// </summary>
	/// <param name="proto">디코딩할 청크 구조</param>
	/// <param name="integers">출력 정렬된 집합</param>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static void Decode(PbSortedSetInteger proto, out SortedSet<Int64> integers)
	{
		ArgumentNullException.ThrowIfNull(proto);
		integers = [];

		foreach (PbChunk chunk in proto.Chunks)
		{
			if (chunk.ChunksOneofCase == PbChunk.ChunksOneofOneofCase.BitmapChunk)
			{
				DecodeBitmap(chunk.BitmapChunk, integers);
			}
			else if (chunk.ChunksOneofCase == PbChunk.ChunksOneofOneofCase.IncrementChunk)
			{
				DecodeIncrement(chunk.IncrementChunk, integers);
			}
		}
	}

	// 정렬된 입력 가정 하에 최솟값(첫 원소)만 검사.
	// useSortValidation=false 시 호출자가 전체 범위 비음수 보장 책임.
	private static void ValidateNonNegative(ReadOnlySpan<Int64> sorted)
	{
		if (sorted[0] < 0)
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

	private static void EncodeCore(ReadOnlySpan<Int64> sorted, PbSortedSetInteger proto)
	{
		if (sorted.Length == 1)
		{
			// 단일 요소: IncrementChunk로 추가
			proto.Chunks.Add(new PbChunk
			{
				IncrementChunk = new PbIncrementChunk
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

		// 마지막 요소: 활성 청크가 있으면 그대로 추가, 없으면 새 IncrementChunk 생성
		if (context.Mode == Mode.None)
		{
			context.Mode = Mode.Increment;
			context.BeginChunk(sorted[lastIdx]);
		}
		else
		{
			context.AddValue(sorted[lastIdx]);
		}

		context.Flush(proto);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void ProcessValue(
		ref EncodingContext context,
		Int64 current,
		Int64 next,
		PbSortedSetInteger proto)
	{
		// gap < BitmapChunkMax(64)이면 비트맵 방식, 이상이면 증분 리스트 방식
		Mode desired = (next - current) < BitmapChunkMax ? Mode.Bitmap : Mode.Increment;

		if (context.Mode != desired)
		{
			context.Flush(proto);
			context.Mode = desired;
			context.BeginChunk(current);
		}
		else
		{
			context.AddValue(current);
		}

		if (context.WillOverflow(next))
		{
			context.Flush(proto);
		}
	}

	private static void DecodeBitmap(PbBitmapChunk chunk, SortedSet<Int64> set)
	{
		Int64 start = chunk.Start;

		if (chunk.Filled)
		{
			for (Int64 i = 0; i < BitmapChunkMax; ++i)
			{
				set.Add(start + i);
			}

			return;
		}

		set.Add(start);

		for (Int32 byteIdx = 0; byteIdx < chunk.BitIncrements.Length; ++byteIdx)
		{
			Int32 bits = chunk.BitIncrements[byteIdx];
			if (bits == 0)
			{
				continue;
			}

			Int64 baseValue = start + (byteIdx * BitsPerByte);

			while (bits != 0)
			{
				Int32 trailingZeros = BitOperations.TrailingZeroCount(bits);
				set.Add(baseValue + trailingZeros + 1);
				bits &= bits - 1;
			}
		}
	}

	private static void DecodeIncrement(PbIncrementChunk chunk, SortedSet<Int64> set)
	{
		Int64 start = chunk.Start;
		set.Add(start);

		// 각 increment는 start 기준 절대 오프셋 (value - start). 누적 합산 아님.
		foreach (Int64 increment in chunk.Increments)
		{
			set.Add(start + increment);
		}
	}

	// 상태 머신:
	//   None → Mode 설정 + BeginChunk → AddValue* (반복)
	//        → WillOverflow가 true이면 Flush → None 복귀
	// 주의: BeginChunk 없이 AddValue 호출 금지.
	//       WillOverflow는 Mode != None일 때만 의미 있는 값 반환.
	private struct EncodingContext
	{
		public Mode Mode;
		private BitmapChunkBuilder _bitmap;
		private IncrementChunkBuilder _increment;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void BeginChunk(Int64 start)
		{
			if (Mode == Mode.Bitmap) _bitmap.Init(start);
			else if (Mode == Mode.Increment) _increment.Init(start);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AddValue(Int64 value)
		{
			if (Mode == Mode.Bitmap) _bitmap.Add(value);
			else if (Mode == Mode.Increment) _increment.Add(value);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> Mode switch
			{
				Mode.Bitmap => _bitmap.WillOverflow(nextValue),
				Mode.Increment => _increment.WillOverflow(nextValue),
				_ => false
			};

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Flush(PbSortedSetInteger output)
		{
			if (Mode == Mode.Bitmap) _bitmap.Flush(output);
			else if (Mode == Mode.Increment) _increment.Flush(output);

			Mode = Mode.None;
		}
	}

	private struct BitmapChunkBuilder
	{
		private Int64 _start;
		private UInt64 _bits;
		private Int32 _count;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Init(Int64 start)
		{
			_start = start;
			_bits = 0UL;
			_count = 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(Int64 value)
		{
			Int64 increment = value - _start;
			if (increment == 0)
			{
				return;
			}

			_bits |= 1UL << ((Int32)increment - 1);
			++_count;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> (nextValue - _start) >= BitmapChunkMax;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Flush(PbSortedSetInteger output)
		{
			PbBitmapChunk chunk = new()
			{
				Start = _start
			};

			if (_count > 0)
			{
				if (_count == BitmapChunkMaxIncrements)
				{
					chunk.Filled = true;
				}
				else
				{
					Int32 highest = BitOperations.Log2(_bits);
					Int32 byteLen = (highest / BitsPerByte) + 1;

					Span<byte> buffer = stackalloc byte[BitmapBufferSize];
					BinaryPrimitives.WriteUInt64LittleEndian(buffer, _bits);

					chunk.BitIncrements = ByteString.CopyFrom(buffer[..byteLen]);
				}
			}

			output.Chunks.Add(new PbChunk
			{
				BitmapChunk = chunk
			});
		}
	}

	private struct IncrementChunkBuilder
	{
		private Int64 _start;
		private PbIncrementChunk _proto;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Init(Int64 start)
		{
			_start = start;
			_proto = new PbIncrementChunk
			{
				Start = start
			};
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(Int64 value)
		{
			Int64 increment = value - _start;
			if (increment == 0)
			{
				return;
			}

			_proto.Increments.Add(increment);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public readonly bool WillOverflow(Int64 nextValue)
			=> (nextValue - _start) >= IncrementChunkMax;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Flush(PbSortedSetInteger output)
		{
			output.Chunks.Add(new PbChunk
			{
				IncrementChunk = _proto
			});
		}
	}
}
