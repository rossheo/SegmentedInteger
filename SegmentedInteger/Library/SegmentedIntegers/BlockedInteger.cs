namespace Library.SegmentedIntegers;

using PbBlock = Pb.BlockedInteger.Types.Block;
using PbBlockedInteger = Pb.BlockedInteger;
using PbDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaBlock;
using PbDeltaOfDeltaBlock = Pb.BlockedInteger.Types.Block.Types.DeltaOfDeltaBlock;

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
/// Constant/Arithmetic 접두부는 임계 길이 이상이면 먼저 분리하여 emit하며(prefix 분리),
/// 누적 중 등차/동일값 run이 임계 길이에 도달하면 그 앞부분을 분리하여 emit합니다(suffix 분리).
/// 임계 길이는 비단조 구간 5개, 단조 구간 16개입니다(단조 구간은 diff 인코딩이 이미 저렴하므로
/// 블록 고정 비용을 상회하는 길이부터 분리).
/// </para>
/// <para>
/// 모든 public 메서드는 호출별로 상태를 갖지 않으므로(Encode는 호출마다 새 BlockAccumulator를 생성),
/// 동일 proto를 동시에 변경하지 않는 한 스레드로부터 안전하게 호출할 수 있습니다.
/// </para>
/// <para>
/// 구현은 역할별 partial 파일로 나뉩니다: Encoders / Decoders / Validators /
/// Accumulator(BlockAccumulator) / Statistics(CompressionStatistics).
/// </para>
/// </summary>
public static partial class BlockedInteger
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
	// 단조 버퍼에서 trailing run 분리 임계. 단조 구간의 run은 분리하지 않아도
	// diff당 1~2바이트로 인코딩되므로, Constant/Arithmetic 블록 고정 비용(~10바이트)과
	// 블록 경계 오버헤드를 확실히 상회하는 길이부터 분리한다.
	private const Int32 MonotonicRunSplitMinCount = 16;
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
	/// 검증하지 않습니다. 신뢰할 수 없는 외부 입력은 먼저
	/// <see cref="TryValidate(PbBlockedInteger, Int64, out List{String})"/>로
	/// (총 개수 상한과 함께) 검증하세요.
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
			Decoders.DecodeBlock(block, result);
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
	public static Int64 GetPageCount(PbBlockedInteger proto, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageSize <= 0)
			throw new ArgumentOutOfRangeException(nameof(pageSize),
				$"pageSize({pageSize})는 0보다 커야 함");

		Int64 totalValueCount = Decoders.GetTotalValueCount(proto);

		if (totalValueCount == 0)
			return 0;

		Int64 pageCount = (totalValueCount + pageSize - 1) / pageSize;
		return pageCount;
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
	/// 신뢰할 수 없는 외부 입력은 먼저
	/// <see cref="TryValidate(PbBlockedInteger, Int64, out List{String})"/>로
	/// (총 개수 상한과 함께) 검증하세요.
	/// </remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentOutOfRangeException">pageIndex &lt; 0 또는 pageSize &lt;= 0인 경우</exception>
	public static IReadOnlyList<Int64> DecodePage(PbBlockedInteger proto,
		Int64 pageIndex, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageIndex < 0)
			throw new ArgumentOutOfRangeException(nameof(pageIndex),
				$"pageIndex({pageIndex})는 0 이상이어야 함");
		if (pageSize <= 0)
			throw new ArgumentOutOfRangeException(nameof(pageSize),
				$"pageSize({pageSize})는 0보다 커야 함");

		// pageIndex * pageSize + pageSize가 Int64 범위를 넘는 위치에는 어떤 데이터도
		// 도달할 수 없으므로, 문서화된 계약(범위 밖 → 빈 목록)에 따라 빈 결과를 반환한다.
		if (pageIndex > (Int64.MaxValue - pageSize) / pageSize)
			return [];

		Int64 startIndex = pageIndex * pageSize;
		Int64 endIndex = startIndex + pageSize;

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
	/// 전체 데이터를 pageSize 단위로 순차 디코딩하는 지연 열거를 반환합니다.
	/// 마지막 페이지는 pageSize보다 작을 수 있습니다.
	/// </summary>
	/// <param name="proto">디코딩할 프로토콜 버퍼</param>
	/// <param name="pageSize">페이지 크기 (값의 개수)</param>
	/// <returns>페이지 단위의 디코딩된 값 목록 열거 (데이터가 없으면 빈 열거)</returns>
	/// <remarks>
	/// <see cref="DecodePage"/>를 페이지 번호로 반복 호출하면 호출마다 블록 목록을 처음부터
	/// 스캔하지만, 이 메서드는 블록을 한 번만 순회하므로 전체 순차 소비에 효율적입니다.
	/// 신뢰된 입력 전용. 각 블록의 내부 invariant를 검증하지 않습니다.
	/// 신뢰할 수 없는 외부 입력은 먼저
	/// <see cref="TryValidate(PbBlockedInteger, Int64, out List{String})"/>로
	/// (총 개수 상한과 함께) 검증하세요.
	/// </remarks>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentOutOfRangeException">pageSize &lt;= 0인 경우</exception>
	public static IEnumerable<IReadOnlyList<Int64>> DecodePages(PbBlockedInteger proto, Int32 pageSize)
	{
		ArgumentNullException.ThrowIfNull(proto);
		if (pageSize <= 0)
			throw new ArgumentOutOfRangeException(nameof(pageSize),
				$"pageSize({pageSize})는 0보다 커야 함");

		return DecodePagesCore(proto, pageSize);
	}

	// 인수 검증을 즉시 수행하기 위해 iterator 본문을 분리한다 (deferred execution 함정 방지).
	private static IEnumerable<IReadOnlyList<Int64>> DecodePagesCore(PbBlockedInteger proto,
		Int32 pageSize)
	{
		List<Int64> page = new(pageSize);

		foreach (PbBlock block in proto.Blocks)
		{
			Int32 blockValueCount = Decoders.GetBlockValueCount(block);
			Int32 offset = 0;

			while (offset < blockValueCount)
			{
				Int32 take = Math.Min(pageSize - page.Count, blockValueCount - offset);
				Decoders.DecodeBlockPage(block, offset, offset + take, page);
				offset += take;

				if (page.Count == pageSize)
				{
					yield return page;
					page = new(pageSize);
				}
			}
		}

		if (page.Count > 0)
		{
			yield return page;
		}
	}

	/// <summary>
	/// 블록 구조의 무결성을 검증합니다.
	/// </summary>
	/// <remarks>
	/// 이 오버로드의 전체 값 개수 상한은 <see cref="Int32.MaxValue"/>(List 물리
	/// 한계)뿐입니다. 네트워크 등 신뢰할 수 없는 경로로 받은 proto에는 상한을
	/// 명시하는 <see cref="TryValidate(PbBlockedInteger, Int64, out List{String})"/>를
	/// 사용하세요 - 개별 블록 검증만으로는 블록 *개수*가 제한되지 않아 작은
	/// 페이로드가 거대한 디코딩 할당으로 증폭될 수 있습니다.
	/// </remarks>
	/// <param name="proto">검증할 프로토콜 버퍼</param>
	/// <param name="errors">발견된 에러 메시지 목록</param>
	/// <returns>유효하면 true, 그렇지 않으면 false</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	public static bool TryValidate(PbBlockedInteger proto, out List<String> errors)
		=> TryValidate(proto, Int32.MaxValue, out errors);

	/// <summary>
	/// <see cref="TryValidate(PbBlockedInteger, out List{String})"/>과 같지만 전체
	/// 디코딩 값 개수에 호출자가 정한 상한을 함께 강제합니다.
	/// </summary>
	/// <remarks>
	/// 신뢰할 수 없는(네트워크) 입력에는 반드시 이 오버로드를 쓰세요. 블록별
	/// 검증은 블록당 값 개수(8,192)만 제한할 뿐 블록 *개수*는 제한하지 않으므로,
	/// 블록당 ~10바이트의 wire 비용으로 블록당 8,192개 값이 만들어집니다 - 예를
	/// 들어 64KB 프레임 하나가 상한 없는 검증을 통과한 채 수천만 개 값(수백 MB)
	/// 디코딩을 유발할 수 있습니다. 프레임 변환(LZ4 등) 없이도 성립하는 프로토콜
	/// 내장 압축이므로, 전송 계층의 프레임 크기 제한이 이 증폭을 막아주지 않습니다.
	/// 상한은 애플리케이션이 그 메시지에서 실제로 기대하는 시퀀스 길이로
	/// 정하세요(예: 인벤토리 id 목록이면 수천).
	/// </remarks>
	/// <param name="proto">검증할 프로토콜 버퍼</param>
	/// <param name="maxTotalValues">허용할 전체 디코딩 값 개수의 상한(1 이상)</param>
	/// <param name="errors">발견된 에러 메시지 목록</param>
	/// <returns>유효하면 true, 그렇지 않으면 false</returns>
	/// <exception cref="ArgumentNullException">proto가 null인 경우</exception>
	/// <exception cref="ArgumentOutOfRangeException">maxTotalValues가 1 미만인 경우</exception>
	public static bool TryValidate(PbBlockedInteger proto, Int64 maxTotalValues, out List<String> errors)
	{
		ArgumentNullException.ThrowIfNull(proto);
		ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxTotalValues);
		errors = [];

		for (Int32 blockIndex = 0; blockIndex < proto.Blocks.Count; ++blockIndex)
		{
			PbBlock block = proto.Blocks[blockIndex];
			Validators.ValidateBlock(block, blockIndex, errors);
		}

		// 전체 값 개수 상한을 집행한다. 상한을 명시하지 않은 오버로드에서도
		// List<Int64> 물리 한계(Int32.MaxValue)를 넘으면 Decode가 예외를 던지므로
		// 여기서 미리 보고한다. (개별 블록은 유효해도 합계가 초과할 수 있다)
		Int64 totalCount = Decoders.GetTotalValueCount(proto);
		Int64 effectiveMax = Math.Min(maxTotalValues, Int32.MaxValue);
		if (totalCount > effectiveMax)
		{
			errors.Add($"전체 값 개수({totalCount})가 허용 상한({effectiveMax})을 초과");
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
		return statistics;
	}
}
