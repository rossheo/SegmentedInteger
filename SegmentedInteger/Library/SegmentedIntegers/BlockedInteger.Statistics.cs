namespace Library.SegmentedIntegers;

using PbBlock = Pb.BlockedInteger.Types.Block;

public static partial class BlockedInteger
{
	/// <summary>
	/// Int64 시퀀스 압축의 통계 정보.
	/// </summary>
	public sealed class CompressionStatistics
	{
		/// <summary>원본 값의 개수.</summary>
		public Int64 TotalValues { get; set; }

		/// <summary>원본 크기 (바이트).</summary>
		public Int64 OriginalSize { get; set; }

		/// <summary>압축 크기 (바이트).</summary>
		public Int64 CompressedSize { get; set; }

		/// <summary>
		/// 압축률 (이론상 0.0 이상, 1.0은 무압축, 0.0에 가까울수록 높은 압축률).
		/// 소규모 입력에서 프로토버프 오버헤드로 인해 1.0을 초과할 수 있습니다.
		/// 기반 값에서 즉시 계산되므로 별도 갱신이 필요 없습니다.
		/// </summary>
		public Double CompressionRatio =>
			OriginalSize > 0 ? (Double)CompressedSize / OriginalSize : 0.0;

		/// <summary>블록 개수.</summary>
		public Int32 BlockCount { get; set; }

		/// <summary>평균 블록 크기 (바이트). 기반 값에서 즉시 계산됩니다.</summary>
		public Double AverageBlockSize =>
			BlockCount > 0 ? (Double)CompressedSize / BlockCount : 0.0;

		/// <summary>블록 타입별 분포.</summary>
		public Dictionary<String, Int32> BlockTypeDistribution { get; set; } = [];
	}

	private static class StatisticsHelper
	{
		internal static void AddBlockStatistics(PbBlock block, CompressionStatistics statistics)
		{
			String blockType = block.BlockOneofCase.ToString();

			statistics.BlockTypeDistribution.TryAdd(blockType, 0);
			statistics.BlockTypeDistribution[blockType]++;
			statistics.TotalValues += Decoders.GetBlockValueCount(block);
		}
	}
}
