using Google.Protobuf;
using System.Diagnostics;

namespace Library.SegmentedIntegers;

public static class SegmentedInteger
{
    public static void ConvertTo(SortedSet<Int64> sortedIntegers,
        out Pb.SegmentedInteger pbSegmentedInteger)
    {
        pbSegmentedInteger = new();

#if DEBUG
        // 빈 컨테이너는 지원하지 않는다.
        Debug.Assert(sortedIntegers.Count != 0);

        // 음수는 지원하지 않는다.
        Debug.Assert(sortedIntegers.Min() >= 0);

        // Type은 Int64를 지원하고, 최대 개수는 Int32 Max
        Debug.Assert(sortedIntegers.Count < Int32.MaxValue);
#endif

        List<Int64> segment64Increments = [];
        Pb.SegmentedInteger.Types.Segment.Types.Segment64 pbSegment64 = new();
        Pb.SegmentedInteger.Types.Segment.Types.Segment2M pbSegment2M = new();

        Int64 max = Convert.ToInt64(
            Pb.SegmentedInteger.Types.Segment.Types.Segment64.Types.SegmentLimit.Max);

        for (Int32 i = 0; i < sortedIntegers.Count; ++i)
        {
            Int64 currentValue = sortedIntegers.ElementAt(i);
            Int64? nextValue = (i + 1 < sortedIntegers.Count) ? sortedIntegers.ElementAt(i + 1) : null;
            Int64 diffValue = nextValue.HasValue ? (nextValue.Value - currentValue) : 0;

            bool useSegment64 = pbSegment64.HasStart || (diffValue > 0 && diffValue < max);

            if (useSegment64 && pbSegment2M.HasStart)
            {
                AddSegment2M(pbSegmentedInteger, ref pbSegment2M);
            }
            else if (!useSegment64 && pbSegment64.HasStart)
            {
                AddSegment64(pbSegmentedInteger, ref segment64Increments, ref pbSegment64);
            }

            if (useSegment64)
            {
                HandleSegment64(currentValue, nextValue, pbSegmentedInteger,
                    ref segment64Increments, ref pbSegment64);
            }
            else
            {
                HandleSegment2M(currentValue, nextValue, pbSegmentedInteger,
                    ref pbSegment2M);
            }
        }

        if (pbSegment64.HasStart)
        {
            AddSegment64(pbSegmentedInteger, ref segment64Increments, ref pbSegment64);
        }
        else if (pbSegment2M.HasStart)
        {
            AddSegment2M(pbSegmentedInteger, ref pbSegment2M);
        }
    }

    private static void HandleSegment64(
        Int64 currentValue, Int64? nextValue,
        Pb.SegmentedInteger pbSegmentedInteger,
        ref List<Int64> segment64Increments,
        ref Pb.SegmentedInteger.Types.Segment.Types.Segment64 pbSegment)
    {
        Int64 max = Convert.ToInt64(
            Pb.SegmentedInteger.Types.Segment.Types.Segment64.Types.SegmentLimit.Max);

        if (!pbSegment.HasStart)
        {
            pbSegment.Start = currentValue;
            return;
        }

        Int64 increment = currentValue - pbSegment.Start;
        if (increment >= max)
        {
            AddSegment64(pbSegmentedInteger, ref segment64Increments, ref pbSegment);
            pbSegment.Start = currentValue;
            return;
        }

        segment64Increments.Add(increment);

        if (segment64Increments.Count == max - 1)
        {
            AddSegment64(pbSegmentedInteger, ref segment64Increments, ref pbSegment);
            return;
        }

        if (nextValue.HasValue)
        {
            Int64 nextIncrement = nextValue.Value - pbSegment.Start;
            if (nextIncrement >= max)
            {
                AddSegment64(pbSegmentedInteger, ref segment64Increments, ref pbSegment);
            }
        }
    }

    private static void HandleSegment2M(
        Int64 currentValue, Int64? nextValue,
        Pb.SegmentedInteger pbSegmentedInteger,
        ref Pb.SegmentedInteger.Types.Segment.Types.Segment2M pbSegment)
    {
        Int64 max = Convert.ToInt64(
            Pb.SegmentedInteger.Types.Segment.Types.Segment2M.Types.SegmentLimit.Max);

        if (!pbSegment.HasStart)
        {
            pbSegment.Start = currentValue;
            return;
        }

        Int64 increment = currentValue - pbSegment.Start;
        if (increment >= max)
        {
            AddSegment2M(pbSegmentedInteger, ref pbSegment);
            pbSegment.Start = currentValue;
            return;
        }

        pbSegment.Increments.Add(increment);

        if (nextValue.HasValue)
        {
            Int64 nextIncrement = nextValue.Value - pbSegment.Start;
            if (nextIncrement >= max)
            {
                AddSegment2M(pbSegmentedInteger, ref pbSegment);
            }
        }
    }

    private static void AddSegment64(
        Pb.SegmentedInteger pbSegmentedInteger,
        ref List<Int64> segment64Increments,
        ref Pb.SegmentedInteger.Types.Segment.Types.Segment64 pbSegment)
    {
        Int64 max = Convert.ToInt64(
            Pb.SegmentedInteger.Types.Segment.Types.Segment64.Types.SegmentLimit.Max);

        if (segment64Increments.Count == max - 1)
        {
            pbSegment.Filled = true;
        }
        else if (segment64Increments.Count > 0)
        {
            const Int32 bitSize = 8;

            Span<byte> stackBuffer = stackalloc byte[(Int32)max / bitSize];

            Int32 byteIndex = 0;
            foreach (Int64 increment in segment64Increments)
            {
                byteIndex = ((Int32)increment - 1) / bitSize;
                Int32 bitIndex = ((Int32)increment - 1) % bitSize;
                stackBuffer[byteIndex] |= (byte)(1 << bitIndex);
            }

            pbSegment.BitIncrements = ByteString.CopyFrom(stackBuffer[..(byteIndex + 1)]);
        }

        segment64Increments.Clear();

        pbSegmentedInteger.Segments.Add(new Pb.SegmentedInteger.Types.Segment
        {
            Segment64 = pbSegment
        });
        pbSegment = new();
    }

    private static void AddSegment2M(
        Pb.SegmentedInteger pbSegmentedInteger,
        ref Pb.SegmentedInteger.Types.Segment.Types.Segment2M pbSegment)
    {
        pbSegmentedInteger.Segments.Add(new Pb.SegmentedInteger.Types.Segment
        {
            Segment2M = pbSegment
        });
        pbSegment = new();
    }

    public static void ConvertTo(Pb.SegmentedInteger pbSegmentedInteger,
        out SortedSet<Int64> integers)
    {
        integers = [];

        foreach (Pb.SegmentedInteger.Types.Segment segment in pbSegmentedInteger.Segments)
        {
            switch (segment.SegmentsOneofCase)
            {
                case Pb.SegmentedInteger.Types.Segment.SegmentsOneofOneofCase.Segment64:
                    Segment64ToIntegers(segment.Segment64, ref integers);
                    break;
                case Pb.SegmentedInteger.Types.Segment.SegmentsOneofOneofCase.Segment2M:
                    Segment2MToIntegers(segment.Segment2M, ref integers);
                    break;
            }
        }
    }

    private static void Segment64ToIntegers(
        Pb.SegmentedInteger.Types.Segment.Types.Segment64 pbSegment,
        ref SortedSet<Int64> integers)
    {
        Int64 max = Convert.ToInt64(
            Pb.SegmentedInteger.Types.Segment.Types.Segment64.Types.SegmentLimit.Max);

        Int64 start = pbSegment.Start;
        integers.Add(start);

        if (pbSegment.Filled)
        {
            for (Int64 increment = 0; increment < max; ++increment)
            {
                integers.Add(start + increment);
            }
        }
        else
        {
            const Int32 bitSize = 8;

            for (Int32 byteIndex = 0; byteIndex < pbSegment.BitIncrements.Length; ++byteIndex)
            {
                Int64 startValue = start + (byteIndex * bitSize);

                for (Int32 bitIndex = 0; bitIndex < bitSize; ++bitIndex)
                {
                    if ((pbSegment.BitIncrements[byteIndex] & (1 << bitIndex)) != 0)
                    {
                        integers.Add(startValue + bitIndex + 1);
                    }
                }
            }
        }
    }

    private static void Segment2MToIntegers(
        Pb.SegmentedInteger.Types.Segment.Types.Segment2M pbSegment,
        ref SortedSet<Int64> integers)
    {
        Int64 start = pbSegment.Start;
        integers.Add(start);

        foreach (Int64 increment in pbSegment.Increments)
        {
            integers.Add(start + increment);
        }
    }
}
