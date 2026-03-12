using CsvHelper;
using CsvHelper.Configuration;
using Library.Disposables;
using Library.SegmentedIntegers;
using System.Globalization;

namespace Library.Tests;

public class SegmentedIntegerTests
{
    [Test]
    public async Task EmptyTest()
    {
        SortedSet<Int64> testSet = [];

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task From0To099Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 100; ++i)
        {
            testSet.Add(i);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task From0To199Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 200; ++i)
        {
            testSet.Add(i);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task FromSortedIntData01Test()
    {
        string filePath = "TestDatas/sorted_int_data_01.csv";

        if (!File.Exists(filePath))
        {
            Assert.Fail($"File is not exist. {filePath}");
            return;
        }

        CsvConfiguration config = new(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
        };

        SortedSet<Int64> testSet = [];

        using (StreamReader reader = new(filePath))
        using (CsvReader csv = new(reader, config))
        {
            while (csv.Read())
            {
                for (Int32 i = 0; i < csv.ColumnCount; ++i)
                {
                    testSet.Add(Convert.ToInt64(csv.GetField(i)));
                }
            }
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task FromSortedIntData02Test()
    {
        string filePath = "TestDatas/sorted_int_data_02.csv";

        if (!File.Exists(filePath))
        {
            Assert.Fail($"File is not exist. {filePath}");
            return;
        }

        CsvConfiguration config = new(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = false,
        };

        SortedSet<Int64> testSet = [];

        using (StreamReader reader = new(filePath))
        using (CsvReader csv = new(reader, config))
        {
            while (csv.Read())
            {
                for (Int32 i = 0; i < csv.ColumnCount; ++i)
                {
                    testSet.Add(Convert.ToInt64(csv.GetField(i)));
                }
            }
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task SingleElementTest()
    {
        SortedSet<Int64> testSet = [42L];

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task TwoElementsAdjacentTest()
    {
        // gap = 1 < 64 → Segment64 mode
        SortedSet<Int64> testSet = [0L, 1L];

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task TwoElementsLargeGapTest()
    {
        // gap > Segment2MMax (2,000,000) → each value becomes its own segment
        SortedSet<Int64> testSet = [0L, 5_000_000L];

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task Segment64ExactlyFullTest()
    {
        // 64 sequential values (0-63): fills one Segment64 with Filled=true
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 64; ++i)
        {
            testSet.Add(i);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task Segment64PlusOneTest()
    {
        // 65 sequential values (0-64): one full Segment64 + overflow into next segment
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 65; ++i)
        {
            testSet.Add(i);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task LargeSequentialTest()
    {
        // 10,000 elements: exceeds ArrayPool threshold (1,024)
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 10_000; ++i)
        {
            testSet.Add(i);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task SparseValuesTest()
    {
        // gap = 1,000 (>= 64, < 2,000,000) → Segment2M mode
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 100; ++i)
        {
            testSet.Add(i * 1_000L);
        }

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task VeryLargeGapTest()
    {
        // gap = 3,000,000 > Segment2MMax (2,000,000) → each value in its own segment
        SortedSet<Int64> testSet = [0L, 3_000_000L, 6_000_000L, 9_000_000L];

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task MixedModeTest()
    {
        // S64 (dense, gap < 64) → S2M (sparse, gap >= 64) → S64 again
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 10; ++i) testSet.Add(i);           // 0-9:   S64
        for (Int64 i = 1; i <= 3; ++i) testSet.Add(i * 10_000L); // 10000, 20000, 30000: S2M
        testSet.Add(30_001L);
        testSet.Add(30_002L);
        testSet.Add(30_003L);                                      // 30001-30003: S64

        Pb.SegmentedInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SegmentedInteger.Encode(testSet, out converted);
            SegmentedInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task NullSortedSetThrowsTest()
    {
        await Assert.That(() =>
        {
            SortedSet<Int64>? nullSet = null;
            SegmentedInteger.Encode(nullSet!, out _);
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task NullProtoThrowsTest()
    {
        await Assert.That(() =>
        {
            SegmentedInteger.Decode(null!, out _);
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task NegativeValueThrowsTest()
    {
        // Span overload: first element negative → ArgumentException
        static void Invoke()
        {
            Int64[] arr = [-1L, 0L, 1L];
            SegmentedInteger.Encode(arr.AsSpan(), out _);
        }

        await Assert.That(Invoke).Throws<ArgumentException>();
    }

    [Test]
    public async Task UnsortedValuesThrowsTest()
    {
        // Span overload: not strictly ascending → ArgumentException
        static void Invoke()
        {
            Int64[] arr = [5L, 3L, 8L];
            SegmentedInteger.Encode(arr.AsSpan(), out _);
        }

        await Assert.That(Invoke).Throws<ArgumentException>();
    }
}
