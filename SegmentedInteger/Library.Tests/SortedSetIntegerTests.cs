using CsvHelper;
using CsvHelper.Configuration;
using Google.Protobuf;
using Library.Disposables;
using Library.SegmentedIntegers;
using System.Globalization;

namespace Library.Tests;

public class SortedSetIntegerTests
{
    private static async Task AssertRoundTrip(SortedSet<Int64> testSet)
    {
        Pb.SortedSetInteger converted;
        SortedSet<Int64> results;

        using (ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true))
        {
            SortedSetInteger.Encode(testSet, out converted);
            SortedSetInteger.Decode(converted, out results);
        }

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * sizeof(Int64):N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentTo(results);
    }

    [Test]
    public async Task EmptyTest()
    {
        await AssertRoundTrip([]);
    }

    [Test]
    public async Task From0To099Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 100; ++i) testSet.Add(i);

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task From0To199Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 200; ++i) testSet.Add(i);

        await AssertRoundTrip(testSet);
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

        await AssertRoundTrip(testSet);
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

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task SingleElementTest()
    {
        await AssertRoundTrip([42L]);
    }

    [Test]
    public async Task TwoElementsAdjacentTest()
    {
        // gap = 1 < 64 → BitmapChunk mode
        await AssertRoundTrip([0L, 1L]);
    }

    [Test]
    public async Task TwoElementsLargeGapTest()
    {
        // gap > IncrementChunkMax (2,000,000) → each value becomes its own segment
        await AssertRoundTrip([0L, 5_000_000L]);
    }

    [Test]
    public async Task BitmapChunkExactlyFullTest()
    {
        // 64 sequential values (0-63): fills one BitmapChunk with Filled=true
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 64; ++i) testSet.Add(i);

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task BitmapChunkPlusOneTest()
    {
        // 65 sequential values (0-64): one full BitmapChunk + overflow into next segment
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 65; ++i) testSet.Add(i);

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task LargeSequentialTest()
    {
        // 10,000 elements: exceeds ArrayPool threshold (1,024)
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 10_000; ++i) testSet.Add(i);

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task SparseValuesTest()
    {
        // gap = 1,000 (>= 64, < 2,000,000) → IncrementChunk mode
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 100; ++i) testSet.Add(i * 1_000L);

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task VeryLargeGapTest()
    {
        // gap = 3,000,000 > IncrementChunkMax (2,000,000) → each value in its own segment
        await AssertRoundTrip([0L, 3_000_000L, 6_000_000L, 9_000_000L]);
    }

    [Test]
    public async Task MixedModeTest()
    {
        // Bitmap (dense, gap < 64) → Increment (sparse, gap >= 64) → Bitmap again
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 10; ++i) testSet.Add(i);           // 0-9:   Bitmap
        for (Int64 i = 1; i <= 3; ++i) testSet.Add(i * 10_000L); // 10000, 20000, 30000: Increment
        testSet.Add(30_001L);
        testSet.Add(30_002L);
        testSet.Add(30_003L);                                      // 30001-30003: Bitmap

        await AssertRoundTrip(testSet);
    }

    [Test]
    public async Task NullSortedSetThrowsTest()
    {
        await Assert.That(() =>
        {
            SortedSet<Int64>? nullSet = null;
            SortedSetInteger.Encode(nullSet!, out _);
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task NullProtoThrowsTest()
    {
        await Assert.That(() =>
        {
            SortedSetInteger.Decode(null!, out _);
        }).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task NegativeValueThrowsTest()
    {
        // Span overload: first element negative → ArgumentException
        static void Invoke()
        {
            Int64[] arr = [-1L, 0L, 1L];
            SortedSetInteger.Encode(arr.AsSpan(), out _);
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
            SortedSetInteger.Encode(arr.AsSpan(), out _);
        }

        await Assert.That(Invoke).Throws<ArgumentException>();
    }

    // byte-snapshot 테스트: NEW 코드 기준 canonical 인코딩 출력을 검증.
    // 이 값들이 바뀌면 byte 호환성이 깨진 것이므로 즉시 확인 필요.

    [Test]
    public async Task ByteSnapshot_EmptyTest()
    {
        SortedSetInteger.Encode([], out var proto);
        await Assert.That(proto.ToByteArray().Length).IsEqualTo(0);
    }

    [Test]
    public async Task ByteSnapshot_BitmapFilledTest()
    {
        // [0..63]: BitmapChunk(Start=0, Filled=true) 단일 세그먼트
        SortedSet<Int64> set = [];
        for (Int64 i = 0; i < 64; ++i) set.Add(i);

        SortedSetInteger.Encode(set, out var proto);
        await Assert.That(Convert.ToHexString(proto.ToByteArray()))
            .IsEqualTo("0A060A0408001801");
    }

    [Test]
    public async Task ByteSnapshot_BitmapFilledPlusIncrementTest()
    {
        // [0..64]: BitmapChunk(Filled=true) + IncrementChunk(Start=64) — NEW canonical 형식
        SortedSet<Int64> set = [];
        for (Int64 i = 0; i < 65; ++i) set.Add(i);

        SortedSetInteger.Encode(set, out var proto);
        await Assert.That(Convert.ToHexString(proto.ToByteArray()))
            .IsEqualTo("0A060A04080018010A0412020840");
    }

    [Test]
    public async Task ByteSnapshot_MixedModeTest()
    {
        // Bitmap(0..9) + Increment(9,10000,20000) + Bitmap(30000..30003)
        SortedSet<Int64> set = [];
        for (Int64 i = 0; i < 10; ++i) set.Add(i);
        for (Int64 i = 1; i <= 3; ++i) set.Add(i * 10_000L);
        set.Add(30_001L); set.Add(30_002L); set.Add(30_003L);

        SortedSetInteger.Encode(set, out var proto);
        await Assert.That(Convert.ToHexString(proto.ToByteArray()))
            .IsEqualTo("0A070A0508001201FF0A0B120908091205874E979C010A090A0708B0EA01120107");
    }

    [Test]
    public async Task BothOverloadsProduceSameBytesTest()
    {
        // SortedSet 경로와 ReadOnlySpan 경로가 동일 bytes를 생성하는지 확인
        SortedSet<Int64> set = [0, 1, 100, 200];
        Int64[] arr = [0, 1, 100, 200];

        SortedSetInteger.Encode(set, out var proto1);
        SortedSetInteger.Encode(arr.AsSpan(), out var proto2);

        await Assert.That(Convert.ToHexString(proto1.ToByteArray()))
            .IsEqualTo(Convert.ToHexString(proto2.ToByteArray()));
    }
}
