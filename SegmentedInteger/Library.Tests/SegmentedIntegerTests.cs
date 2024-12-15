using CsvHelper;
using CsvHelper.Configuration;
using Library.Disposables;
using Library.SegmentedIntegers;
using System.Globalization;

namespace Library.Tests;

public class SegmentedIntegerTests
{
    [Test]
    public async Task From0To099Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 100; ++i)
        {
            testSet.Add(i);
        }

        using ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true);

        SegmentedInteger.ConvertTo(testSet, out Pb.SegmentedInteger converted);
        SegmentedInteger.ConvertTo(converted, out SortedSet<Int64> results);

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * 4:N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentCollectionTo(results);
    }

    [Test]
    public async Task From0To199Test()
    {
        SortedSet<Int64> testSet = [];
        for (Int64 i = 0; i < 200; ++i)
        {
            testSet.Add(i);
        }

        using ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true);

        SegmentedInteger.ConvertTo(testSet, out Pb.SegmentedInteger converted);
        SegmentedInteger.ConvertTo(converted, out SortedSet<Int64> results);

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * 4:N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentCollectionTo(results);
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

        using ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true);

        SegmentedInteger.ConvertTo(testSet, out Pb.SegmentedInteger converted);
        SegmentedInteger.ConvertTo(converted, out SortedSet<Int64> results);

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * 4:N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentCollectionTo(results);
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

        using ElapseWriter elapse = new(TestContext.Current!.OutputWriter, disableStartLogging: true);

        SegmentedInteger.ConvertTo(testSet, out Pb.SegmentedInteger converted);
        SegmentedInteger.ConvertTo(converted, out SortedSet<Int64> results);

        await TestContext.Current!.OutputWriter.WriteLineAsync(
            $"IntSize: {testSet.Count * 4:N0}, pbSize: {converted.CalculateSize():N0}");

        await Assert.That(testSet).IsEquivalentCollectionTo(results);
    }
}
