using System.Diagnostics;

namespace Library.Disposables;

public class ElapseWriter : IDisposable
{
    private readonly TextWriter _writer;
    private readonly string _name;
    private readonly Int64 _timestamp = Stopwatch.GetTimestamp();

    public ElapseWriter(TextWriter writer, string name = "", bool disableStartLogging = false)
    {
        _writer = writer;
        _name = name;

        if (!disableStartLogging)
        {
            if (string.IsNullOrEmpty(_name))
            {
                _writer.WriteLine("elapse begin");
            }
            else
            {
                _writer.WriteLine($"elapse begin: {_name}");
            }
        }
    }

    public void Dispose()
    {
        TimeSpan elapsed = TimeSpan.FromTicks(Stopwatch.GetTimestamp() - _timestamp);

        if (string.IsNullOrEmpty(_name))
        {
            _writer.WriteLine($"elapse end: {elapsed:hh\\:mm\\:ss\\.fff}");
        }
        else
        {
            _writer.WriteLine($"elapse end: {_name}, {elapsed:hh\\:mm\\:ss\\.fff}");
        }
    }
}