namespace SqlParser;

public class ParserException : Exception
{
    public ParserException(string message) : this(message, null)
    {
    }

    public ParserException(string message, Location location) : base(message)
    {
        _location = location;
    }

    private readonly Location? _location;

    public long Line => _location?.Line ?? 0;
    public long Column => _location?.Column ?? 0;
}