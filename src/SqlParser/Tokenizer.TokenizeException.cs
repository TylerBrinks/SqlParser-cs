namespace SqlParser;

public class TokenizeException : Exception
{
    public TokenizeException(string? message, Location location) : base(message)
    {
        _location = location;
    }

    private readonly Location? _location;

    public long Line => _location?.Line ?? 0;
    public long Column => _location?.Column ?? 0;
}