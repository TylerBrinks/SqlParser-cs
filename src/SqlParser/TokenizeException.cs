namespace SqlParser;

public class TokenizeException(string? message, Location? location) : Exception(message)
{
    //private readonly Location? _location = location;

    public long Line => location?.Line ?? 0;
    public long Column => location?.Column ?? 0;
}