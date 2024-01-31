namespace SqlParser;

public class ParserException(string message, Location? location) : Exception(message)
{
    public ParserException(string message) : this(message, null)
    {
    }

    public long Line => location?.Line ?? 0;
    public long Column => location?.Column ?? 0;
}