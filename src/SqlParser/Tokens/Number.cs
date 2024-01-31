namespace SqlParser.Tokens;

/// <summary>
/// An unsigned numeric literal token
/// </summary>
public class Number(string value, bool @long) : StringToken(value)
{
    public Number(string value) : this(value, false)
    {
    }

    public bool Long { get; } = @long;

    public override string ToString()
    {
        var longText = Long ? "L" : null;
        return $"{Value}{longText}";
    }
}