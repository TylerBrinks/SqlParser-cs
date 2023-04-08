namespace SqlParser.Tokens;

/// <summary>
/// An unsigned numeric literal token
/// </summary>
public class Number : StringToken
{
    public Number(string value) : this(value, false)
    {
    }

    public Number(string value, bool @long) : base(value)
    {
        Long = @long;
    }

    public bool Long { get; }

    public override string ToString()
    {
        var longText = Long ? "L" : null;
        return $"{Value}{longText}";
    }
}