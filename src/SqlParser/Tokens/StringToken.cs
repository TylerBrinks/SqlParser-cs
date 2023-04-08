namespace SqlParser.Tokens;

/// <summary>
/// Represents a token composed of a character array
/// </summary>
public abstract class StringToken : Token
{
    protected StringToken(string value)
    {
        Value = value;
    }

    public string Value { get; init; }

    public override string ToString()
    {
        return Value;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as StringToken);
    }

    protected bool Equals(StringToken? other)
    {
        return other != null && Value == other.Value;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Value);
    }
}