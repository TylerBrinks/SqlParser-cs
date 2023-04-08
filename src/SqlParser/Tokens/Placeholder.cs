namespace SqlParser.Tokens;

/// <summary>
/// `?` or `$` a prepared statement arg placeholder
/// </summary>
public class Placeholder : StringToken
{
    public Placeholder(string value) : base(value)
    {
    }
}