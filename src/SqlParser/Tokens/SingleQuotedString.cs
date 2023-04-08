namespace SqlParser.Tokens;

/// <summary>
/// Single quoted string: i.e: 'string'
/// </summary>
public class SingleQuotedString : StringToken
{
    public SingleQuotedString(string value) : base(value) { }

    public override string ToString()
    {
        return $"'{Value}'";
    }
}