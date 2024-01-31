namespace SqlParser.Tokens;

/// <summary>
/// Single quoted string: i.e: 'string'
/// </summary>
public class SingleQuotedString(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"'{Value}'";
    }
}