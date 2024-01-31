namespace SqlParser.Tokens;

/// <summary>
/// "National" string literal: i.e: N'string'
/// </summary>
public class NationalStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"N'{Value}'";
    }
}