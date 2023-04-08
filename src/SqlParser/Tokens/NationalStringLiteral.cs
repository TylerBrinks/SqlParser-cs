namespace SqlParser.Tokens;

/// <summary>
/// "National" string literal: i.e: N'string'
/// </summary>
public class NationalStringLiteral : StringToken
{
    public NationalStringLiteral(string value) : base(value)
    {
    }

    public override string ToString()
    {
        return $"N'{Value}'";
    }
}