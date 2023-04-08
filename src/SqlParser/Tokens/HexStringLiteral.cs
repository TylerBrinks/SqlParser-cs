namespace SqlParser.Tokens;

/// <summary>
/// Hexadecimal string literal: i.e.: X'deadbeef'
/// </summary>
public class HexStringLiteral : StringToken
{
    public HexStringLiteral(string value) : base(value) { }

    public override string ToString()
    {
        return $"X'{Value}'";
    }
}