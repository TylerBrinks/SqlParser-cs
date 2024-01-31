namespace SqlParser.Tokens;

/// <summary>
/// Hexadecimal string literal: i.e.: X'deadbeef'
/// </summary>
public class HexStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"X'{Value}'";
    }
}