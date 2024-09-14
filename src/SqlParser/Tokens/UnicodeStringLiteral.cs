namespace SqlParser.Tokens;

/// <summary>
/// Unicode string literal: i.e: U&'first \000A second'
/// </summary>
/// <param name="value"></param>
public class UnicodeStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"U&'{Value}'";
    }
}