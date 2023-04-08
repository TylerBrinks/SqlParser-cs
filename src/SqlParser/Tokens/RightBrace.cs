namespace SqlParser.Tokens;

/// <summary>
/// Right brace `}`
/// </summary>
public class RightBrace : SingleCharacterToken
{
    public RightBrace() : base(Symbols.CurlyBracketClose)
    {
    }
}