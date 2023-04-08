namespace SqlParser.Tokens;

/// <summary>
/// Left brace `{`
/// </summary>
public class LeftBrace : SingleCharacterToken
{
    public LeftBrace() : base(Symbols.CurlyBracketOpen)
    {
    }
}