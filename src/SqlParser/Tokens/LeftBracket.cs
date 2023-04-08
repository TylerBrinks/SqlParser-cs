namespace SqlParser.Tokens;

/// <summary>
/// Left bracket `[`
/// </summary>
public class LeftBracket : SingleCharacterToken
{
    public LeftBracket() : base(Symbols.SquareBracketOpen)
    {
    }
}