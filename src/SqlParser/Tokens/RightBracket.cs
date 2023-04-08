namespace SqlParser.Tokens;

/// <summary>
/// Right bracket `]`
/// </summary>
public class RightBracket : SingleCharacterToken
{
    public RightBracket() : base(Symbols.SquareBracketClose)
    {
    }
}