namespace SqlParser.Tokens;

/// <summary>
/// Right parenthesis `)`
/// </summary>
public class RightParen : SingleCharacterToken
{
    public RightParen() : base(Symbols.ParenClose)
    {
    }
}