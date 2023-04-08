namespace SqlParser.Tokens;

/// <summary>
/// Left parenthesis `(`
/// </summary>
public class LeftParen : SingleCharacterToken
{
    public LeftParen() : base(Symbols.ParenOpen)
    {
    }
}