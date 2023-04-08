namespace SqlParser.Tokens;

/// <summary>
/// Caret `^`
/// </summary>
public class Caret : SingleCharacterToken
{
    public Caret() : base(Symbols.Caret)
    {
    }
}