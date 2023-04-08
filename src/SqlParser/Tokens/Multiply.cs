namespace SqlParser.Tokens;

/// <summary>
/// Multiplication operator `*`
/// </summary>
public class Multiply : SingleCharacterToken
{
    public Multiply() : base(Symbols.Asterisk)
    {
    }
}