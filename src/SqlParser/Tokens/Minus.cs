namespace SqlParser.Tokens;

/// <summary>
/// Minus operator `-`
/// </summary>
public class Minus : SingleCharacterToken
{
    public Minus() : base(Symbols.Minus)
    {
    }
}