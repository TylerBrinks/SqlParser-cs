namespace SqlParser.Tokens;

/// <summary>
/// Modulo Operator `%`
/// </summary>
public class Modulo : SingleCharacterToken
{
    public Modulo() : base(Symbols.Percent)
    {
    }
}