namespace SqlParser.Tokens;

/// <summary>
/// Colon `:`
/// </summary>
public class Colon : SingleCharacterToken
{
    public Colon() : base(Symbols.Colon) { }
}