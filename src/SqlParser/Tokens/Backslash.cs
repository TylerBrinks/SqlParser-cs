namespace SqlParser.Tokens;

/// <summary>
/// Backslash `\` used in terminating the COPY payload with `\.`
/// </summary>
public class Backslash : SingleCharacterToken
{
    public Backslash() : base(Symbols.Backslash)
    {
    }
}