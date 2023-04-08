namespace SqlParser.Tokens;

/// <summary>
/// SemiColon `;` used as separator for COPY and payload
/// </summary>
public class SemiColon : SingleCharacterToken
{
    public SemiColon() : base(Symbols.Semicolon)
    {
    }
}