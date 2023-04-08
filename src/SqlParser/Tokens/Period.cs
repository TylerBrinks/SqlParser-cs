namespace SqlParser.Tokens;

/// <summary>
/// Period (used for compound identifiers or projections into nested types)
/// </summary>
public class Period : SingleCharacterToken
{
    public Period() : base(Symbols.Dot)
    {
    }
}