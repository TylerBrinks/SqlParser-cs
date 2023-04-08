namespace SqlParser.Tokens;

/// <summary>
/// Division operator `/`
/// </summary>
public class Divide : SingleCharacterToken
{
    public Divide() : base(Symbols.Divide)
    {
    }
}