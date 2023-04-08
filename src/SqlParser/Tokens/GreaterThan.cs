namespace SqlParser.Tokens;

/// <summary>
/// Greater Than operator `>`
/// </summary>
public class GreaterThan : SingleCharacterToken
{
    public GreaterThan() : base(Symbols.GreaterThan)
    {
    }
}