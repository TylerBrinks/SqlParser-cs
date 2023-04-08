namespace SqlParser.Tokens;

/// <summary>
/// Pipe `|`
/// </summary>
public class Pipe : SingleCharacterToken
{
    public Pipe() : base(Symbols.Pipe)
    {
    }
}