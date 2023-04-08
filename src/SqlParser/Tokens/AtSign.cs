namespace SqlParser.Tokens;

/// <summary>
/// AtSign `@` used for PostgreSQL abs operator
/// </summary>
public class AtSign : SingleCharacterToken
{
    public AtSign():base(Symbols.At)
    {
    }
}