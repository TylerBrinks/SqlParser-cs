namespace SqlParser.Tokens;

/// <summary>
/// Hash `#` used for PostgreSQL Bitwise XOR operator
/// </summary>
public class Hash : SingleCharacterToken
{
    public Hash():base(Symbols.Num)
    {
    }
}