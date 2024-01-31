namespace SqlParser.Tokens;

/// <summary>
/// Hash `#` used for PostgreSQL Bitwise XOR operator
/// </summary>
public class Hash() : SingleCharacterToken(Symbols.Num);