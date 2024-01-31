namespace SqlParser.Tokens;

/// <summary>
/// Tilde `~` used for PostgreSQL Bitwise NOT operator or case sensitive match regular expression operator
/// </summary>
public class Tilde() : SingleCharacterToken(Symbols.Tilde);