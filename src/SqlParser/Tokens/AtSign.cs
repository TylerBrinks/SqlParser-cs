namespace SqlParser.Tokens;

/// <summary>
/// AtSign `@` used for PostgreSQL abs operator
/// </summary>
public class AtSign() : SingleCharacterToken(Symbols.At);