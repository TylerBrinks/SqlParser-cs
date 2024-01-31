namespace SqlParser.Tokens;

/// <summary>
/// Exclamation Mark `!` used for PostgreSQL factorial operator
/// </summary>
public class ExclamationMark() : SingleCharacterToken(Symbols.ExclamationMark);