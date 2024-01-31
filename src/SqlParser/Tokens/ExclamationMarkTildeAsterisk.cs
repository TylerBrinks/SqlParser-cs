namespace SqlParser.Tokens;

/// <summary>
/// `!~*` a case insensitive not match regular expression operator in PostgreSQL
/// </summary>
public class ExclamationMarkTildeAsterisk() : StringToken("!~*");