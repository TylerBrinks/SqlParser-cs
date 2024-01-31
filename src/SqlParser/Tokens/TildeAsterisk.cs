namespace SqlParser.Tokens;

/// <summary>
/// `~*` a case insensitive match regular expression operator in PostgreSQL
/// </summary>
public class TildeAsterisk() : StringToken("~*");