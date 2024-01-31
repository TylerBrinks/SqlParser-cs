namespace SqlParser.Tokens;

/// <summary>
///  `!~~*`, a case insensitive not match pattern operator in PostgreSQL
/// </summary>
public class ExclamationMarkDoubleTildeAsterisk() : StringToken("!~~*");