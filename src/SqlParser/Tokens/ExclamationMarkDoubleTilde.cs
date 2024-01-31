namespace SqlParser.Tokens;

/// <summary>
/// `!~~`, a case sensitive not match pattern operator in PostgreSQL
/// </summary>
public class ExclamationMarkDoubleTilde() : StringToken("!~~");