namespace SqlParser.Tokens;

/// <summary>
/// `?` or `$` a prepared statement arg placeholder
/// </summary>
public class Placeholder(string value) : StringToken(value);