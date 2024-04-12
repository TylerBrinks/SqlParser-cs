namespace SqlParser.Tokens;

/// <summary>
/// Assignment `:=` (used for keyword argument in DuckDB macros)
/// </summary>
public class Assignment() : StringToken(":=");