namespace SqlParser.Tokens;

/// <summary>
/// '<<' a bitwise shift left operator in PostgreSQL
/// </summary>
public class ShiftLeft() : StringToken("<<");