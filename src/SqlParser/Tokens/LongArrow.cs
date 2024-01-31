namespace SqlParser.Tokens;

/// <summary>
/// ->> used as a operator to extract json field as text in PostgreSQL
/// </summary>
public class LongArrow() : StringToken("->>");