namespace SqlParser.Ast;

/// <summary>
/// RAISE statement for PostgreSQL/Snowflake
/// </summary>
public record RaiseStatement : IWriteSql, IElement
{
    public RaiseStatementLevel? Level { get; init; }
    public Expression? Value { get; init; }
    public Sequence<RaiseStatementOption>? Options { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("RAISE");

        if (Level != null)
        {
            writer.WriteSql($" {Level}");
        }

        if (Value != null)
        {
            writer.WriteSql($" {Value}");
        }

        if (Options.SafeAny())
        {
            writer.WriteSql($" USING {Options.ToSqlDelimited()}");
        }
    }
}

/// <summary>
/// Raise statement level
/// </summary>
public enum RaiseStatementLevel
{
    Debug,
    Log,
    Info,
    Notice,
    Warning,
    Exception
}

/// <summary>
/// RAISE USING option (name = expression)
/// </summary>
public record RaiseStatementOption(Ident Name, Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} = {Value}");
    }
}
