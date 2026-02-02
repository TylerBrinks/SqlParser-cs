namespace SqlParser.Ast;

/// <summary>
/// SQL Server RAISERROR statement
/// </summary>
public record RaiseErrorStatement(
    Expression Message,
    Expression Severity,
    Expression State) : IWriteSql, IElement
{
    public Sequence<Expression>? Arguments { get; init; }
    public Sequence<RaiseErrorOption>? Options { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"RAISERROR({Message}, {Severity}, {State}");

        if (Arguments.SafeAny())
        {
            writer.WriteSql($", {Arguments.ToSqlDelimited()}");
        }

        writer.Write(")");

        if (Options.SafeAny())
        {
            writer.WriteSql($" WITH {Options.ToSqlDelimited()}");
        }
    }
}

/// <summary>
/// RAISERROR options
/// </summary>
public enum RaiseErrorOption
{
    Log,
    NoWait,
    SetError
}
