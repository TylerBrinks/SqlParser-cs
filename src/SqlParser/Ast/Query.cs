namespace SqlParser.Ast;

/// <summary>
/// The most complete variant of a SELECT expression, optionally
/// including WITH, UNION / other set operations, and ORDER BY.
/// </summary>
public record Query([Visit(1)] SetExpression Body) : IWriteSql, IElement
{
    [Visit(0)] public With? With { get; init; }
    [Visit(2)] public Sequence<OrderByExpression>? OrderBy { get; set; }
    [Visit(3)] public Expression? Limit { get; init; }
    [Visit(4)] public Offset? Offset { get; init; }
    [Visit(5)] public Fetch? Fetch { get; init; }
    [Visit(6)] public Sequence<LockClause>? Locks { get; init; }
    [Visit(7)] public Sequence<Expression>? LimitBy { get; init; }
    [Visit(8)] public ForClause? ForClause { get; init; }

    public static implicit operator Query(Statement.Select select)
    {
        return select.Query;
    }

    public static implicit operator Statement.Select(Query query)
    {
        return new Statement.Select(query);
    }

    public void ToSql(SqlTextWriter writer)
    {
        if (With != null)
        {
            writer.WriteSql($"{With} ");
        }

        Body.ToSql(writer);

        if (OrderBy != null)
        {
            writer.WriteSql($" ORDER BY {OrderBy}");
        }

        if (Limit != null)
        {
            writer.WriteSql($" LIMIT {Limit}");
        }

        if (Offset != null)
        {
            writer.WriteSql($" {Offset}");
        }

        if (LimitBy.SafeAny())
        {
            writer.Write($" BY {LimitBy.ToSqlDelimited()}");
        }

        if (Fetch != null)
        {
            writer.WriteSql($" {Fetch}");
        }

        if (Locks != null && Locks.Any())
        {
            writer.WriteSql($" {Locks.ToSqlDelimited(Symbols.Space)}");
        }

        if (ForClause != null)
        {
            writer.WriteSql($" {ForClause}");
        }
    }
}