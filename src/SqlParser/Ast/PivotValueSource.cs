namespace SqlParser.Ast;

public abstract record PivotValueSource : IWriteSql
{
    public record List(Sequence<ExpressionWithAlias> Values) : PivotValueSource;
    public record Any(Sequence<OrderByExpression>? OrderBy) : PivotValueSource;
    public record Subquery(Query Query) : PivotValueSource;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case List l:
                writer.WriteDelimited(l.Values, Constants.SpacedComma);
                break;

            case Any a:
                writer.Write("ANY");
                if (a.OrderBy.SafeAny())
                {
                    writer.Write(" ORDER BY ");
                    writer.WriteDelimited(a.OrderBy, Constants.SpacedComma);
                }
                break;

            case Subquery s:
                writer.WriteSql($"{s.Query}");
                break;
        }
    }
}
