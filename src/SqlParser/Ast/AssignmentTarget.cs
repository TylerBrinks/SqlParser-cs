namespace SqlParser.Ast;

public abstract record AssignmentTarget : IWriteSql
{
    public record ColumnName(ObjectName Name) : AssignmentTarget;
    public record Tuple(Sequence<ObjectName> Names) : AssignmentTarget;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case ColumnName c:
                writer.WriteSql($"{c.Name}");
                break;

            case Tuple t:
                writer.WriteSql($"({t.Names.ToSqlDelimited()})");
                break;
        }
    }
}
