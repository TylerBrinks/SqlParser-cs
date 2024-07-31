namespace SqlParser.Ast;

public record HavingBound(HavingBoundKind Kind, Expression Expression) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"HAVING {Kind} {Expression}");
    }
}
