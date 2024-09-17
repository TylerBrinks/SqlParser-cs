namespace SqlParser.Ast;

public abstract record Partition : IWriteSql, IElement
{
    public record Identifier(Ident Id) : Partition;
    public record Expr(Expression Expression) : Partition;
    public record Partitions(Sequence<Expression> Expressions) : Partition;
    public record Part(Expression Expression) : Partition;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Identifier i:
                writer.WriteSql($"PARTITION ID {i.Id}");
                break;

            case Expr e:
                writer.WriteSql($"PARTITION {e.Expression}");
                break;

            case Part pa:
                writer.WriteSql($"PART {pa.Expression}");
                break;

            case Partitions p:
                writer.WriteSql($"PARTITION ({p.Expressions.ToSqlDelimited()})");
                break;
        }
    }
}