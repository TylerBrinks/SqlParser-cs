namespace SqlParser.Ast;

public record Partition(Sequence<Expression> Partitions) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write($"PARTITION ({Partitions.ToSqlDelimited()})");
    }
}