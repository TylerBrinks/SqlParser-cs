namespace SqlParser.Ast;

public record Partition(Sequence<Expression> Partitions) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("PARTITION (");
        writer.WriteDelimited(Partitions, ", ");
        writer.Write(")");
    }
}