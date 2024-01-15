
namespace SqlParser.Ast;

public abstract record GroupByExpression : IWriteSql
{
    public record All : GroupByExpression;

    public record Expressions(Sequence<Expression> ColumnNames) : GroupByExpression { }

    public void ToSql(SqlTextWriter writer)
    {
        if (this is All all)
        {
            writer.Write("GROUP BY ALL");
        }
        else if (this is Expressions expressions)
        {
            writer.Write("GROUP BY ");
            writer.WriteDelimited(expressions.ColumnNames, ", ");
            //writer.Write(")");
        }
    }
}