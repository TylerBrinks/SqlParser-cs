
namespace SqlParser.Ast;

public abstract record GroupByExpression : IWriteSql, IElement
{
    public record All : GroupByExpression;

    public record Expressions(Sequence<Expression> ColumnNames) : GroupByExpression;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case All:
                writer.Write("GROUP BY ALL");
                break;
            case Expressions expressions:
                writer.Write($"GROUP BY {expressions.ColumnNames.ToSqlDelimited()}");
                break;
        }
    }
}