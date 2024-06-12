
namespace SqlParser.Ast;
public record ConnectBy(Expression Condition, Sequence<Expression> Relationships) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"START WITH {Condition} CONNECT BY {Relationships.ToSqlDelimited()}");
    }
}
