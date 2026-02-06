namespace SqlParser.Ast;

public record InputFormatClause(Ident Ident, Sequence<Expression>? Values = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"FORMAT {Ident}");

        if (Values is { Count: > 0 })
        {
            writer.WriteSql($" {Values}");
        }
    }
}
