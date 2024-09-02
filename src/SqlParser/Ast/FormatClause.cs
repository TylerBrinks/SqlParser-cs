namespace SqlParser.Ast;

public abstract record FormatClause : IWriteSql, IElement
{
    public record Identifier(Ident Name) : FormatClause;

    public record Null : FormatClause;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is Identifier i)
        {
            writer.WriteSql($"FORMAT {i.Name}");
        }
        else
        {
            writer.Write("FORMAT NULL");
        }
    }
}
