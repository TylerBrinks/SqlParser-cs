namespace SqlParser.Ast;

public record Setting(Ident Key, Value Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key} = {Value}");
    }
}
