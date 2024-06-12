namespace SqlParser.Ast;

public record DictionaryField(Ident Key, Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key}: {Value}");
    }
}