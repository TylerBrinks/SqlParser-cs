namespace SqlParser.Ast;

public record DictionaryField(Ident Key, Expression Value) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key}: {Value}");
    }
}