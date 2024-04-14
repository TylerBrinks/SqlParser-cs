namespace SqlParser.Ast;

public record SecretOption(Ident Key, Ident Value) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key} {Value}");
    }
}