//namespace SqlParser.Ast;

//public record DuckDbDatabase(
//    bool IfExists,
//    bool Database,
//    Ident? DatabaseAlias) : IWriteSql
//{
//    public void ToSql(SqlTextWriter writer)
//    {
//        var ifExists = IfExists ? $" {IIfNotExists.IfExistsPhrase}" : null;
//        writer.WriteSql($"DETACH{Database}{ifExists} {DatabaseAlias}");
//    }
//}