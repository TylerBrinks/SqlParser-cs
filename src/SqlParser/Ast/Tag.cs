using System.Collections;

namespace SqlParser.Ast;

public record Tag(Ident Key, string Value) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key}='{Value}'");
    }
}