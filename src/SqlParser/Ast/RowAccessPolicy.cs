
namespace SqlParser.Ast;

public record RowAccessPolicy(ObjectName Name, Sequence<Ident> On) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"WITH ROW ACCESS POLICY {Name} ON ({On.ToSqlDelimited()})");
    }
}
