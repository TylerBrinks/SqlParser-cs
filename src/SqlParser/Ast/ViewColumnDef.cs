namespace SqlParser.Ast;

public  record ViewColumnDef(Ident Name, Sequence<SqlOption>? Options = null) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");

        if (Options.SafeAny())
        {
            writer.Write($" OPTIONS({Options.ToSqlDelimited()})");
        }
    }
}
