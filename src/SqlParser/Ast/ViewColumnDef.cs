namespace SqlParser.Ast;

public  record ViewColumnDef(Ident Name, DataType? DataType = null, Sequence<ColumnOption>? Options = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");

        if (DataType != null)
        {
            writer.WriteSql($" {DataType}");
        }

        if (Options.SafeAny())
        {
            writer.Write($" {Options.ToSqlDelimited()}");
        }
    }
}
