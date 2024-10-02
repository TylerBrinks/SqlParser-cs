namespace SqlParser.Ast;

public record ClusteredIndex(Ident Name, bool? Asc) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");

        if (Asc.HasValue)
        {
            writer.Write(Asc.Value ? " ASC" : " DESC");
        }
    }
}

public abstract record TableOptionsClustered : IWriteSql, IElement
{
    public record ColumnstoreIndex : TableOptionsClustered;
    public record ColumnstoreIndexOrder(Sequence<Ident> Names) : TableOptionsClustered;
    public record Index(Sequence<ClusteredIndex> Indices) : TableOptionsClustered;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case ColumnstoreIndex:
                writer.Write("CLUSTERED COLUMNSTORE INDEX");
                break;

            case ColumnstoreIndexOrder o:
                writer.WriteSql($"CLUSTERED COLUMNSTORE INDEX ORDER ({o.Names.ToSqlDelimited()})");
                break;

            case Index i:
                writer.WriteSql($"CLUSTERED INDEX ({i.Indices.ToSqlDelimited()})");
                break;
        }
    }
}