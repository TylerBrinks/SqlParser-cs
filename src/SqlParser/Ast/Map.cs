namespace SqlParser.Ast;

public record Map(Sequence<MapEntry> Entries) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"MAP {{{Entries}}}");
    }
}

public record MapEntry(Expression Key, Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key}: {Value}");
    }
}
