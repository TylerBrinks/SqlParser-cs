namespace SqlParser.Ast;

public abstract record WindowType : IWriteSql, IElement
{
    public record WindowSpecType(WindowSpec Spec) : WindowType;
    public record NamedWindow(Ident Name) : WindowType;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is WindowSpecType w)
        {
            writer.WriteSql($"({w.Spec})");
        }
        else if (this is NamedWindow n)
        {
            writer.WriteSql($"{n.Name}");
        }
    }
}