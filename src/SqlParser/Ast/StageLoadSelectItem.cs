namespace SqlParser.Ast;

public record StageLoadSelectItem : IWriteSql
{
    public Ident? Alias { get; set; }
    public int FileColumnNumber { get; set; }
    public Ident? Element { get; set; }
    public Ident? ItemAs { get; set; }

    public void ToSql(SqlTextWriter writer)
    {
        if (Alias != null)
        {
            writer.WriteSql($"{Alias}.");
        }

        writer.Write($"${FileColumnNumber}");

        if (Element != null)
        {
            writer.WriteSql($":{Element}");
        }

        if (ItemAs != null)
        {
            writer.WriteSql($" AS {ItemAs}");
        }
    }
}