namespace SqlParser.Ast;

/// <summary>
/// Select into operation
/// </summary>
/// <param name="Name">Object name identifier</param>
public record SelectInto(ObjectName Name) : IWriteSql, IElement
{
    public bool Temporary { get; set; }
    public bool Unlogged { get; set; }
    public bool Table { get; set; }

    public void ToSql(SqlTextWriter writer)
    {
        var temp = Temporary ? " TEMPORARY" : null;
        var unlogged = Unlogged ? " UNLOGGED" : null;
        var table = Table ? " TABLE" : null;

        writer.Write($"INTO{temp}{unlogged}{table} {Name}");
    }
}