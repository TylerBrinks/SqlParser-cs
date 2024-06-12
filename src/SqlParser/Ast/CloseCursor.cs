namespace SqlParser.Ast;

/// <summary>
/// Close Cursor specifications
/// </summary>
public abstract record CloseCursor : IWriteSql, IElement
{
    /// <summary>
    /// Close all cursors
    /// </summary>
    public record All : CloseCursor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ALL");
        }
    }

    /// <summary>
    /// Close specific cursors
    /// </summary>
    /// <param name="Name">Cursor name identifier</param>
    public record Specific(Ident Name) : CloseCursor
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Name.ToSql(writer);
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}