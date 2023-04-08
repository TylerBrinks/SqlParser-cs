namespace SqlParser.Ast;

/// <summary>
/// A single CTE (used after WITH): alias [(col1, col2, ...)] AS ( query )
/// The names in the column list before AS, when specified, replace the names
/// of the columns returned by the query. The parser does not validate that the
/// number of columns in the query matches the number of columns in the query.
/// </summary>
/// <param name="Alias">CTE Alias</param>
/// <param name="Query">CTE Select</param>
/// <param name="From">Optional From identifier</param>
public record CommonTableExpression(TableAlias Alias, Query Query, Ident? From = null) : IWriteSql, IElement
{
    public Ident? From { get; internal set; } = From;

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Alias} AS ({Query.ToSql()})");

        if (From != null)
        {
            writer.WriteSql($" FROM {From}");
        }
    }
}