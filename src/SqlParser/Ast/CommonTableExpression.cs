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
public record CommonTableExpression(TableAlias Alias, Query Query, Ident? From = null, CteAsMaterialized? Materialized = null) : IWriteSql, IElement
{
    public Ident? From { get; internal set; } = From;

    public void ToSql(SqlTextWriter writer)
    {
        if (Materialized == null)
        {
            if (Alias.AsKeyword)
                writer.WriteSql($"{Alias} AS ({Query})");
            else
                writer.WriteSql($"{Alias} ({Query})");
        }
        else
        {
            if (Alias.AsKeyword)
                writer.WriteSql($"{Alias} AS {Materialized} ({Query})");
            else
                writer.WriteSql($"{Alias} {Materialized} ({Query})");
        }

        if (From != null)
        {
            writer.WriteSql($" FROM {From}");
        }
    }
}

public enum CteAsMaterialized
{
    // The `WITH` statement specifies `AS MATERIALIZED` behavior
    Materialized,
    // The `WITH` statement specifies `AS NOT MATERIALIZED` behavior
    NotMaterialized
}