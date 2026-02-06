namespace SqlParser.Ast;

/// <summary>
/// A column definition for CREATE INDEX
/// </summary>
public record IndexColumn(
    Expression Expression
) : IWriteSql, IElement
{
    /// <summary>
    /// Operator class - PostgreSQL specific
    /// </summary>
    public Ident? OperatorClass { get; init; }

    /// <summary>
    /// Ascending or descending order
    /// </summary>
    public bool? Asc { get; init; }

    /// <summary>
    /// Whether NULLs should be first or last
    /// </summary>
    public bool? NullsFirst { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Expression}");

        if (OperatorClass != null)
        {
            writer.WriteSql($" {OperatorClass}");
        }

        if (Asc.HasValue)
        {
            writer.Write(Asc.Value ? " ASC" : " DESC");
        }

        if (NullsFirst.HasValue)
        {
            writer.Write(NullsFirst.Value ? " NULLS FIRST" : " NULLS LAST");
        }
    }
}
