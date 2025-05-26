namespace SqlParser.Ast;

/// <summary>
/// Additional options for wildcards, e.g. Snowflake EXCLUDE/RENAME and BigQuery EXCEPT.
/// </summary>
public record WildcardAdditionalOptions : IWriteSql, IElement
{
    public IlikeSelectItem? ILikeOption { get; init; }
    // [EXCLUDE...]
    public ExcludeSelectItem? ExcludeOption { get; init; }
    // [EXCEPT...]
    public ExceptSelectItem? ExceptOption { get; init; }
    // [RENAME ...]
    public RenameSelectItem? RenameOption { get; init; }
    // [REPLACE]
    // BigQuery syntax: <https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_replace>
    public ReplaceSelectItem? ReplaceOption { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        if (ILikeOption != null)
        {
            writer.WriteSql($" {ILikeOption}");
        }

        if (ExcludeOption != null)
        {
            writer.WriteSql($" {ExcludeOption}");
        }

        if (ExceptOption != null)
        {
            writer.WriteSql($" {ExceptOption}");
        }

        if (ReplaceOption != null)
        {
            writer.WriteSql($" {ReplaceOption}");
        }

        if (RenameOption != null)
        {
            writer.WriteSql($" {RenameOption}");
        }
    }
}

public record ReplaceSelectItem(Sequence<ReplaceSelectElement> Items) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"REPLACE ({Items})");
    }
}

public record ReplaceSelectElement(Expression Expr, Ident Name, bool AsKeyword = true) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        if (AsKeyword)
            writer.WriteSql($"{Expr} AS {Name}");
        else
            writer.WriteSql($"{Expr} {Name}");
    }
}

public record IlikeSelectItem(string Pattern) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"ILIKE '{Pattern}'");
    }
}