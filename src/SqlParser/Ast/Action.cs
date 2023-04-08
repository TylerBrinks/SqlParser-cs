namespace SqlParser.Ast;

/// <summary>
/// Actions such as create, execute, select, etc.
/// </summary>
public abstract record Action : IWriteSql
{
    public abstract record ColumnAction(Sequence<Ident>? Columns = null) : Action;

    /// <summary>
    /// Connect action
    /// </summary>
    public record Connect : Action;
    /// <summary>
    /// Create action
    /// </summary>
    public record Create : Action;
    /// <summary>
    /// Delete action
    /// </summary>
    public record Delete : Action;
    /// <summary>
    /// Execute action
    /// </summary>
    public record Execute : Action;
    /// <summary>
    /// Insert action
    /// </summary>
    public record Insert(Sequence<Ident>? Columns = null) : ColumnAction(Columns);
    /// <summary>
    /// References action
    /// </summary>
    public record References(Sequence<Ident>? Columns = null) : ColumnAction(Columns);
    /// <summary>
    /// Select action
    /// </summary>
    public record Select(Sequence<Ident>? Columns = null) : ColumnAction(Columns);
    /// <summary>
    /// Temporary action
    /// </summary>
    public record Temporary : Action;
    /// <summary>
    /// Trigger action
    /// </summary>
    public record Trigger : Action;
    /// <summary>
    /// Truncate action
    /// </summary>
    public record Truncate : Action;
    /// <summary>
    /// Update action
    /// </summary>
    public record Update(Sequence<Ident>? Columns) : ColumnAction(Columns);
    /// <summary>
    /// Usage action
    /// </summary>
    public record Usage : Action;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Connect:
                writer.Write(value: "CONNECT");
                break;

            case Create:
                writer.Write(value: "CREATE");
                break;

            case Delete:
                writer.Write(value: "DELETE");
                break;

            case Execute:
                writer.Write(value: "EXECUTE");
                break;

            case Insert:
                writer.Write(value: "INSERT");
                break;

            case References:
                writer.Write(value: "REFERENCES");
                break;

            case Select:
                writer.Write(value: "SELECT");
                break;

            case Temporary:
                writer.Write(value: "TEMPORARY");
                break;

            case Trigger:
                writer.Write(value: "TRIGGER");
                break;

            case Truncate:
                writer.Write(value: "TRUNCATE");
                break;

            case Update:
                writer.Write(value: "UPDATE");
                break;

            case Usage:
                writer.Write(value: "USAGE");
                break;
        }

        if (this is not ColumnAction c)
        {
            return;
        }

        if (c.Columns.SafeAny())
        {
            writer.WriteSql($" ({c.Columns})");
        }
    }
}