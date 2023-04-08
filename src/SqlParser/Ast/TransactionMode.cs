namespace SqlParser.Ast;

/// <summary>
/// Transaction mode
/// </summary>
public abstract record TransactionMode : IWriteSql
{
    public record AccessMode(TransactionAccessMode TransactionAccessMode) : TransactionMode;

    public record IsolationLevel(TransactionIsolationLevel TransactionIsolationLevel) : TransactionMode;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is AccessMode a)
        {
            writer.WriteSql($"{a.TransactionAccessMode}");
        }
        else if (this is IsolationLevel i)
        {
            writer.WriteSql($"ISOLATION LEVEL {i.TransactionIsolationLevel}");
        }
    }
}