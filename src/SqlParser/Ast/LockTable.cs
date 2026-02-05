namespace SqlParser.Ast;

public record LockTable(Ident Table, Ident? Alias, LockTableType LockTableType) : IWriteSql, IElement
{
    /// <summary>
    /// For PostgreSQL LOCK TABLE syntax with IN ... MODE
    /// </summary>
    public bool PostgresMode { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Table}");

        if (Alias != null)
        {
            writer.WriteSql($" AS {Alias}");
        }

        if (PostgresMode)
        {
            writer.WriteSql($" IN {LockTableType} MODE");
        }
        else
        {
            writer.WriteSql($" {LockTableType}");
        }
    }
}

public abstract record LockTableType : IWriteSql, IElement
{
    public record Read(bool Local) : LockTableType;
    public record Write(bool LowPriority) : LockTableType;
    // PostgreSQL lock modes
    public record AccessShare : LockTableType;
    public record RowShare : LockTableType;
    public record RowExclusive : LockTableType;
    public record ShareUpdateExclusive : LockTableType;
    public record Share : LockTableType;
    public record ShareRowExclusive : LockTableType;
    public record Exclusive : LockTableType;
    public record AccessExclusive : LockTableType;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Read r:
                writer.Write("READ");

                if (r.Local)
                {
                    writer.Write(" LOCAL");
                }

                break;
            case Write w:

                if (w.LowPriority)
                {
                    writer.Write("LOW_PRIORITY ");
                }

                writer.Write("WRITE");

                break;
            case AccessShare:
                writer.Write("ACCESS SHARE");
                break;
            case RowShare:
                writer.Write("ROW SHARE");
                break;
            case RowExclusive:
                writer.Write("ROW EXCLUSIVE");
                break;
            case ShareUpdateExclusive:
                writer.Write("SHARE UPDATE EXCLUSIVE");
                break;
            case Share:
                writer.Write("SHARE");
                break;
            case ShareRowExclusive:
                writer.Write("SHARE ROW EXCLUSIVE");
                break;
            case Exclusive:
                writer.Write("EXCLUSIVE");
                break;
            case AccessExclusive:
                writer.Write("ACCESS EXCLUSIVE");
                break;
        }
    }
}
