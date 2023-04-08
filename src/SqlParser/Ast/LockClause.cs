namespace SqlParser.Ast;

/// <summary>
/// Lock clause
/// </summary>
/// <param name="LockType">Lock type</param>
/// <param name="Of">Lock object Name</param>
/// <param name="NonBlock">Non-block flag</param>
public record LockClause(LockType LockType, NonBlock NonBlock, ObjectName? Of = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"FOR {LockType}");

        if (Of != null)
        {
            writer.Write($" OF {Of}");
        }

        if (NonBlock != NonBlock.None)
        {
            writer.WriteSql($" {NonBlock}");
        }
    }
}