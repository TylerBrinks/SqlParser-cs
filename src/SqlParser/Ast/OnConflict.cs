namespace SqlParser.Ast;

/// <summary>
/// On conflict statement
/// </summary>
/// <param name="OnConflictAction">On conflict action</param>
/// <param name="ConflictTarget">Conflict target</param>
public record OnConflict(OnConflictAction OnConflictAction, ConflictTarget? ConflictTarget = null) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write(" ON CONFLICT");
        if (ConflictTarget != null)
        {
            ConflictTarget.ToSql(writer);
        }
        writer.WriteSql($" {OnConflictAction}");
    }
}

/// <summary>
/// On conflict action
/// </summary>
public abstract record OnConflictAction : IWriteSql, IElement
{
    /// <summary>
    /// Do nothing on conflict
    /// </summary>
    public record DoNothing : OnConflictAction;
    /// <summary>
    /// Update on conflict
    /// </summary>
    /// <param name="DoUpdateAction">Do update instruction</param>
    public record DoUpdate(DoUpdateAction DoUpdateAction) : OnConflictAction;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case DoNothing:
                writer.Write("DO NOTHING");
                break;

            case DoUpdate d:
                writer.Write("DO UPDATE");
                if (d.DoUpdateAction.Assignments.SafeAny())
                {
                    writer.WriteSql($" SET {d.DoUpdateAction.Assignments}");
                }

                if (d.DoUpdateAction.Selection != null)
                {
                    writer.WriteSql($" WHERE {d.DoUpdateAction.Selection}");
                }
                break;
        }
    }
}