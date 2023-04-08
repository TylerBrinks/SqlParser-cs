namespace SqlParser.Ast;

/// <summary>
/// Conflict targets
/// </summary>
public abstract record ConflictTarget : IWriteSql
{
    /// <summary>
    /// Column conflict targets
    /// </summary>
    /// <param name="Columns">Column name identifiers</param>
    public record Column(Sequence<Ident> Columns) : ConflictTarget;
    /// <summary>
    /// On Constraint conflict target
    /// </summary>
    /// <param name="Name">Object name</param>
    public record OnConstraint(ObjectName Name) : ConflictTarget, IElement;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Column c:
                writer.WriteSql($"({c.Columns})");
                break;

            case OnConstraint oc:
                writer.WriteSql($" ON CONSTRAINT {oc.Name}");
                break;
        }
    }
}