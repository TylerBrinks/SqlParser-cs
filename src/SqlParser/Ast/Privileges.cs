namespace SqlParser.Ast;

/// <summary>
/// Privileges
/// </summary>
public abstract record Privileges : IWriteSql
{
    /// <summary>
    /// All privileges applicable to the object type
    /// </summary>
    /// <param name="WithPrivilegesKeyword"></param>
    public record All(bool WithPrivilegesKeyword) : Privileges;
    /// <summary>
    /// Specific privileges (e.g. `SELECT`, `INSERT`)
    /// </summary>
    /// <param name="Privileges"></param>
    public record Actions(Sequence<Action> Privileges) : Privileges;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is All all)
        {
            var withPrivileges = all.WithPrivilegesKeyword ? " PRIVILEGES" : null;
            writer.Write($"ALL{withPrivileges}");
        }
        else if(this is Actions actions)
        {
            //writer.WriteSqlObject(actions.Privileges);
            writer.WriteSql($"{actions.Privileges}");
        }
    }
}