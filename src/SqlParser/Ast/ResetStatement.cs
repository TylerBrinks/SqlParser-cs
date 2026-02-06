namespace SqlParser.Ast;

/// <summary>
/// RESET statement
/// </summary>
public abstract record ResetStatement : IWriteSql, IElement
{
    /// <summary>
    /// RESET configuration_parameter
    /// </summary>
    public record Name(ObjectName ConfigName) : ResetStatement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RESET {ConfigName}");
        }
    }

    /// <summary>
    /// RESET ALL
    /// </summary>
    public record All : ResetStatement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("RESET ALL");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
