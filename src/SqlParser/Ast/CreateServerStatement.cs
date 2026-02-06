namespace SqlParser.Ast;

/// <summary>
/// CREATE SERVER statement - PostgreSQL
/// </summary>
public record CreateServerStatement(
    Ident ServerName,
    ObjectName ForeignDataWrapperName) : IWriteSql, IElement
{
    public bool IfNotExists { get; init; }
    public Ident? ServerType { get; init; }
    public Ident? ServerVersion { get; init; }
    public Sequence<SqlOption>? Options { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("CREATE SERVER ");

        if (IfNotExists)
        {
            writer.Write("IF NOT EXISTS ");
        }

        writer.WriteSql($"{ServerName}");

        if (ServerType != null)
        {
            writer.WriteSql($" TYPE {ServerType}");
        }

        if (ServerVersion != null)
        {
            writer.WriteSql($" VERSION {ServerVersion}");
        }

        writer.WriteSql($" FOREIGN DATA WRAPPER {ForeignDataWrapperName}");

        if (Options.SafeAny())
        {
            writer.WriteSql($" OPTIONS ({Options.ToSqlDelimited()})");
        }
    }
}
