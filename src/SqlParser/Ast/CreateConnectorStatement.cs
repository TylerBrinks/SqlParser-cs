namespace SqlParser.Ast;

/// <summary>
/// CREATE CONNECTOR statement
/// </summary>
public record CreateConnectorStatement(
    Ident Name,
    ObjectName ConnectorType) : IWriteSql, IElement
{
    public bool IfNotExists { get; init; }
    public Sequence<SqlOption>? Options { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("CREATE CONNECTOR ");

        if (IfNotExists)
        {
            writer.Write("IF NOT EXISTS ");
        }

        writer.WriteSql($"{Name} TYPE {ConnectorType}");

        if (Options.SafeAny())
        {
            writer.WriteSql($" OPTIONS ({Options.ToSqlDelimited()})");
        }
    }
}
