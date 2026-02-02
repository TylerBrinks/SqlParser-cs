namespace SqlParser.Ast;

/// <summary>
/// ALTER CONNECTOR statement
/// </summary>
public record AlterConnectorStatement(
    Ident Name,
    AlterConnectorOperation Operation) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"ALTER CONNECTOR {Name} {Operation}");
    }
}

/// <summary>
/// ALTER CONNECTOR operation
/// </summary>
public abstract record AlterConnectorOperation : IWriteSql, IElement
{
    /// <summary>
    /// SET OPTIONS (...)
    /// </summary>
    public record SetOptions(Sequence<SqlOption> Options) : AlterConnectorOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET OPTIONS ({Options.ToSqlDelimited()})");
        }
    }

    /// <summary>
    /// SET CREDENTIAL credential_name
    /// </summary>
    public record SetCredential(Ident CredentialName) : AlterConnectorOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET CREDENTIAL {CredentialName}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
