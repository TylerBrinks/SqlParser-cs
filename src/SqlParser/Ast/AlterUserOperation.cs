namespace SqlParser.Ast;

/// <summary>
/// ALTER USER operation types
/// </summary>
public abstract record AlterUserOperation : IWriteSql, IElement
{
    /// <summary>
    /// SET options operation
    /// </summary>
    public record Set(Sequence<KeyValueOption> Options) : AlterUserOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET {Options.ToSqlDelimited(Symbols.Space)}");
        }
    }

    /// <summary>
    /// RESET options operation
    /// </summary>
    public record Reset(Sequence<Ident> Options) : AlterUserOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RESET {Options.ToSqlDelimited(Symbols.Space)}");
        }
    }

    /// <summary>
    /// RENAME TO operation
    /// </summary>
    public record RenameTo(ObjectName NewName) : AlterUserOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {NewName}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Key-value option pair for ALTER USER SET
/// </summary>
public record KeyValueOption(Ident Key, Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Key} = {Value}");
    }
}
