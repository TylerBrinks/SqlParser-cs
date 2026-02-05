namespace SqlParser.Ast;

/// <summary>
/// PostgreSQL REPLICA IDENTITY type
/// </summary>
public abstract record ReplicaIdentityType : IWriteSql, IElement
{
    /// <summary>
    /// DEFAULT
    /// </summary>
    public record Default : ReplicaIdentityType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DEFAULT");
        }
    }

    /// <summary>
    /// FULL
    /// </summary>
    public record Full : ReplicaIdentityType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FULL");
        }
    }

    /// <summary>
    /// NOTHING
    /// </summary>
    public record Nothing : ReplicaIdentityType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("NOTHING");
        }
    }

    /// <summary>
    /// USING INDEX index_name
    /// </summary>
    public record UsingIndex(Ident IndexName) : ReplicaIdentityType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"USING INDEX {IndexName}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
