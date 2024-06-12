namespace SqlParser.Ast;

/// <summary>
/// Schema name
/// </summary>
public abstract record SchemaName : Statement, IElement
{
    /// <summary>
    /// Only schema name specified: schema name
    /// </summary>
    /// <param name="Name"></param>
    public record Simple(ObjectName Name) : SchemaName
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Name);
        }
    }

    /// <summary>
    /// Only authorization identifier specified: `AUTHORIZATION schema authorization identifier`
    /// </summary>
    /// <param name="Value"></param>
    public record UnnamedAuthorization(Ident Value) : SchemaName
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"AUTHORIZATION {Value}");
        }
    }

    /// <summary>
    /// Both schema name and authorization identifier specified: `schema name  AUTHORIZATION schema authorization identifier`
    /// </summary>
    /// <param name="Name"></param>
    /// <param name="Value"></param>
    public record NamedAuthorization(ObjectName Name, Ident Value) : SchemaName
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"{Name} AUTHORIZATION {Value}");
        }
    }
}