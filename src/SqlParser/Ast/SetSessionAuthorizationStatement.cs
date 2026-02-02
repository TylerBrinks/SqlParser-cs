namespace SqlParser.Ast;

/// <summary>
/// SET SESSION AUTHORIZATION statement
/// </summary>
public record SetSessionAuthorizationStatement(SetSessionAuthorizationValue Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"SET SESSION AUTHORIZATION {Value}");
    }
}

/// <summary>
/// Value for SET SESSION AUTHORIZATION
/// </summary>
public abstract record SetSessionAuthorizationValue : IWriteSql, IElement
{
    /// <summary>
    /// Specific user name
    /// </summary>
    public record Ident(Ast.Ident Name) : SetSessionAuthorizationValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name}");
        }
    }

    /// <summary>
    /// DEFAULT
    /// </summary>
    public record Default : SetSessionAuthorizationValue
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DEFAULT");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
