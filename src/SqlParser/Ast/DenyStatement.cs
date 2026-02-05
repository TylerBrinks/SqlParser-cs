namespace SqlParser.Ast;

/// <summary>
/// SQL Server DENY statement
/// </summary>
public record DenyStatement(
    Privileges Privileges,
    Sequence<Ident> Grantees) : IWriteSql, IElement
{
    public GrantObjects? Objects { get; init; }
    public Ident? GrantedBy { get; init; }
    public bool CascadeOption { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"DENY {Privileges}");

        if (Objects != null)
        {
            writer.WriteSql($" ON {Objects}");
        }

        writer.WriteSql($" TO {Grantees.ToSqlDelimited()}");

        if (CascadeOption)
        {
            writer.Write(" CASCADE");
        }

        if (GrantedBy != null)
        {
            writer.WriteSql($" AS {GrantedBy}");
        }
    }
}
