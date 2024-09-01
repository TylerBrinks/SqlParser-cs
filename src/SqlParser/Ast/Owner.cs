namespace SqlParser.Ast;

public abstract record Owner : IWriteSql, IElement
{
    public record Identity(Ident Name) : Owner;
    public record CurrentRole : Owner;
    public record CurrentUser : Owner;
    public record SessionUser : Owner;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Identity id:
                writer.WriteSql($"{id.Name}");
                break;
            case CurrentRole:
                writer.Write("CURRENT_ROLE");
                break;
            case CurrentUser:
                writer.Write("CURRENT_USER");
                break;
            case SessionUser:
                writer.Write("SESSION_USER");
                break;
        }
    }
}
