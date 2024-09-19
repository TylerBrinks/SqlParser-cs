namespace SqlParser.Ast;

public abstract record Use : IWriteSql, IElement
{
    public record Catalog(ObjectName Name) : Use;
    public record Schema(ObjectName Name) : Use;
    public record Database(ObjectName Name) : Use;
    public record Warehouse(ObjectName Name) : Use;
    public record Object(ObjectName Name) : Use;
    public record Default : Use;

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("USE ");
        switch (this)
        {
            case Catalog c:
                writer.WriteSql($"CATALOG {c.Name}");
                break;

            case Schema s:
                writer.WriteSql($"SCHEMA {s.Name}");
                break;

            case Database d:
                writer.WriteSql($"DATABASE {d.Name}");
                break;

            case Warehouse w:
                writer.WriteSql($"WAREHOUSE {w.Name}");
                break;

            case Object o:
                writer.WriteSql($"{o.Name}");
                break;

            case Default:
                writer.WriteSql($"DEFAULT");
                break;
        }
    }
}
