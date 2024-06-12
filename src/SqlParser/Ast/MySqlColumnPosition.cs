namespace SqlParser.Ast;

public abstract record MySqlColumnPosition : IWriteSql, IElement
{
    public record First : MySqlColumnPosition;

    public record After(Ident ColumnName) : MySqlColumnPosition;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case First:
                writer.Write("FIRST");
                break;

            case After a:
                writer.WriteSql($"AFTER {a.ColumnName}");
                break;
        }
    }
}
