namespace SqlParser.Ast;

public abstract record FlushLocation : IWriteSql, IElement
{
    public record NoWriteToBinlog : FlushLocation;
    public record Local : FlushLocation;

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write(this is NoWriteToBinlog ? "NO_WRITE_TO_BINLOG" : "LOCAL");
    }
}