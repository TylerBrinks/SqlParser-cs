namespace SqlParser.Ast;

public abstract record CommentDef(string? Comment) : IWriteSql
{
    public record WithEq(string? Comment) : CommentDef(Comment);
    public record WithoutEq(string? Comment) : CommentDef(Comment);

    public void ToSql(SqlTextWriter writer)
    {
        writer.Write(Comment);
    }
}
