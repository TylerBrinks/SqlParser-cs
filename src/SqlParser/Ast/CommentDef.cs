namespace SqlParser.Ast;

public abstract record CommentDef(string? Comment)
{
    public record WithEq(string? Comment) : CommentDef(Comment);
    public record WithoutEq(string? Comment) : CommentDef(Comment);
    public record AfterColumnDefsWithoutEq(string? Comment) : CommentDef(Comment);
}
