namespace SqlParser.Ast;

public interface IIfNotExists
{
    public const string IfNotExistsPhrase = "IF NOT EXISTS";

    bool IfNotExists { get; init; }

    string? IfNotExistsText => IfNotExists ? $"{IfNotExistsPhrase}" : null;
}