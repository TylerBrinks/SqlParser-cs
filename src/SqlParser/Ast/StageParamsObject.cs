namespace SqlParser.Ast;

public record StageParamsObject
{
    public string Url { get; set; }
    public List<DataLoadingOption> Encryption { get; set; }
    public string? Endpoint { get; set; }
    public string? StorageIntegration { get; set; }
    public List<DataLoadingOption> Credentials { get; set; }
}