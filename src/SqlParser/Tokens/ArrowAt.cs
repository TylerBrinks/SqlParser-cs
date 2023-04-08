namespace SqlParser.Tokens;

/// <summary>
/// jsonb <@ jsonb -> boolean: Test whether right json contains the left json
/// </summary>
public class ArrowAt : StringToken
{
    public ArrowAt() : base("<@")
    {
    }
}