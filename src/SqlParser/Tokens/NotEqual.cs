namespace SqlParser.Tokens;

/// <summary>
/// Not Equals operator `<>` (or `!=` in some dialects)
/// </summary>
public class NotEqual : StringToken
{
    public NotEqual() : base("<>")
    {
    }
}