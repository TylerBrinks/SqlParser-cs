namespace SqlParser.Tokens;

/// <summary>
/// jsonb @> jsonb -> boolean: Test whether left json contains the right json
/// </summary>
public class AtArrow : StringToken
{
    public AtArrow() : base("@>")
    {
    }
}