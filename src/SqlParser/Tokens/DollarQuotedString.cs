namespace SqlParser.Tokens;

/// <summary>
/// Dollar quoted string: i.e: $$string$$ or $tag_name$string$tag_name$
/// </summary>
public class DollarQuotedString(string value) : StringToken(value)
{
    public string? Tag { get; set; }
}