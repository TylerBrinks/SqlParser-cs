namespace SqlParser.Tokens;

/// <summary>
/// Double quoted string: i.e: "string"
/// </summary>
public class DoubleQuotedString : StringToken
{
    public DoubleQuotedString(string value) : base(value) { }

    public override string ToString()
    {
        return $"\"{Value}\"";
    }
}