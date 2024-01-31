namespace SqlParser.Tokens;

/// <summary>
/// Double quoted string: i.e: "string"
/// </summary>
public class DoubleQuotedString(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"\"{Value}\"";
    }
}