namespace SqlParser.Tokens;

public class DoubleQuotedRawStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"R\"{Value}\"";
    }
}