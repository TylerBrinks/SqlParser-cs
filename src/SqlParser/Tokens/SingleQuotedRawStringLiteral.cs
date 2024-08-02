namespace SqlParser.Tokens;

public class SingleQuotedRawStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"R'{Value}'";
    }
}