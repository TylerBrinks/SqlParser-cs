namespace SqlParser.Tokens;

public class TripleDoubleQuotedByteStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"B\"\"\"{Value}\"\"\"";
    }
}