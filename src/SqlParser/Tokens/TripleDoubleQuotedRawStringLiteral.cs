namespace SqlParser.Tokens;

public class TripleDoubleQuotedRawStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"R\"\"\"{Value}\"\"\"";
    }
}