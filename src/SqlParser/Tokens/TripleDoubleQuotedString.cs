namespace SqlParser.Tokens;

public class TripleDoubleQuotedString(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"\"\"\"{Value}\"\"\"";
    }
}