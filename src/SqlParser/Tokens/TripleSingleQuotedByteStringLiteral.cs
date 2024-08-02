namespace SqlParser.Tokens;

public class TripleSingleQuotedByteStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"B'''{Value}'''";
    }
}