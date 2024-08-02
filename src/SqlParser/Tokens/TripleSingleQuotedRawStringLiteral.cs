namespace SqlParser.Tokens;

public class TripleSingleQuotedRawStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"R'''{Value}'''";
    }
}