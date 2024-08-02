namespace SqlParser.Tokens;

public class TripleSingleQuotedString(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"'''{Value}'''";
    }
}