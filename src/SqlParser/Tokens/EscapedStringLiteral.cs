namespace SqlParser.Tokens;

/// <summary>
/// "escaped" string literal, which are an extension to the SQL standard:
/// i.e: e'first \n second' or E 'first \n second'
/// </summary>
public class EscapedStringLiteral(string value) : StringToken(value)
{
    public override string ToString()
    {
        return $"E'{Value}'";
    }
}