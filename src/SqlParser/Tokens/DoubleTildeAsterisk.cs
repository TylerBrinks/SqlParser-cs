namespace SqlParser.Tokens;

/// <summary>
/// `~~*`, a case insensitive match pattern operator in PostgreSQL
/// </summary>
public class DoubleTildeAsterisk : StringToken
{
    public DoubleTildeAsterisk() : base("~~*")
    {
    }
}