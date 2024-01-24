namespace SqlParser.Tokens;

/// <summary>
/// `~~`, a case sensitive match pattern operator in PostgreSQL
/// </summary>
public class DoubleTilde : StringToken
{
    public DoubleTilde() : base("~~")
    {
    }
}