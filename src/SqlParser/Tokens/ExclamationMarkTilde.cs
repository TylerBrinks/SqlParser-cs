namespace SqlParser.Tokens;

/// <summary>
/// `!~` a case sensitive not match regular expression operator in PostgreSQL
/// </summary>
public class ExclamationMarkTilde : StringToken
{
    public ExclamationMarkTilde() : base("!~")
    {
    }
}