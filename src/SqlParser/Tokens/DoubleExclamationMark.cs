namespace SqlParser.Tokens;

/// <summary>
/// Double Exclamation Mark `!!` used for PostgreSQL prefix factorial operator
/// </summary>
public class DoubleExclamationMark : StringToken
{
    public DoubleExclamationMark() : base("!!")
    {
    }
}