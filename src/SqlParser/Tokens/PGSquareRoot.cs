namespace SqlParser.Tokens;

/// <summary>
/// `|/` a square root math operator in PostgreSQL
/// </summary>
public class PGSquareRoot : StringToken
{
    public PGSquareRoot() : base("|/")
    {
    }
}