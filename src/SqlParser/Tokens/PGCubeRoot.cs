namespace SqlParser.Tokens;

/// <summary>
/// `||/`  a cube root math operator in PostgreSQL
/// </summary>
public class PGCubeRoot : StringToken
{
    public PGCubeRoot() : base("||/")
    {
    }
}