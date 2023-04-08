namespace SqlParser.Tokens;

/// <summary>
/// Spaceship operator <=>
/// </summary>
public class Spaceship : StringToken
{
    public Spaceship() : base("<=>")
    {
    }
}