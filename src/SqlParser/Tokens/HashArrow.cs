namespace SqlParser.Tokens;

/// <summary>
/// #> Extracts JSON sub-object at the specified path
/// </summary>
public class HashArrow : StringToken
{
    public HashArrow() :base("#>")
    {
    }
}