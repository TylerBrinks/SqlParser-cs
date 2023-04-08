namespace SqlParser.Tokens;

/// <summary>
/// Equality operator `=`
/// </summary>
public class Equal :SingleCharacterToken
{
    public Equal() : base(Symbols.Equal)
    {
    }
}