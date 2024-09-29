namespace SqlParser.Dialects;

/// <summary>
/// ANSI SQL dialect
/// </summary>
public class AnsiDialect : Dialect
{
    /// <summary>
    /// Checks if a given character is an ASCII letter 
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier; otherwise false</returns>
    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter();
    }

    /// <summary>
    /// Checks if a given character is an ASCII letter, number, or underscore 
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier part; otherwise false</returns>
    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() || character == Symbols.Underscore;
    }

    public override bool RequireIntervalQualifier => true;

}