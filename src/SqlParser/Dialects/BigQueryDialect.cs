namespace SqlParser.Dialects;

/// <summary>
/// BigQuery SQL Dialect
///
/// <see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/introduction"/>
/// </summary>
public class BigQueryDialect : Dialect
{
    /// <summary>
    /// Checks if a given character is an ASCII letter 
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier; otherwise false</returns>
    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter() || character == Symbols.Underscore;
    }
    /// <summary>
    /// Checks if a given character is an ASCII letter, number, underscore, or minus
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier part; otherwise false</returns>
    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() || character is Symbols.Underscore or Symbols.Minus;
    }
    /// <summary>
    /// Checks if a character is a
    /// </summary>
    /// <param name="character"></param>
    /// <returns></returns>
    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character == Symbols.Backtick;
    }
}