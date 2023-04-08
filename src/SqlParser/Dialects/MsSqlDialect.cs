namespace SqlParser.Dialects;

/// <summary>
/// MS SQL dialect
///
/// <see href="https://learn.microsoft.com/en-us/sql/t-sql/language-elements/language-elements-transact-sql?view=sql-server-ver16"/>
/// </summary>
public class MsSqlDialect : Dialect
{
    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.DoubleQuote or Symbols.SquareBracketOpen;
    }

    public override bool IsIdentifierStart(char character)
    {
        // See https://docs.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-2017#rules-for-regular-identifiers
        // We don't support non-latin "letters" currently.

        return character.IsLetter() || 
               character is Symbols.Underscore 
                   or Symbols.Num 
                   or Symbols.At;
    }

    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() ||
               character is Symbols.At 
                   or Symbols.Dollar
                   or Symbols.Num 
                   or Symbols.Underscore;
    }
}