using SqlParser.Ast;

namespace SqlParser.Dialects;

/// <summary>
/// SQLite dialect
///
/// <see href="https://www.sqlite.org/lang.html"/>
/// </summary>
public class SQLiteDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter() ||
               // See https://www.sqlite.org/draft/tokenreq.html
               character is >= (char) 0x007f and < Symbols.EndOfFile
                   or Symbols.Underscore
                   //or Symbols.Dollar
                   ;
    }

    public override bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character.IsDigit();
    }
    /// <summary>
    /// Checks if a character is a SQLite identifier delimiter
    /// parses `...`, [...] and "..." as identifier
    /// 
    /// </summary>
    /// <remarks>
    /// <see href="https://www.sqlite.org/lang_keywords.html"/>
    /// </remarks>
    /// <param name="character">Character to evaluate</param>
    /// <returns>True if identifier start; otherwise false</returns>
    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.Backtick or Symbols.DoubleQuote or Symbols.SquareBracketOpen;
    }
    /// <summary>
    /// Handles SQLite Replace statement parsing
    /// </summary>
    public override Statement? ParseStatement(Parser parser)
    {
        if (!parser.ParseKeyword(Keyword.REPLACE))
        {
            return null;
        }

        parser.PrevToken();
        return parser.ParseInsert();
    }
    public override char? IdentifierQuoteStyle(string identifier) => Symbols.Backtick;

    public override bool SupportsFilterDuringAggregation => true;

    public override bool SupportsInEmptyList => true;

    public override bool SupportsStartTransactionModifier => true;
}