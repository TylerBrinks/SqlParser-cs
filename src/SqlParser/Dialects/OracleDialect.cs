using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// Oracle dialect
///
/// <see href="https://docs.oracle.com/en/database/oracle/oracle-database/"/>
/// </summary>
public class OracleDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return char.IsLetter(character) || character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetterOrDigit(character) || character is Symbols.Dollar or Symbols.Underscore or Symbols.Num;
    }

    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character == Symbols.DoubleQuote;
    }

    public override char? IdentifierQuoteStyle(string identifier) => Symbols.DoubleQuote;

    public override bool SupportsConnectBy => true;
    public override bool SupportsFilterDuringAggregation => true;
}
