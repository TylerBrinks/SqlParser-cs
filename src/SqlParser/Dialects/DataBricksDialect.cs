
namespace SqlParser.Dialects;

public class DatabricksDialect : Dialect
{
    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.Backtick;
    }

    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter() ||
               character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetterOrDigit(character) ||
              character is Symbols.Underscore;
    }

    public override bool SupportsFilterDuringAggregation => true;
    public override bool SupportsGroupByExpression => true;
    public override bool SupportsLambdaFunctions => true;
    public override bool SupportsSelectWildcardExcept => true;
    public override bool RequireIntervalQualifier => true;
}
