namespace SqlParser.Dialects;

/// <summary>
/// Apache HIVE dialect (Flink)
///
/// <see href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/hive/hive_dialect/"/>
/// </summary>
public class HiveDialect : Dialect
{
    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.DoubleQuote or Symbols.Backtick;
    }

    public override bool IsIdentifierStart(char character)
    {
        return character.IsAlphaNumeric() || character == Symbols.Dollar;
    }

    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() ||
               character is Symbols.Underscore or Symbols.Dollar or Symbols.CurlyBracketOpen or Symbols.CurlyBracketClose;
    }

    public override bool SupportsFilterDuringAggregation => true;
    public override bool SupportsNumericPrefix => true;
    public override bool RequireIntervalQualifier => true;
}