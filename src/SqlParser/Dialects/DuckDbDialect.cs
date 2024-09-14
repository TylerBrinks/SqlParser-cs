namespace SqlParser.Dialects;

/// <summary>
/// DuckDb SQL Dialect
/// </summary>
public class DuckDbDialect : Dialect
{
    /// <summary>
    /// Checks if a given character is an ASCII letter 
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier; otherwise false</returns>
    public override bool IsIdentifierStart(char character)
    {
        return character.IsAlphabetic() || character == Symbols.Underscore;
    }
    /// <summary>
    /// Checks if a given character is an ASCII letter, number, underscore, or minus
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier part; otherwise false</returns>
    public override bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character.IsDigit() || character == Symbols.Dollar;
    }

    /// <summary>
    /// Supports filter during aggregation is always true
    /// </summary>
    /// <returns>True</returns>
    public override bool SupportsFilterDuringAggregation => true;

    public override bool SupportsNamedFunctionArgsWithEqOperator => true;
    public override bool SupportsDictionarySyntax => true;
    public override bool SupportsTrailingCommas => true;
    public override bool SupportMapLiteralSyntax => true;
}