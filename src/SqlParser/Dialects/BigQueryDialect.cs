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
    public override bool IsIdentifierStart(char character) =>  character.IsLetter() || character == Symbols.Underscore;
    /// <summary>
    /// Checks if a given character is an ASCII letter, number, underscore, or minus
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if an identifier part; otherwise false</returns>
    public override bool IsIdentifierPart(char character) => character.IsAlphaNumeric() || character is Symbols.Underscore;
    /// <summary>
    /// Checks if a character is a
    /// </summary>
    /// <param name="character"></param>
    /// <returns></returns>
    public override bool IsDelimitedIdentifierStart(char character) => character == Symbols.Backtick;
    /// <summary>
    /// Returns true if the dialect supports referencing another named window
    /// within a window clause declaration.
    /// </summary>
    public override bool SupportsWindowClauseNamedWindowReference => true;
    public override bool SupportsStringLiteralBackslashEscape => true;
    public override bool SupportsWindowFunctionNullTreatmentArg => true;
    public override bool SupportsTripleQuotedString => true;
    public override bool SupportsSelectWildcardExcept => true;
    public override bool SupportsProjectionTrailingCommas => true;
    public override bool RequireIntervalQualifier => true;
}