namespace SqlParser.Dialects;

/// <summary>
/// Base dialect definition
/// </summary>
public partial class Dialect
{
    /// <summary>
    /// True if the dialect supports filtering during aggregation
    /// </summary>
    /// <returns>True if supported; otherwise false.</returns>
    public virtual bool SupportsFilterDuringAggregation => false;
    /// <summary>
    /// True if the dialect supports `(NOT) in ()`                                
    /// </summary>
    public virtual bool SupportsInEmptyList => false;
    /// <summary>
    /// True if the dialect supports 'group sets, rollup, or cube' expressions
    /// </summary>
    public virtual bool SupportsGroupByExpression => false;
    /// <summary>
    /// Returns true if the dialect supports `SUBSTRING(expr [FROM start] [FOR len])` expressions
    /// </summary>
    public virtual bool SupportsSubstringFromForExpression => true;
    /// <summary>
    /// Returns true if the dialect has a CONVERT function which accepts a type first
    /// and an expression second, e.g. `CONVERT(varchar, 1)`
    /// </summary>
    public virtual bool ConvertTypeBeforeValue => false;
    /// <summary>
    /// Returns true if the dialect supports `BEGIN {DEFERRED | IMMEDIATE | EXCLUSIVE} [TRANSACTION]` statements
    /// </summary>
    public virtual bool SupportsStartTransactionModifier => false;
    /// <summary>
    /// Returns true if the dialect supports function arguments with equals operator
    /// </summary>
    public virtual bool SupportsNamedFunctionArgsWithEqOperator => false;
    /// <summary>
    /// Returns true if the dialect supports backslash escaping
    /// </summary>
    public virtual bool SupportsStringLiteralBackslashEscape => false;
    /// <summary>
    /// Returns true if the dialect supports match recognize
    /// </summary>
    public virtual bool SupportsMatchRecognize => false;
    /// <summary>
    /// Returns true if the dialect supports dictionary syntax
    /// </summary>
    public virtual bool SupportsDictionarySyntax => false;
    /// <summary>
    /// Returns true if the dialect supports connect by syntax
    /// </summary>
    public virtual bool SupportsConnectBy => false;
    public virtual bool SupportsWindowClauseNamedWindowReference => false;
    /// <summary>
    /// Returns true if the dialect supports identifiers starting with a numeric
    /// prefix such as tables named: '59901_user_login'
    /// </summary>
    public virtual bool SupportsNumericPrefix => false;
    public virtual bool SupportsWindowFunctionNullTreatmentArg => false;
    public virtual bool SupportsLambdaFunctions => false;
    public virtual bool SupportsParenthesizedSetVariables => false;
    public virtual bool SupportsTripleQuotedString => false;
    public virtual bool SupportsSelectWildcardExcept => false;
    public virtual bool SupportsTrailingCommas => false;
    public virtual bool SupportsProjectionTrailingCommas => false;
    public virtual bool SupportMapLiteralSyntax => false;
    public virtual bool SupportsUnicodeStringLiteral => false;
    public virtual bool DescribeRequiresTableKeyword => false;
    public virtual bool AllowExtractCustom => false;
    public virtual bool AllowExtractSingleQuotes => false;
    public virtual bool SupportsCreateIndexWithClause => false;
    public virtual bool RequireIntervalQualifier => false;
    public virtual bool SupportsExplainWithUtilityOptions => false;
}