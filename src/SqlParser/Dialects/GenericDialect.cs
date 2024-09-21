namespace SqlParser.Dialects;

/// <summary>
/// Generic SQL dialect
/// </summary>
public class GenericDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return char.IsLetter(character) ||
               character is Symbols.Underscore
                   or Symbols.Num
                   or Symbols.At;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetter(character) ||
               char.IsDigit(character) ||
               character is Symbols.At 
                   or Symbols.Dollar 
                   or Symbols.Num 
                   or Symbols.Underscore;
    }

    public override bool SupportsGroupByExpression => true;
    public override bool SupportsConnectBy => true;
    public override bool SupportsMatchRecognize => true;
    public override bool SupportsStartTransactionModifier => true;
    public override bool SupportsWindowFunctionNullTreatmentArg => true;
    public override bool SupportsDictionarySyntax => true;
    public override bool SupportsWindowClauseNamedWindowReference => true;
    public override bool SupportsParenthesizedSetVariables => true;
    public override bool SupportsSelectWildcardExcept => true;
    public override bool SupportMapLiteralSyntax => true;
    public override bool SupportsUnicodeStringLiteral => true;
    public override bool AllowExtractCustom => true;
    public override bool AllowExtractSingleQuotes => true;
    public override bool SupportsCreateIndexWithClause => true;
}