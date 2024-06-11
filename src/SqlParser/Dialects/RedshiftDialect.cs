namespace SqlParser.Dialects;

/// <summary>
/// In most cases the redshift dialect is identical to [`PostgresSqlDialect`].
/// Notable differences:
/// 1. Redshift treats brackets `[` and `]` differently. For example, `SQL SELECT a[1][2] FROM b`
/// in the Postgres dialect, the query will be parsed as an array, while in the Redshift dialect it will
/// be a json path
///
/// <see href="https://docs.aws.amazon.com/redshift/latest/dg/c_SQL_reference.html"/>
/// </summary>
public class RedshiftDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return char.IsLetter(character) || character is Symbols.Underscore or Symbols.Num;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetterOrDigit(character) || character is Symbols.Dollar or Symbols.Underscore or Symbols.Num;
    }

    public override bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.DoubleQuote or Symbols.SquareBracketOpen;
    }
    /// Determine if quoted characters are proper for identifier
    /// It's needed to distinguish treating square brackets as quotes from
    /// treating them as json path. If there is identifier then we assume
    /// there is no json path.
    public override bool IsProperIdentifierInsideQuotes(State state)
    {
        state.Next();
        var notWhiteChar = state.SkipWhile(c => string.IsNullOrWhiteSpace(c.ToString()));

        return notWhiteChar != null && IsIdentifierPart(notWhiteChar.Value);
    }

    public override bool ConvertTypeBeforeValue => true;
    public virtual bool SupportsConnectBy => true;
}