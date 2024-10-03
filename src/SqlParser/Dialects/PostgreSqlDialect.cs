using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// PostgreSql dialect
///
/// <see href="https://www.postgresql.org/docs/current/sql-syntax.html"/>
/// </summary>
public class PostgreSqlDialect : Dialect
{
    private const short DoubleColonPrecedence = 140;
    private const short BracketPrecedence = 130;
    private const short CollatePrecedence = 120;
    private const short AtTimeZonePrecedence = 110;
    private const short CaretPrecedence = 100;
    private const short MulDivModOpPrecedence = 90;
    private const short PlusMinusPrecedence = 80;
    private const short XOrPrecedence = 75;
    private const short PgOtherPrecedence = 70;
    private const short BetweenLikePrecedence = 60;
    private const short EqualPrecedence = 50;
    private const short IsPrecedence = 40;
    private const short AndPrecedence = 20;
    private const short OrPrecedence = 10;

    // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
    // We don't yet support identifiers beginning with "letters with
    public override bool IsIdentifierStart(char character)
    {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        return char.IsLetter(character) || character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return char.IsLetterOrDigit(character) || character is Symbols.Dollar or Symbols.Underscore;
    }

    public override bool SupportsFilterDuringAggregation => true;

    public override char? IdentifierQuoteStyle(string identifier) => Symbols.DoubleQuote;

    public override bool IsDelimitedIdentifierStart(char character) => character is Symbols.DoubleQuote;

    public override bool IsCustomOperatorPart(char character) => character
            is Symbols.Plus
            or Symbols.Minus
            or Symbols.Asterisk
            or Symbols.Divide
            or Symbols.LessThan
            or Symbols.GreaterThan
            or Symbols.Equal
            or Symbols.Tilde
            or Symbols.ExclamationMark
            or Symbols.At
            or Symbols.Num
            or Symbols.Percent
            or Symbols.Caret
            or Symbols.Ampersand
            or Symbols.Pipe
            or Symbols.Backtick
            or Symbols.QuestionMark;

    public override Statement? ParseStatement(Parser parser)
    {
        return parser.ParseKeyword(Keyword.COMMENT) ? ParseComment(parser) : null;
    }

    public static Statement.Comment ParseComment(Parser parser)
    {
        var ifExists = parser.ParseIfExists();

        parser.ExpectKeyword(Keyword.ON);
        var token = parser.NextToken();
        CommentObject objectType;
        ObjectName name;

        switch (token)
        {
            case Word { Keyword: Keyword.COLUMN }:
                objectType = CommentObject.Column;
                name = parser.ParseObjectName();
                break;

            case Word { Keyword: Keyword.TABLE }:
                objectType = CommentObject.Table;
                name = parser.ParseObjectName();
                break;

            default:
                throw Parser.Expected("comment object_type", token);
        }

        parser.ExpectKeyword(Keyword.IS);

        var comment = parser.ParseKeyword(Keyword.NULL) ? null : parser.ParseLiteralString();

        return new Statement.Comment(name, objectType, comment, ifExists);
    }

    public override short GetPrecedence(Precedence precedence)
    {
        if (precedence == Precedence.Between)
        {
            return BetweenLikePrecedence;
        }

        return base.GetPrecedence(precedence);
    }

    public override short? GetNextPrecedence(Parser parser)
    {
        var token = parser.PeekToken();

        return token switch
        {
            Word { Keyword: Keyword.OR } => OrPrecedence,
            Word { Keyword: Keyword.XOR } => XOrPrecedence,
            Word { Keyword: Keyword.AND } => AndPrecedence,
            Word { Keyword: Keyword.AT } => GetAtPrecedence(),
            Word { Keyword: Keyword.NOT } => GetNotPrecedence(),

            Word { Keyword: Keyword.IS } => IsPrecedence,
            Word
            {
                Keyword: Keyword.IN
                or Keyword.BETWEEN
                or Keyword.LIKE
                or Keyword.ILIKE
                or Keyword.RLIKE
                or Keyword.REGEXP
                or Keyword.SIMILAR
                or Keyword.OPERATOR
            } => BetweenLikePrecedence,
            Word { Keyword: Keyword.DIV } => MulDivModOpPrecedence,
            Word { Keyword: Keyword.COLLATE } => CollatePrecedence,

            Equal
                or LessThan
                or LessThanOrEqual
                or NotEqual
                or GreaterThan
                or GreaterThanOrEqual
                or DoubleEqual
                or Tilde
                or TildeAsterisk
                or ExclamationMarkTilde
                or ExclamationMarkTildeAsterisk
                or DoubleTilde
                or DoubleTildeAsterisk
                or ExclamationMarkDoubleTilde
                or ExclamationMarkDoubleTildeAsterisk
                or Spaceship => EqualPrecedence,

            Caret => CaretPrecedence,
            Plus or Minus => PlusMinusPrecedence,
            Multiply
                or Divide
                or Modulo
                => MulDivModOpPrecedence,
            DoubleColon => DoubleColonPrecedence,
            LeftBracket => BracketPrecedence,

            Arrow
                or LongArrow
                or HashArrow
                or HashLongArrow
                or AtArrow
                or ArrowAt
                or HashMinus
                or AtQuestion
                or AtAt
                or Question
                or QuestionAnd
                or QuestionPipe
                or ExclamationMark
                or Overlap
                or CaretAt
                or StringConcat
                or Hash
                or ShiftRight
                or ShiftLeft
                or Pipe
                or Ampersand
                or CustomBinaryOperator
                => PgOtherPrecedence,

            _ => 0
        };

        short GetAtPrecedence()
        {
            if (parser.PeekNthToken(1) is Word { Keyword: Keyword.TIME } &&
                parser.PeekNthToken(2) is Word { Keyword: Keyword.ZONE })
            {
                return AtTimeZonePrecedence;
            }

            return 0;
        }

        // The precedence of NOT varies depending on keyword that
        // follows it. If it is followed by IN, BETWEEN, or LIKE,
        // it takes on the precedence of those tokens. Otherwise, it
        // is not an infix operator, and therefore has zero
        // precedence.
        short GetNotPrecedence()
        {
            return parser.PeekNthToken(1) switch
            {
                Word
                {
                    Keyword: Keyword.IN
                    or Keyword.BETWEEN
                    or Keyword.LIKE
                    or Keyword.ILIKE
                    or Keyword.SIMILAR
                    or Keyword.REGEXP
                    or Keyword.RLIKE
                } => BetweenLikePrecedence,

                _ => 0
            };
        }
    }
    public override bool SupportsGroupByExpression => true;
    public override bool SupportsUnicodeStringLiteral => true;
    public override bool AllowExtractCustom => true;
    public override bool AllowExtractSingleQuotes => true;
    public override bool SupportsCreateIndexWithClause => true;
    public override bool SupportsExplainWithUtilityOptions => true;
}