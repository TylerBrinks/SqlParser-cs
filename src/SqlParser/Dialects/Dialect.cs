using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// Base dialect definition
/// </summary>
public abstract class Dialect
{
    /// <summary>
    /// Determine if a character is the start of an identifier
    /// </summary>
    /// <param name="character">Character to test</param>
    /// <returns>True if the start of an ident; otherwise false.</returns>
    public abstract bool IsIdentifierStart(char character);
    /// <summary>
    /// Determine if a character is the part of an identifier
    /// </summary>
    /// <param name="character">Character to test</param>
    /// <returns>True if part of an ident; otherwise false.</returns>
    public abstract bool IsIdentifierPart(char character);
    /// <summary>
    /// Determine if a character starts a quoted identifier. The default
    /// implementation, accepting "double-quoted" ids is both ANSI-compliant
    /// and appropriate for most dialects (with the notable exception of
    /// MySQL, MS SQL, and sqlite). You can accept one of characters listed
    /// in `Word::matching_end_quote` here
    /// </summary>
    /// <param name="character"></param>
    /// <returns>True if ident start; otherwise false</returns>
    public virtual bool IsDelimitedIdentifierStart(char character)
    {
        return character is Symbols.DoubleQuote or Symbols.Backtick;
    }
    /// <summary>
    /// Determine if quoted characters are proper for identifier
    /// </summary>
    /// <returns>True if proper; otherwise false.</returns>
    public virtual bool IsProperIdentifierInsideQuotes(State state)
    {
        return true;
    }
    /// <summary>
    /// Return the character used to quote identifiers
    /// </summary>
    public virtual char? IdentifierQuoteStyle(string identifier) => null;
    /// <summary>
    /// Allow dialect implementations to override statement parsing
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Parsed Expression</returns>
    public virtual Statement? ParseStatement(Parser parser)
    {
        return null;
    }
    /// <summary>
    /// Allow dialect implementations to override prefix parsing
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Parsed Expression</returns>
    public virtual Expression? ParsePrefix(Parser parser)
    {
        return null;
    }
    /// <summary>
    /// Dialect-specific infix parser override
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <param name="expression">Expression</param>
    /// <param name="precedence">Token precedence</param>
    /// <returns>Parsed Expression</returns>
    public virtual Expression? ParseInfix(Parser parser, Expression expression, int precedence)
    {
        return null;
    }
    /// <summary>
    /// Decide the lexical Precedence of operators.
    /// </summary>
    /// <param name="precedence">Precedence to evaluate</param>
    /// <returns>Precedence value</returns>
    public virtual short GetPrecedence(Precedence precedence)
    {
        // https://www.postgresql.org/docs/7.0/operators.htm#AEN2026ExpectRightParen
        return precedence switch
        {
            Precedence.DoubleColon => 50,
            Precedence.AtTz => 41,
            Precedence.MulDivModOp => 40,
            Precedence.PlusMinus => 30,
            Precedence.Xor => 24,
            Precedence.Ampersand => 23,
            Precedence.Caret => 22,
            Precedence.Pipe => 21,
            Precedence.Between => 20,
            Precedence.Eq => 20,
            Precedence.Like => 19,
            Precedence.Is => 17,
            Precedence.PgOther => 16,
            Precedence.UnaryNot => 15,
            Precedence.And => 10,
            Precedence.Or => 5,
            _ => 0
        };
    }
    /// <summary>
    /// Allow dialect implementations to override parser token precedence
    /// </summary>
    /// <param name="parser">Parser instance</param>
    /// <returns>Token Precedence</returns>
    public virtual short? GetNextPrecedence(Parser parser)
    {
        return null;
    }

    public short GetNextPrecedenceDefault(Parser parser)
    {
        var dialectPrecedence = GetNextPrecedence(parser);
        if (dialectPrecedence != null)
        {
            return dialectPrecedence.Value;
        }

        var token = parser.PeekToken();

        // use https://www.postgresql.org/docs/7.0/operators.htm#AEN2026 as a reference
        return token switch
        {
            Word { Keyword: Keyword.OR } => GetPrecedence(Precedence.Or),
            Word { Keyword: Keyword.AND } => GetPrecedence(Precedence.And),
            Word { Keyword: Keyword.XOR } => GetPrecedence(Precedence.Xor),
            Word { Keyword: Keyword.AT } => GetAtPrecedence(),
            Word { Keyword: Keyword.NOT } => GetNotPrecedence(),
            Word { Keyword: Keyword.IS } => GetPrecedence(Precedence.Is),
            Word { Keyword: Keyword.IN or Keyword.BETWEEN or Keyword.OPERATOR } => GetPrecedence(Precedence.Between),
            Word {
                Keyword: Keyword.LIKE 
                or Keyword.ILIKE 
                or Keyword.SIMILAR 
                or Keyword.REGEXP 
                or Keyword.RLIKE } => GetPrecedence(Precedence.Like),
           
            Word { Keyword: Keyword.DIV } => GetPrecedence(Precedence.MulDivModOp),

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
                or Spaceship
                => GetPrecedence(Precedence.Between),

            Pipe => GetPrecedence(Precedence.Pipe),

            Caret
                or Hash
                or ShiftRight
                or ShiftLeft
                => GetPrecedence(Precedence.Caret),

            Ampersand => GetPrecedence(Precedence.Ampersand),
            Plus or Minus => GetPrecedence(Precedence.PlusMinus),

            Multiply
                or Divide
                or DuckIntDiv
                or Modulo
                or StringConcat
                => GetPrecedence(Precedence.MulDivModOp),

            DoubleColon
                //or Colon
                or ExclamationMark
                or LeftBracket
                or Overlap
                or CaretAt
                => GetPrecedence(Precedence.DoubleColon),

            Colon when this is SnowflakeDialect => GetPrecedence(Precedence.DoubleColon),

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
                or CustomBinaryOperator
                => GetPrecedence(Precedence.PgOther),

            _ => 0
        };

        short GetAtPrecedence()
        {
            if (parser.PeekNthToken(1) is Word { Keyword: Keyword.TIME } &&
                parser.PeekNthToken(2) is Word { Keyword: Keyword.ZONE })
            {
                return GetPrecedence(Precedence.AtTz);
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
                Word { Keyword: Keyword.IN or Keyword.BETWEEN } => GetPrecedence(Precedence.Between),
                Word
                {
                    Keyword: Keyword.LIKE or Keyword.ILIKE or Keyword.SIMILAR or Keyword.REGEXP or Keyword.RLIKE
                } => GetPrecedence(Precedence.Like), // LikePrecedence,
                _ => 0
            };
        }
    }
    /// <summary>
    /// Most dialects do not have custom operators.This method to provide custom operators.
    /// </summary>
    /// <param name="character">Character</param>
    /// <returns>True if custom operator part; otherwise false</returns>
    public virtual bool IsCustomOperatorPart(char character)
    {
        return false;
    }
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
}