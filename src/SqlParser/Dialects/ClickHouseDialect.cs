using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// ClickHouse SQL dialect
///
/// <see href="https://clickhouse.com/docs/en/sql-reference/ansi/"/>
/// </summary>
public class ClickHouseDialect : Dialect
{
    public override bool IsIdentifierStart(char character) => character.IsLetter() || character == Symbols.Underscore;

    public override bool IsIdentifierPart(char character) => IsIdentifierStart(character) || character.IsDigit();

    public override bool SupportsStringLiteralBackslashEscape => true;
    public override bool SupportsSelectWildcardExcept => true;
    public override bool DescribeRequiresTableKeyword => true;
    public override bool RequireIntervalQualifier => true;
    public override bool SupportsLimitComma => true;

    public override CommonTableExpression ParseCommonTableExpression(Parser parser)
    {
        if (parser.PeekToken() is LeftParen)
        {
            parser.ConsumeToken<LeftParen>();
            var query = parser.ParseQuery(false);
            parser.ConsumeToken<RightParen>();
            parser.ExpectKeyword(Keyword.AS);
            var identifier = parser.ParseIdentifier();
            var tableAlias = new TableAlias(identifier);
            return new CommonTableExpression(tableAlias, query);
        }

        return parser.ParseCommonTableExpressionInternal();
    }
}