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
        if (parser.PeekToken() is not LeftParen) return parser.ParseCommonTableExpressionInternal();
        parser.ConsumeToken<LeftParen>();
        var query = parser.ParseQuery(false);
        parser.ConsumeToken<RightParen>();
        parser.ExpectKeyword(Keyword.AS);
        var identifier = parser.ParseIdentifier();
        var tableAlias = new TableAlias(identifier);
        return new CommonTableExpression(tableAlias, query);
    }


    public override Statement? ParseStatement(Parser parser)
    {
        if (parser.PeekToken() is not Word { Keyword: Keyword.WITH }) return null;
        parser.NextToken();

        if (parser.PeekToken() is LeftParen)
        {
            parser.NextToken();

            var isQuery = false;
            var peekToken = parser.PeekToken();
            if (peekToken is Word { Keyword: Keyword.SELECT } ||
                peekToken is Word { Keyword: Keyword.WITH } ||
                peekToken is Word { Keyword: Keyword.VALUES } ||
                peekToken is LeftParen)
            {
                isQuery = true;
            }

            if (isQuery)
            {
                parser.PrevToken();
                parser.PrevToken();
                return null;
            }

            var expr = parser.ParseExpr();
            parser.ExpectRightParen();
            parser.ExpectKeyword(Keyword.AS);
            var identifier = parser.ParseIdentifier();

            return new WithStatement(expr, identifier);
        }
        else
        {
            parser.PrevToken();
        }

        return null;
    }

    private record WithStatement : Statement
    {
        private Expression Expression { get; }
        private Ident Identifier { get; }

        public WithStatement(Expression expression, Ident identifier)
        {
            Expression = expression;
            Identifier = identifier;
        }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("WITH (");
            Expression.ToSql(writer);
            writer.Write($") AS {Identifier}");
        }
    }
}