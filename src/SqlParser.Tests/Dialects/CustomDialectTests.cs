using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using Assert = Xunit.Assert;
using Query = SqlParser.Ast.Query;

namespace SqlParser.Tests.Dialects;

public class CustomDialectTests : ParserTestBase
{
    public CustomDialectTests()
    {
        DefaultDialects = new[] { new ClickHouseDialect() };
    }

    [Fact]
    public void Custom_Prefix_Parser()
    {
        var dialect = new CustomDialect { ParsePrefixAction = ParsePrefix };
        var statements = new Parser().ParseSql("SELECT 1 + 2", dialect);
        var query = statements.First().AsQuery();
        var body = new SetExpression.SelectExpression(new Select(new []
        {
            new SelectItem.UnnamedExpression(new Expression.BinaryOp(
                new Expression.LiteralValue(new Value.Null()),
                BinaryOperator.Plus,
                new Expression.LiteralValue(new Value.Null())
            ))
        }));
        var expected = new Query(body);

        Assert.Equal(expected, query);
    }

    [Fact]
    public void Custom_Infix_Parser()
    {
        var dialect = new CustomDialect { ParseInfixAction = ParseInfix };

        var statements = new Parser().ParseSql("SELECT 1 + 2", dialect);
        var query = statements.First();
        var body = new SetExpression.SelectExpression(new Select(new []
        {
            new SelectItem.UnnamedExpression(new Expression.BinaryOp(
                new Expression.LiteralValue(Number("1")),
                BinaryOperator.Multiply,
                new Expression.LiteralValue(Number("2"))
            ))
        }));
        var expected = new Query(body);

        Assert.Equal(expected, query);
    }

    [Fact]
    public void Custom_Statement_Parser()
    {
        var dialect = new CustomDialect { ParseStatementAction = ParseStatement };

        var statements = new Parser().ParseSql("SELECT 1 + 2", dialect);
        var query = statements.First();
        Assert.IsType<Statement.Commit>(query);
        Assert.True(((Statement.Commit)query).Chain);
    }

    [Fact]
    public void Test_Map_Syntax_Not_Support_Default()
    {
        var dialect = new CustomDialect();
        Assert.Throws<ParserException>(() => new Parser().ParseSql("SELECT MAP {1: 2}", dialect));

    }

    private static Expression? ParsePrefix(Parser parser)
    {
        return parser.ConsumeToken<Number>()
            ? new Expression.LiteralValue(new Value.Null()) 
            : null;
    }

    private static Expression? ParseInfix(Parser parser, Expression expr, int precedence)
    {
        if (parser.ConsumeToken<Plus>())
        {
            return new Expression.BinaryOp(
                expr,
                BinaryOperator.Multiply, // translate Plus to Multiply
                parser.ParseExpr()
            );
        }

        return null;
    }

    private static Statement? ParseStatement(Parser parser)
    {
        if (parser.ParseKeyword(Keyword.SELECT))
        {
            for (var i = 0; i < 3; i++)
            {
                parser.NextToken();
            }

            return new Statement.Commit(true);
        }

        return null;
    }
}

public class CustomDialect : Dialect
{
    public Func<Parser, Statement?>? ParseStatementAction { get; set; }
    public Func<Parser, Expression?>? ParsePrefixAction { get; set; }
    public Func<Parser, Expression, int, Expression?>? ParseInfixAction { get; set; }

    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter() ||
               character.IsDigit() ||
               character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return character.IsLetter() ||
               character.IsDigit() ||
               character is Symbols.Underscore
                   or Symbols.Dollar;
    }

    public override Expression? ParsePrefix(Parser parser)
    {
        return ParsePrefixAction?.Invoke(parser);
    }

    public override Expression? ParseInfix(Parser parser, Expression expr, int precedence)
    {
        return ParseInfixAction?.Invoke(parser, expr, precedence);
    }

    public override Statement? ParseStatement(Parser parser)
    {
        return ParseStatementAction?.Invoke(parser);
    }
}

//public class MyDialect : Dialect
//{
//    public override bool IsIdentifierStart(char character)
//    {
//        return character.IsLetter() ||
//               character.IsDigit() ||
//               character is Symbols.Underscore;
//    }

//    public override bool IsIdentifierPart(char character)
//    {
//        return character.IsLetter() ||
//               character.IsDigit() ||
//               character is Symbols.Underscore
//                   or Symbols.Dollar;
//    }
//}