using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Tests;

public class CoverageTests : ParserTestBase
{
    [Fact]
    public void Work_Evaluates_Equality()
    {
        var word1 = new Word("test", Symbols.SingleQuote);
        var word2 = new Word("test", Symbols.DoubleQuote);
        var word3 = new Word("fail", Symbols.SingleQuote);
        var word4 = new Word("test", Symbols.SingleQuote);

        Assert.False(word1.Equals(word2));
        Assert.False(word1.Equals(word3));
        Assert.True(word1.Equals(word4));
    }

    [Fact]
    public void Whitespace_Evaluates_Equality()
    {
        var whitespace1 = new Whitespace(WhitespaceKind.InlineComment, "val");
        var whitespace2 = new Whitespace(WhitespaceKind.MultilineComment, "val");
        var whitespace3 = new Whitespace(WhitespaceKind.NewLine, "val");
        var whitespace4 = new Whitespace(WhitespaceKind.Space, "val");
        var whitespace5 = new Whitespace(WhitespaceKind.Tab, "val");
        var whitespace6 = new Whitespace(WhitespaceKind.InlineComment, "fail");
        var whitespace7 = new Whitespace(WhitespaceKind.InlineComment, "val");

        Assert.False(whitespace1.Equals(whitespace2));
        Assert.False(whitespace1.Equals(whitespace3));
        Assert.False(whitespace1.Equals(whitespace4));
        Assert.False(whitespace1.Equals(whitespace5));
        Assert.False(whitespace1.Equals(whitespace6));
        Assert.True(whitespace1.Equals(whitespace7));
    }

    [Fact]
    public void StringToken_Evaluates_Equality()
    {
        var string1 = new SingleQuotedString("test");
        var string2 = new SingleQuotedString("fail");
        var string3 = new SingleQuotedString("test");

        Assert.False(string1.Equals(string2));
        Assert.True(string1.Equals(string3));
        Assert.True(string1.Equals(string3));
    }

    [Fact]
    public void Ampersand_Evaluates_Equality()
    {
        var amp1 = new Ampersand();
        var amp2 = new Ampersand();

        Assert.True(amp1.Equals(amp2));
    }

    [Fact]
    public void Top_Renders_Sql()
    {
        //ar top = new Top(new Expression.LiteralValue(new Value.Number("1")), false, false);
        var top = new Top(new TopQuantity.TopExpression(new Expression.LiteralValue(new Value.Number("1"))), false, false);
        Assert.Equal("TOP (1)", top.ToSql());
    }

    [Fact]
    public void Unnest_WithAlias_ToSql()
    {
        var unnest = new TableFactor.UnNest([new Expression.LiteralValue(new Value.SingleQuotedString("test"))])
        {
            WithOffsetAlias = "hello"
        };

        Assert.Equal("UNNEST('test') AS hello", unnest.ToSql());
    }
}