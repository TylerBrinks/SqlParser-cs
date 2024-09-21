using SqlParser.Tests.Dialects;

namespace SqlParser.Tests;

public class CustomDialectTests
{
    [Fact]
    public void Custom_Dialect_Renders_Custom_SQL()
    {
        var dialect = new BogusCounterDialect();
        var ast = new Parser().ParseSql("select * from abc", dialect);

        Assert.Equal("Totally Custom SQL", ast.ToSql());
    }

    [Fact]
    public void Custom_Dialect_Parses_Invalid_SQL()
    {
        var dialect = new BogusCounterDialect();
        var ast = new Parser().ParseSql("custom dialects can parse any text!", dialect);

        Assert.Equal("custom dialects can parse any text!", ast.ToSql());
    }
}