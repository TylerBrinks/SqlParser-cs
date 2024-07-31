using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser.Tests.Dialects;

public class ClickhouseDialectTests : ParserTestBase
{
    public ClickhouseDialectTests()
    {
        DefaultDialects = new[] { new ClickHouseDialect() };
    }

    [Fact]
    public void Parse_Map_Access_Expr()
    {
        var select = VerifiedOnlySelect("SELECT string_values[indexOf(string_names, 'endpoint')] FROM foos WHERE id = 'test' AND string_value[indexOf(string_name, 'app')] <> 'foo'");

        var args = new Sequence<FunctionArg>
        {
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("string_names"))),
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.SingleQuotedString("endpoint"))))
        };

        var selectionArgs = new Sequence<FunctionArg>
        {
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("string_name"))),
            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.LiteralValue(new Value.SingleQuotedString("app"))))
        };

        var expected = new Select([
            new SelectItem.UnnamedExpression(new Expression.MapAccess(new Expression.Identifier("string_values"),
            [
                new(new Expression.Function("indexOf")
                {
                    Args = new FunctionArguments.List(new FunctionArgumentList(null, args, null))
                }, 
                MapAccessSyntax.Bracket)
            ]))
        ])
        {
            From = new Sequence<TableWithJoins>
            {
                new(new TableFactor.Table("foos"))
            },
            Selection = new Expression.BinaryOp(
                // Left
                new Expression.BinaryOp(
                    new Expression.Identifier("id"),
                    BinaryOperator.Eq,
                    new Expression.LiteralValue(new Value.SingleQuotedString("test"))),
                // Op
                BinaryOperator.And,
               // Right
               new Expression.BinaryOp(
                    new Expression.MapAccess(new Expression.Identifier("string_value"),
                    [
                        new Expression.MapAccessKey(new Expression.Function("indexOf")
                        {
                            Args = new FunctionArguments.List(new FunctionArgumentList(null, selectionArgs, null))

                        }, MapAccessSyntax.Bracket)


                    ]),
                    BinaryOperator.NotEq,
                    new Expression.LiteralValue(new Value.SingleQuotedString("foo"))
                )
            )
        };

        Assert.Equal(expected, select);
    }

    [Fact]
    public void Parse_Array_Expr()
    {
        var select = VerifiedOnlySelect("SELECT ['1', '2'] FROM test");
        var expected = new Expression.Array(new ArrayExpression([
            new Expression.LiteralValue(new Value.SingleQuotedString("1")),
            new Expression.LiteralValue(new Value.SingleQuotedString("2"))
        ]));

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Array_Fn()
    {
        var select = VerifiedOnlySelect("SELECT array(x1, x2) FROM foo");

        var args = new Sequence<FunctionArg>
        {
             new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("x1"))),
             new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Expression.Identifier("x2")))
        };

        var expected = new Expression.Function("array")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(null, args, null))
        };

        Assert.Equal(expected, select.Projection.First().AsExpr());

    }

    [Fact]
    public void Parse_Kill()
    {
        var statement = VerifiedStatement("KILL MUTATION 5");

        var expected = new Statement.Kill(KillType.Mutation, 5);
        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Delimited_Identifiers()
    {
        var select = VerifiedOnlySelect("""
                        SELECT "alias"."bar baz", "myfun"(), "simple id" AS "column alias" FROM "a table" AS "alias"
                        """);

        var relation = (TableFactor.Table)select.From!.First().Relation!;

        Assert.Equal(new Ident("a table", Symbols.DoubleQuote), relation.Name);
        Assert.Equal(new Ident("alias", Symbols.DoubleQuote), relation.Alias!.Name);

        Assert.Equal(3, select.Projection.Count);
        var compound = new Expression.CompoundIdentifier([
            new Ident("alias", Symbols.DoubleQuote),
            new Ident("bar baz", Symbols.DoubleQuote)
        ]);
        Assert.Equal(compound, select.Projection[0].AsExpr());

        var fn = new Expression.Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
        {
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        };
        Assert.Equal(fn, select.Projection[1].AsExpr());

        var exp = (SelectItem.ExpressionWithAlias)select.Projection[2];
        var simpleId = new Expression.Identifier(new Ident("simple id", Symbols.DoubleQuote));
        var quote = new Ident("column alias", Symbols.DoubleQuote);
        Assert.Equal(simpleId, exp.Expression);
        Assert.Equal(quote, exp.Alias);

        VerifiedStatement("""
                        CREATE TABLE "foo" ("bar" "int")
                        """);

        VerifiedStatement("""
                      ALTER TABLE foo ADD CONSTRAINT "bar" PRIMARY KEY (baz)
                      """);
    }

    [Fact]
    public void Parse_Create_Table()
    {
        VerifiedStatement("""
                     CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")
                     """);

        OneStatementParsesTo(
            """
            CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY "x"
            """,
            """
            CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x")
            """);

        VerifiedStatement("""
                          CREATE TABLE "x" ("a" "int") ENGINE=MergeTree ORDER BY ("x") AS SELECT * FROM "t" WHERE true
                          """);
    }

    [Fact]
    public void Parse_Double_Equal()
    {
        OneStatementParsesTo("SELECT foo FROM bar WHERE buz == 'buz'", "SELECT foo FROM bar WHERE buz = 'buz'");
    }

    [Fact]
    public void Parse_Select_Star_Except()
    {
        VerifiedStatement("SELECT * EXCEPT (prev_status) FROM anomalies");
    }

    [Fact]
    public void Parse_Select_Star_Except_No_Parens()
    {
        OneStatementParsesTo("SELECT * EXCEPT prev_status FROM anomalies", "SELECT * EXCEPT (prev_status) FROM anomalies");
    }

    [Fact]
    public void Parse_Select_Replace()
    {
        VerifiedStatement("SELECT * REPLACE (i + 1 AS i) FROM columns_transformers");
    }
}
