using System.ComponentModel.DataAnnotations;
using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;

public class MsSqlDialectTests : ParserTestBase
{
    public MsSqlDialectTests()
    {
        DefaultDialects = new[] { new MsSqlDialect() };
    }

    [Fact]
    public void Parse_MsSql_Identifiers()
    {
        var select = VerifiedOnlySelect("SELECT @@version, _foo$123 FROM ##temp");
        Assert.Equal(new Identifier("@@version"), select.Projection[0].AsExpr());
        Assert.Equal(new Identifier("_foo$123"), select.Projection[1].AsExpr());
        Assert.Equal(2, select.Projection.Count);
        var table = (TableFactor.Table)select.From!.Single().Relation!;
        Assert.Equal("##temp", table.Name);
    }

    [Fact]
    public void Parse_MsSql_Single_Quoted_Identifiers()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        OneStatementParsesTo("SELECT foo 'alias'", "SELECT foo AS 'alias'");
    }

    [Fact]
    public void Parse_MsSql_Delimited_Identifiers()
    {
        OneStatementParsesTo(
            "SELECT [a.b!] [FROM] FROM foo [WHERE]",
            "SELECT [a.b!] AS [FROM] FROM foo AS [WHERE]");
    }

    [Fact]
    public void Parse_MsSql_Top_Paren()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT TOP (5) * FROM foo");
        Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
        Assert.False(select.Top!.Percent);
    }

    [Fact]
    public void Parse_MsSql_Top_Percent()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT TOP (5) PERCENT * FROM foo");
        Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("5"))), select.Top!.Quantity);
        Assert.True(select.Top!.Percent);
    }

    [Fact]
    public void Parse_MsSql_Top_With_Ties()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT TOP (10) PERCENT WITH TIES * FROM foo");
        Assert.Equal(new TopQuantity.TopExpression(new LiteralValue(new Value.Number("10"))), select.Top!.Quantity);
        Assert.True(select.Top!.Percent);
    }

    [Fact]
    public void Parse_MsSql_Top()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        OneStatementParsesTo("SELECT TOP 5 bar, baz FROM foo", "SELECT TOP 5 bar, baz FROM foo");
    }

    [Fact]
    public void Parse_MsSql_Bin_Literal()
    {
        DefaultDialects = [new MsSqlDialect(), new GenericDialect()];

        OneStatementParsesTo("SELECT 0xdeadBEEF", "SELECT X'deadBEEF'");
    }

    [Fact]
    public void Parse_MsSql_Create_Role()
    {
        var role = VerifiedStatement<Statement.CreateRole>("CREATE ROLE mssql AUTHORIZATION helena");

        Assert.Equal(new ObjectName[] { new("mssql") }, role.Names);
        Assert.Equal(new ObjectName("helena"), role.AuthorizationOwner);
    }

    [Fact]
    public void Parse_Delimited_Identifiers()
    {
        var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

        var table = (TableFactor.Table)select.From!.Single().Relation!;

        Assert.Equal(new Ident[] { new("a table", Symbols.DoubleQuote) }, table.Name.Values);
        Assert.Equal(new Ident("alias", Symbols.DoubleQuote), table.Alias!.Name);

        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new CompoundIdentifier(new Ident[]
        {
            new("alias", Symbols.DoubleQuote),
            new("bar baz", Symbols.DoubleQuote)
        }), select.Projection[0].AsExpr());

        Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
        {
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        },
            select.Projection[1].AsExpr());

        var withAlias = (SelectItem.ExpressionWithAlias)select.Projection[2];

        Assert.Equal(new Identifier(new Ident("simple id", Symbols.DoubleQuote)), withAlias.Expression);
        Assert.Equal(new Ident("column alias", Symbols.DoubleQuote), withAlias.Alias);
    }

    [Fact]
    public void Parse_Create_Procedure()
    {
        const string sql = "CREATE OR ALTER PROCEDURE test (@foo INT, @bar VARCHAR(256)) AS BEGIN SELECT 1 END";

        var one = new Value.Number("1");

        var create = VerifiedStatement(sql);

        var select = new Select(
        [
            new SelectItem.UnnamedExpression(new LiteralValue(one))
        ]);

        var selectExpr = new SetExpression.SelectExpression(select);
        var query = new Query(selectExpr);

        var parameters = new Sequence<ProcedureParam>
        {
            new ("@foo", new DataType.Int()),
            new ("@bar", new DataType.Varchar(new CharacterLength.IntegerLength(256))),
        };

        var expected = new Statement.CreateProcedure(true, "test", parameters, [query]);

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Parse_Table_Name_In_Square_Brackets()
    {
        var select = VerifiedOnlySelect("SELECT [a column] FROM [a schema].[a table]");

        var table = (TableFactor.Table)select.From!.Single().Relation!;

        Assert.Equal(new ObjectName(
        [
            new Ident("a schema", '['),
            new Ident("a table", '['),
        ]), table.Name);

        Assert.Equal(new Identifier(new Ident("a column", '[')), select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Cast_Varchar_Max()
    {
        VerifiedExpr("CAST('foo' AS VARCHAR(MAX))", [new MsSqlDialect(), new GenericDialect()]);
        VerifiedExpr("CAST('foo' AS NVARCHAR(MAX))", [new MsSqlDialect(), new GenericDialect()]);
    }

    [Fact]
    public void Parse_For_Clause()
    {
        var dialects = new Dialect[] { new MsSqlDialect(), new GenericDialect() };
        VerifiedStatement("SELECT a FROM t FOR JSON PATH", dialects);
        VerifiedStatement("SELECT b FROM t FOR JSON AUTO", dialects);
        VerifiedStatement("SELECT c FROM t FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER", dialects);
        VerifiedStatement("SELECT 1 FROM t FOR JSON PATH, ROOT('x'), INCLUDE_NULL_VALUES", dialects);
        VerifiedStatement("SELECT 2 FROM t FOR XML AUTO", dialects);
        VerifiedStatement("SELECT 3 FROM t FOR XML AUTO, TYPE, ELEMENTS", dialects);
        VerifiedStatement("SELECT * FROM t WHERE x FOR XML AUTO, ELEMENTS", dialects);
        VerifiedStatement("SELECT x FROM t ORDER BY y FOR XML AUTO, ELEMENTS", dialects);
        VerifiedStatement("SELECT y FROM t FOR XML PATH('x'), ROOT('y'), ELEMENTS", dialects);
        VerifiedStatement("SELECT z FROM t FOR XML EXPLICIT, BINARY BASE64", dialects);
        VerifiedStatement("SELECT * FROM t FOR XML RAW('x')", dialects);
        VerifiedStatement("SELECT * FROM t FOR BROWSE", dialects);
    }

    [Fact]
    public void Dont_Parse_Trailing_For()
    {
        new[] { new MsSqlDialect() }.RunParserMethod("SELECT * FROM foo FOR", parser =>
        {
            Assert.Throws<ParserException>(() => parser.ParseQuery());
        });
    }

    [Fact]
    public void Parse_For_Json_Expect_Ast()
    {
        var query = VerifiedQuery("SELECT * FROM t FOR JSON PATH, ROOT('root')");

        var expected = new ForClause.Json(new ForJson.Path(), "root", false, false);

        Assert.Equal(expected, query.ForClause);
    }

    [Fact]
    public void Parse_Convert()
    {
        const string sql = "CONVERT(INT, 1, 2, 3, NULL)";
        var convert = (Expression.Convert)VerifiedExpr(sql);

        Assert.Equal(new LiteralValue(new Value.Number("1")), convert.Expression);
        Assert.Equal(new DataType.Int(), convert.DataType);
        Assert.Null(convert.CharacterSet);
        Assert.True(convert.TargetBeforeValue);

        Assert.Equal([
            new LiteralValue(new Value.Number("2")),
            new LiteralValue(new Value.Number("3")),
            new LiteralValue(new Value.Null())
        ], convert.Styles);

        VerifiedExpr("CONVERT(VARCHAR(MAX), 'foo')");
        VerifiedExpr("CONVERT(VARCHAR(10), 'foo')");
        VerifiedExpr("CONVERT(DECIMAL(10,5), 12.55)");

        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT CONVERT(INT, 'foo',) FROM T"));
    }

    [Fact]
    public void Parse_MsSql_Declare()
    {
        var statement = ParseSqlStatements("DECLARE @foo CURSOR, @bar INT, @baz AS TEXT = 'foobar';")[0];

        var expected = new Statement.Declare([

            new Declare(["@foo"], null, null, DeclareType.Cursor),
            new Declare(["@bar"], new DataType.Int(), null, null),
            new Declare(["@baz"], new DataType.Text(),
                new DeclareAssignment.MsSqlAssignment(new LiteralValue(new Value.SingleQuotedString("foobar"))), null)
        ]);

        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Ampersand_Arobase()
    {
        ExpressionParsesTo("a&@b", "a & @b", new List<Dialect> { new MsSqlDialect() });
    }


    [Fact]
    public void Parse_Use()
    {
        List<string> validObjectNames = ["mydb", "SCHEMA", "DATABASE", "CATALOG", "WAREHOUSE", "DEFAULT"];

        List<char> quoteStyles = [Symbols.SingleQuote, Symbols.DoubleQuote];

        foreach (var objectName in validObjectNames)
        {
            var useStatement = VerifiedStatement<Statement.Use>($"USE {objectName}");
            var expected = new Use.Object(new ObjectName(new Ident(objectName)));
            Assert.Equal(expected, useStatement.Name);

            foreach (var quote in quoteStyles)
            {
                useStatement = VerifiedStatement<Statement.Use>($"USE {quote}{objectName}{quote}");
                expected = new Use.Object(new ObjectName(new Ident(objectName, quote)));
                Assert.Equal(expected, useStatement.Name);
            }
        }
    }

    [Fact]
    public void Parse_Create_Table_With_Valid_Options()
    {
        #region queries
        var options = new List<(string, Sequence<SqlOption>)>
        {
            new(
                "CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (DISTRIBUTION = ROUND_ROBIN, PARTITION (column_a RANGE FOR VALUES (10, 11)))",
                [

                    new SqlOption.KeyValue("DISTRIBUTION", new Identifier("ROUND_ROBIN")),
                    new SqlOption.Partition("column_a",
                    [
                        new LiteralValue(new Value.Number("10")),
                        new LiteralValue(new Value.Number("11"))
                    ])
                ]),

            new(
                "CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (PARTITION (column_a RANGE LEFT FOR VALUES (10, 11)))",
                [
                    new SqlOption.Partition("column_a",
                        [
                            new LiteralValue(new Value.Number("10")),
                            new LiteralValue(new Value.Number("11"))
                        ],
                        PartitionRangeDirection.Left)
                ]),

            new("CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (CLUSTERED COLUMNSTORE INDEX)",
            [
                new SqlOption.Clustered(new TableOptionsClustered.ColumnstoreIndex())
            ]),

            new(
                "CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (CLUSTERED COLUMNSTORE INDEX ORDER (column_a, column_b))",
                [
                    new SqlOption.Clustered(new TableOptionsClustered.ColumnstoreIndexOrder([
                        "column_a",
                        "column_b",
                    ]))
                ]),

            new(
                "CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (CLUSTERED INDEX (column_a ASC, column_b DESC, column_c))",
                [
                    new SqlOption.Clustered(new TableOptionsClustered.Index([
                        new ClusteredIndex("column_a", true),
                        new ClusteredIndex("column_b", false),
                        new ClusteredIndex("column_c", null)
                    ]))
                ]),

            new(
                "CREATE TABLE mytable (column_a INT, column_b INT, column_c INT) WITH (DISTRIBUTION = HASH(column_a, column_b), HEAP)",
                [
                    new SqlOption.KeyValue("DISTRIBUTION",
                        new Function("HASH")
                        {
                            Args = new FunctionArguments.List(new FunctionArgumentList(
                            [
                                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("column_a"))),
                                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("column_b"))),
                            ]))
                        }),
                    new SqlOption.Identifier("HEAP")
                ])
        };
        #endregion

        foreach (var (sql, withOptions) in options)
        {
            var create = VerifiedStatement<Statement.CreateTable>(sql);

            Assert.Equal("mytable", create.Element.Name);

            var columns = new Sequence<ColumnDef>
            {
                new ("column_a", new DataType.Int()),
                new ("column_b", new DataType.Int()),
                new ("column_c", new DataType.Int())
            };

            Assert.Equal(columns, create.Element.Columns);
            Assert.Equal(withOptions, create.Element.WithOptions);
        }
    }

    [Fact]
    public void Parse_Create_Table_With_Identity_Column()
    {
        var withColumnOptions = new List<(string, Sequence<ColumnOptionDef>)>
        {
            ("CREATE TABLE mytable (columnA INT IDENTITY NOT NULL)", 
                [
                    new ColumnOptionDef(new ColumnOption.Identity()),
                    new ColumnOptionDef(new ColumnOption.NotNull())
                ]),

            ("CREATE TABLE mytable (columnA INT IDENTITY(1, 1) NOT NULL)", [
                new ColumnOptionDef(
                    new ColumnOption.Identity(new IdentityProperty(
                    new LiteralValue(new Value.Number("1")), new LiteralValue(new Value.Number("1"))))),
                new ColumnOptionDef(new ColumnOption.NotNull())
            ])
        };

        foreach (var (sql, columnOptions) in withColumnOptions)
        {
            var create = VerifiedStatement<Statement.CreateTable>(sql);

            Sequence<ColumnDef> columns =
            [
                new ("columnA", new DataType.Int(), Options: columnOptions)
            ];
            var expected = new Statement.CreateTable(new CreateTable("mytable", columns));
            Assert.Equal(expected, create);

        }
    }
}