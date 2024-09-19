using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

// ReSharper disable CommentTypo

namespace SqlParser.Tests.Dialects;

public class SqliteDialectTests : ParserTestBase
{
    public SqliteDialectTests()
    {
        DefaultDialects = new[] { new SQLiteDialect() };
    }

    [Fact]
    public void Parse_Create_Table_Without_Rowid()
    {
        DefaultDialects = [new SQLiteDialect(), new GenericDialect()];

        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t (a INT) WITHOUT ROWID");

        Assert.Equal("t", create.Element.Name);
    }

    [Fact]
    public void Parse_Create_Virtual_Table()
    {
        DefaultDialects = [new SQLiteDialect(), new GenericDialect()];

        var create =
            VerifiedStatement<Statement.CreateVirtualTable>(
                "CREATE VIRTUAL TABLE IF NOT EXISTS t USING module_name (arg1, arg2)");

        Assert.Equal("t", create.Name);
        Assert.Equal("module_name", create.ModuleName!);
        Assert.Equal(new Ident[] { "arg1", "arg2" }, create.ModuleArgs!);

        VerifiedStatement("CREATE VIRTUAL TABLE t USING module_name");
    }

    [Fact]
    public void Parse_Create_Table_Auto_Increment()
    {
        DefaultDialects = [new SQLiteDialect(), new GenericDialect()];

        var create =
            VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (bar INT PRIMARY KEY AUTOINCREMENT)");

        Assert.Equal("foo", create.Element.Name);

        var expected = new ColumnDef[]
        {
                new("bar", new DataType.Int(), Options: new ColumnOptionDef[]
                {
                    new(new ColumnOption.Unique(true)),
                    new(new ColumnOption.DialectSpecific(new[] {new Word("AUTOINCREMENT")}))
                })
        };

        Assert.Equal(expected, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Sqlite_Quote()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE `PRIMARY` (\"KEY\" INT, [INDEX] INT)");
        var expected = new ColumnDef[]
        {
                new(new Ident("KEY", Symbols.DoubleQuote), new DataType.Int()),
                new(new Ident("INDEX", Symbols.SquareBracketOpen), new DataType.Int())
        };
        Assert.Equal("`PRIMARY`", create.Element.Name);
        Assert.Equal(expected, create.Element.Columns);
    }

    [Fact]
    public void Test_Placeholder()
    {
        // In postgres, this would be the absolute value operator '@' applied to the column 'xxx'
        // But in sqlite, this is a named parameter.
        // see https://www.sqlite.org/lang_expr.html#varparam
        var select = VerifiedOnlySelect("SELECT @xxx");
        Assert.Equal(new LiteralValue(new Value.Placeholder("@xxx")), select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Create_Table_With_Strict()
    {
        const string sql = "CREATE TABLE Fruits (id TEXT NOT NULL PRIMARY KEY) STRICT";

        var create = (Statement.CreateTable)VerifiedStatement(sql);

        Assert.Equal("Fruits", create.Element.Name);
        Assert.True(create.Element.Strict);
    }

    [Fact]
    public void Parse_Create_View_Temporary_If_Not_Exists()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE TEMPORARY VIEW IF NOT EXISTS myschema.myview AS SELECT foo FROM bar");

        Assert.Equal("myschema.myview", create.Name);
        Assert.Null(create.Columns);
        Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
        Assert.False(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.False(create.WithNoSchemaBinding);
        Assert.True(create.IfNotExists);
        Assert.True(create.Temporary);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Window_Function_With_Filter()
    {
        var functionNames = new[] { "row_number", "rank", "max", "count", "user_defined_function" };

        foreach (var fn in functionNames)
        {
            var sql = $"SELECT {fn}(x) FILTER (WHERE y) OVER () FROM t";

            var select = VerifiedOnlySelect(sql);
            Assert.Equal(sql, select.ToSql());
            var expected = new SelectItem.UnnamedExpression(new Function(fn)
            {
                Over = new WindowType.WindowSpecType(new WindowSpec()),
                Filter = new Identifier("y"),
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("x")))
                ]))
            });
            Assert.Equal(expected, select.Projection.First());
        }
    }

    [Fact]
    public void Parse_Pragma_No_Value()
    {
        const string sql = "PRAGMA cache_size";

        var pragma = VerifiedStatement(sql, [new SQLiteDialect(), new GenericDialect()]);
        var expected = new Statement.Pragma("cache_size", null, false);
        Assert.Equal(expected, pragma);
    }

    [Fact]
    public void Parse_Pragma_Eq_Style()
    {
        const string sql = "PRAGMA cache_size = 10";

        var pragma = VerifiedStatement(sql, [new SQLiteDialect(), new GenericDialect()]);
        var expected = new Statement.Pragma("cache_size", new Value.Number("10"), true);
        Assert.Equal(expected, pragma);
    }

    [Fact]
    public void Parse_Pragma_Function_Style()
    {
        const string sql = "PRAGMA cache_size(10)";

        var pragma = VerifiedStatement(sql, [new SQLiteDialect(), new GenericDialect()]);
        var expected = new Statement.Pragma("cache_size", new Value.Number("10"), false);
        Assert.Equal(expected, pragma);
    }

    [Fact]
    public void Parse_Single_Quoted_Identified()
    {
        VerifiedOnlySelect("SELECT 't'.*, t.'x' FROM 't'");
    }

    [Fact]
    public void Parse_Where_In_Empty_List()
    {
        const string sql = "SELECT * FROM t1 WHERE a IN ()";
        var select = VerifiedOnlySelect(sql);

        var inList = (InList)select.Selection!;

        Assert.Empty(inList.List);

        OneStatementParsesTo(
            "SELECT * FROM t1 WHERE a IN (,)",
            "SELECT * FROM t1 WHERE a IN ()",
            dialects: new[] { new SQLiteDialect() },
            options: new ParserOptions { TrailingCommas = true }
        );
    }

    [Fact]
    public void Invalid_Empty_List()
    {
        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM t1 WHERE a IN (,,)",
            new[] { new SQLiteDialect() }, options: new ParserOptions { TrailingCommas = true }));
    }

    [Fact]
    public void Parse_Create_Table_Gencol()
    {
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2))");
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)");
    }

    [Fact]
    public void Parse_Start_Transaction_With_Modifier()
    {
        var dialects = new Dialect[] { new SQLiteDialect(), new GenericDialect() };
        VerifiedStatement("BEGIN DEFERRED TRANSACTION", dialects);
        VerifiedStatement("BEGIN IMMEDIATE TRANSACTION", dialects);
        VerifiedStatement("BEGIN EXCLUSIVE TRANSACTION", dialects);
        OneStatementParsesTo("BEGIN DEFERRED", "BEGIN DEFERRED TRANSACTION", dialects);
        OneStatementParsesTo("BEGIN IMMEDIATE", "BEGIN IMMEDIATE TRANSACTION", dialects);
        OneStatementParsesTo("BEGIN EXCLUSIVE", "BEGIN EXCLUSIVE TRANSACTION", dialects);

        var unsupported = AllDialects.Where(d => d is not SQLiteDialect or GenericDialect).ToArray();

        Assert.Throws<ParserException>(() => ParseSqlStatements("BEGIN DEFERRED", unsupported));
        Assert.Throws<ParserException>(() => ParseSqlStatements("BEGIN IMMEDIATE", unsupported));
        Assert.Throws<ParserException>(() => ParseSqlStatements("BEGIN EXCLUSIVE", unsupported));
    }

    [Fact]
    public void Parse_Create_Table_Untyped()
    {
        VerifiedStatement("CREATE TABLE t1 (a, b AS (a * 2), c NOT NULL)");
    }

    [Fact]
    public void Pragma_Eq_String_Style()
    {
        var pragma = (Statement.Pragma)VerifiedStatement("PRAGMA table_info = 'sqlite_master'");

        Assert.Equal("table_info", pragma.Name);
        Assert.Equal("'sqlite_master'", pragma.Value!.ToSql());
    }

    [Fact]
    public void Pragma_Function_String_Style()
    {
        var pragma = (Statement.Pragma)VerifiedStatement("PRAGMA table_info(\"sqlite_master\")");

        Assert.Equal("table_info", pragma.Name);
        Assert.Equal("\"sqlite_master\"", pragma.Value!.ToSql());
    }

    [Fact]
    public void Pragma_Eq_Placeholder_Style()
    {
        var pragma = (Statement.Pragma)VerifiedStatement("PRAGMA table_info = ?");

        Assert.Equal("table_info", pragma.Name);
        Assert.Equal("?", pragma.Value!.ToSql());
    }

    [Fact]
    public void Parse_Update_Tuple_Row_Values()
    {
        var update = VerifiedStatement<Statement.Update>("UPDATE x SET (a, b) = (1, 2)");

        var expected = new Sequence<Statement.Assignment>
            {
                new (new AssignmentTarget.Tuple([
                        new ObjectName("a"),
                        new ObjectName("b")
                    ]),
                    new Expression.Tuple([
                        new LiteralValue(new Value.Number("1")),
                        new LiteralValue(new Value.Number("2")),
                    ]))
            };

        Assert.Equal(expected, update.Assignments);
    }

    [Fact]
    public void Test_Dollar_Identifier_As_Placeholder()
    {
        var expression = (BinaryOp)VerifiedExpr("id = $id", new[] { new SQLiteDialect() });

        Assert.Equal(BinaryOperator.Eq, expression.Op);
        Assert.Equal(new Identifier("id"), expression.Left);
        Assert.Equal(new LiteralValue(new Value.Placeholder("$id")), expression.Right);
    }
}
