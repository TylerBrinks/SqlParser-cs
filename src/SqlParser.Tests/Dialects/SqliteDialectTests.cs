using System.Collections.Generic;
using System.Data;
using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;

// ReSharper disable CommentTypo

namespace SqlParser.Tests.Dialects
{
    public class SqliteDialectTests : ParserTestBase
    {
        public SqliteDialectTests()
        {
            DefaultDialects = new[] { new SQLiteDialect() };
        }

        [Fact]
        public void Parse_Create_Table_Without_Rowid()
        {
            DefaultDialects = new Dialect[] { new SQLiteDialect(), new GenericDialect() };

            var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t (a INT) WITHOUT ROWID");

            Assert.Equal("t", create.Name);
        }

        [Fact]
        public void Parse_Create_Virtual_Table()
        {
            DefaultDialects = new Dialect[] { new SQLiteDialect(), new GenericDialect() };

            var create = VerifiedStatement<Statement.CreateVirtualTable>("CREATE VIRTUAL TABLE IF NOT EXISTS t USING module_name (arg1, arg2)");

            Assert.Equal("t", create.Name);
            Assert.Equal("module_name", create.ModuleName!);
            Assert.Equal(new Ident[] { "arg1", "arg2" }, create.ModuleArgs!);

            VerifiedStatement("CREATE VIRTUAL TABLE t USING module_name");
        }

        [Fact]
        public void Parse_Create_Table_Auto_Increment()
        {
            DefaultDialects = new Dialect[] { new SQLiteDialect(), new GenericDialect() };

            var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (bar INT PRIMARY KEY AUTOINCREMENT)");

            Assert.Equal("foo", create.Name);

            var expected = new ColumnDef[]
            {
                new ("bar", new DataType.Int(), Options:new ColumnOptionDef[]
                {
                    new (new ColumnOption.Unique(true)),
                    new (new ColumnOption.DialectSpecific(new []{ new Word("AUTOINCREMENT") }))
                })
            };

            Assert.Equal(expected, create.Columns);
        }

        [Fact]
        public void Parse_Create_Sqlite_Quote()
        {
            var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE `PRIMARY` (\"KEY\" INT, [INDEX] INT)");
            var expected = new ColumnDef[]
            {
                new (new Ident("KEY", Symbols.DoubleQuote), new DataType.Int()),
                new (new Ident("INDEX", Symbols.SquareBracketOpen), new DataType.Int())
            };
            Assert.Equal("`PRIMARY`", create.Name);
            Assert.Equal(expected, create.Columns);
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
        public void Parse_Like()
        {
            Test(false);
            Test(true);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;

                var select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a'");
                var expected = new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);

                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a' ESCAPE '\\'");
                expected = new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash);
                Assert.Equal(expected, select.Selection);

                // This statement tests that LIKE and NOT LIKE have the same precedence.
                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}LIKE '%a' IS NULL");
                var isNull = new IsNull(new Like(new Identifier("name"), negated,
                    new LiteralValue(new Value.SingleQuotedString("%a"))));
                Assert.Equal(isNull, select.Selection);
            }
        }

        [Fact]
        public void Parse_Similar_To()
        {
            Test(false);
            Test(true);

            void Test(bool negated)
            {
                var negation = negated ? "NOT " : null;

                var select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a'");
                var expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")));
                Assert.Equal(expected, select.Selection);

                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\'");
                expected = new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash);
                Assert.Equal(expected, select.Selection);

                // This statement tests that LIKE and NOT LIKE have the same precedence.
                select = VerifiedOnlySelect($"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '\\' IS NULL");
                var isNull = new IsNull(new SimilarTo(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")), Symbols.Backslash));
                Assert.Equal(isNull, select.Selection);
            }
        }

        [Fact]
        public void Parse_Create_Table_With_Strict()
        {
            const string sql = "CREATE TABLE Fruits (id TEXT NOT NULL PRIMARY KEY) STRICT";

            var create = (Statement.CreateTable)VerifiedStatement(sql);

            Assert.Equal("Fruits", create.Name);
            Assert.True(create.Strict);
        }

        [Fact]
        public void Parse_Create_View_Temporary_If_Not_Exists()
        {
            var create = VerifiedStatement<Statement.CreateView>("CREATE TEMPORARY VIEW IF NOT EXISTS myschema.myview AS SELECT foo FROM bar");

            Assert.Equal("myschema.myview", create.Name);
            Assert.Equal(new Sequence<Ident>(), create.Columns);
            Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
            Assert.False(create.Materialized);
            Assert.False(create.OrReplace);
            Assert.False(create.WithNoSchemaBinding);
            Assert.True(create.IfNotExists);
            Assert.True(create.Temporary);
        }

        [Fact]
        public void Parse_Window_Function_With_Filter()
        {
            var functioNames = new[] { "row_number", "rank", "max", "count", "user_defined_function" };

            foreach (var fn in functioNames)
            {
                var sql = $"SELECT {fn}(x) FILTER (WHERE y) OVER () FROM t";

                var select = VerifiedOnlySelect(sql);
                Assert.Equal(sql, select.ToSql());
                var expected = new SelectItem.UnnamedExpression(new Function(fn)
                {
                    Over = new WindowType.WindowSpecType(new WindowSpec()),
                    Filter = new Identifier("y"),
                    Args = [new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("x")))]
                });
                Assert.Equal(expected, select.Projection.First());
            }
        }

        [Fact]
        public void Parse_Pragma_No_Value()
        {
            const string sql = "PRAGMA cache_size";

            var pragma = VerifiedStatement(sql, new Dialect[] { new SQLiteDialect(), new GenericDialect() });
            var expected = new Statement.Pragma("cache_size", null, false);
            Assert.Equal(expected, pragma);
        }

        [Fact]
        public void Parse_Pragma_Eq_Style()
        {
            const string sql = "PRAGMA cache_size = 10";

            var pragma = VerifiedStatement(sql, new Dialect[] { new SQLiteDialect(), new GenericDialect() });
            var expected = new Statement.Pragma("cache_size", new Value.Number("10"), true);
            Assert.Equal(expected, pragma);
        }

        [Fact]
        public void Parse_Pragma_Function_Style()
        {
            const string sql = "PRAGMA cache_size(10)";

            var pragma = VerifiedStatement(sql, new Dialect[] { new SQLiteDialect(), new GenericDialect() });
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
            var sql = "SELECT * FROM t1 WHERE a IN ()";
            var select = VerifiedOnlySelect(sql);

            var inList = (InList)select.Selection;

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
                new[] {new SQLiteDialect()}, options: new ParserOptions {TrailingCommas = true}));
        }
    }
}
