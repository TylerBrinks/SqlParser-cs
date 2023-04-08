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
    }
}
