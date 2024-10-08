using System.Text;
using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.DataType;
using static SqlParser.Ast.Expression;
using Action = SqlParser.Ast.Action;
using DataType = SqlParser.Ast.DataType;
using Map = SqlParser.Ast.Map;
using Subscript = SqlParser.Ast.Subscript;

// ReSharper disable StringLiteralTypo
// ReSharper disable CommentTypo

namespace SqlParser.Tests;

public class ParserCommonTests : ParserTestBase
{
    void TestExplain(
        string sql,
        bool expectedVerbose,
        bool expectedAnalyze,
        AnalyzeFormat expectedFormat,
        IEnumerable<Dialect>? dialects = null,
        Sequence<UtilityOption>? expectedOptions = null)
    {
        var explain = VerifiedStatement<Statement.Explain>(sql, dialects ?? AllDialects);
        Assert.Equal(expectedVerbose, explain.Verbose);
        Assert.Equal(expectedAnalyze, explain.Analyze);
        Assert.Equal(expectedFormat, explain.Format);

        Assert.Equal("SELECT sqrt(id) FROM foo", explain.Statement.ToSql());

        if (expectedOptions != null)
        {
            Assert.Equal(expectedOptions, explain.Options);
        }
    }

    [Fact]
    public void Parse_Insert_Values()
    {
        var rows = new[]
        {
            new LiteralValue(Number("1")),
            new LiteralValue(Number("2")),
            new LiteralValue(Number("3"))
        };
        var rows1 = new Sequence<Expression>[] { rows };
        var rows2 = new Sequence<Expression>[] { rows, rows };

        CheckOne("INSERT customer VALUES (1, 2, 3)", "customer", [], rows1);
        CheckOne("INSERT INTO customer VALUES (1, 2, 3)", "customer", [], rows1);
        CheckOne("INSERT INTO customer VALUES (1, 2, 3), (1, 2, 3)", "customer", [], rows2);
        CheckOne("INSERT INTO public.customer VALUES (1, 2, 3)", "public.customer", [], rows1);
        CheckOne("INSERT INTO db.public.customer VALUES (1, 2, 3)", "db.public.customer", [], rows1);
        return;

        void CheckOne(string sql, string expectedTableName, ICollection<string> expectedColumns,
            Sequence<Sequence<Expression>> expectedRows)
        {
            var statement = VerifiedStatement(sql);

            if (statement is Statement.Insert insert)
            {
                Assert.Equal(insert.InsertOperation.Name, expectedTableName);
                Assert.Equal(insert.InsertOperation.Columns!.Count, expectedColumns.Count);
                foreach (var column in expectedColumns)
                {
                    Assert.Equal(column, new Ident(column));
                }

                if (insert.InsertOperation.Source!.Query.Body is SetExpression.ValuesExpression v)
                {
                    Assert.Equal(v.Values.Rows, expectedRows);
                }
            }
            else
            {
                Assert.Fail("Statement is not Insert");
            }
        }
    }

    [Fact]
    public void Parse_Insert_Sqlite()
    {
        Check("INSERT INTO test_table(id) VALUES(1)", SqliteOnConflict.None);
        Check("REPLACE INTO test_table(id) VALUES(1)", SqliteOnConflict.Replace);
        Check("INSERT OR REPLACE INTO test_table(id) VALUES(1)", SqliteOnConflict.Replace);
        Check("INSERT OR ROLLBACK INTO test_table(id) VALUES(1)", SqliteOnConflict.Rollback);
        Check("INSERT OR ABORT INTO test_table(id) VALUES(1)", SqliteOnConflict.Abort);
        Check("INSERT OR FAIL INTO test_table(id) VALUES(1)", SqliteOnConflict.Fail);
        Check("INSERT OR IGNORE INTO test_table(id) VALUES(1)", SqliteOnConflict.Ignore);
        return;

        static void Check(string sql, SqliteOnConflict expected)
        {
            var statements = new Parser().ParseSql(sql, new SQLiteDialect());
            var insert = statements[0] as Statement.Insert;
            Assert.Equal(expected, insert!.InsertOperation.Or);
        }
    }

    [Fact]
    public void Parse_Update()
    {
        var update = VerifiedStatement<Statement.Update>("UPDATE t SET a = 1, b = 2, c = 3 WHERE d");
        Assert.Equal("t", update.Table.ToSql());

        var assignments = new[]
        {
            new Statement.Assignment(new AssignmentTarget.ColumnName("a"), new LiteralValue(Number("1"))),
            new Statement.Assignment(new AssignmentTarget.ColumnName("b"), new LiteralValue(Number("2"))),
            new Statement.Assignment(new AssignmentTarget.ColumnName("c"), new LiteralValue(Number("3")))
        };

        Assert.Equal(assignments, update.Assignments);

    }

    [Fact]
    public void Parse_Update_Set_From()
    {
        const string sql =
            "UPDATE t1 SET name = t2.name FROM (SELECT name, id FROM t1 GROUP BY id) AS t2 WHERE t1.id = t2.id";

        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new DuckDbDialect(),
            new PostgreSqlDialect(),
            new BigQueryDialect(),
            new SnowflakeDialect(),
            new RedshiftDialect(),
            new MsSqlDialect(),
            new SQLiteDialect()
        };

        var statement = VerifiedStatement(sql, dialects);

        var table = new TableWithJoins(new TableFactor.Table("t1"));
        var assignment = new Statement.Assignment(new AssignmentTarget.ColumnName("name"),
            new CompoundIdentifier(new Ident[] { "t2", "name" }));
        var assignments = new[] { assignment };

        var projection = new[]
        {
            new SelectItem.UnnamedExpression(new Identifier("name")),
            new SelectItem.UnnamedExpression(new Identifier("id"))
        };
        var body = new SetExpression.SelectExpression(new Select(projection)
        {
            From = new TableWithJoins[]
            {
                new(new TableFactor.Table("t1"))
            },
            GroupBy = new GroupByExpression.Expressions([new Identifier("id")])
        });
        var subQuery = new Query(body);
        var derived = new TableFactor.Derived(subQuery)
        {
            Alias = new TableAlias("t2")
        };

        var from = new TableWithJoins(derived);
        var left = new CompoundIdentifier(new Ident[] { "t1", "id" });
        var right = new CompoundIdentifier(new Ident[] { "t2", "id" });
        var selection = new BinaryOp(left, BinaryOperator.Eq, right);

        var expected = new Statement.Update(table, assignments, from, selection);

        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Update_With_Table_Alias()
    {
        const string sql = "UPDATE users AS u SET u.username = 'new_user' WHERE u.username = 'old_user'";

        var statement = VerifiedStatement(sql);
        var table = new TableWithJoins(new TableFactor.Table("users")
        {
            Alias = new TableAlias("u")
        });
        var assignment = new Statement.Assignment(
            new AssignmentTarget.ColumnName(new ObjectName(["u", "username"])),
            new LiteralValue(new Value.SingleQuotedString("new_user")));
        var assignments = new[] { assignment };

        var selection = new BinaryOp(
            new CompoundIdentifier(new Ident[] { "u", "username" }),
            BinaryOperator.Eq,
            new LiteralValue(new Value.SingleQuotedString("old_user"))
        );

        Assert.Equal(table, statement.AsUpdate().Table);
        Assert.Equal(assignments, statement.AsUpdate().Assignments);
        Assert.Equal(selection, statement.AsUpdate().Selection);
    }

    [Fact]
    public void Parse_Invalid_Table_Name()
    {
        AllDialects.RunParserMethod("db.public..customer",
            parser => Assert.Throws<ParserException>(parser.ParseObjectName));
    }

    [Fact]
    public void Parse_No_Table_Name()
    {
        Assert.Throws<ParserException>(() => AllDialects.RunParserMethod("", parser => parser.ParseObjectName()));
    }

    [Fact]
    public void Parse_Quoted_Delete_Statement()
    {
        var delete = (Statement.Delete)VerifiedStatement("DELETE FROM \"table\"");
        var relation = ((FromTable.WithFromKeyword)delete.DeleteOperation.From).From.First().Relation;

        var expected = new TableFactor.Table(new ObjectName(new Ident("table", Symbols.DoubleQuote)));

        Assert.Equal(expected, relation);
    }

    [Fact]
    public void Parse_Delete_Without_fFrom_Error()
    {
        Assert.Throws<ParserException>(() => ParseSqlStatements("DELETE \"table\" WHERE 1"));
    }

    [Fact]
    public void Parse_Where_Delete_Statement()
    {
        var statement = VerifiedStatement("DELETE FROM foo WHERE name = 5");

        var table = new TableFactor.Table("foo");
        var binaryOp = new BinaryOp(new Identifier("name"), BinaryOperator.Eq, new LiteralValue(Number("5")));

        var delete = statement.AsDelete();
        Assert.Equal(table, ((FromTable.WithFromKeyword)delete.DeleteOperation.From).From.First().Relation);
        Assert.Null(delete.DeleteOperation.Using);
        Assert.Equal(binaryOp, delete.DeleteOperation.Selection);
    }

    [Fact]
    public void Parse_Where_Delete_With_Alias_Statement()
    {
        var delete = VerifiedStatement<Statement.Delete>("DELETE FROM basket AS a USING basket AS b WHERE a.id < b.id");

        var table = new TableFactor.Table("basket") { Alias = new TableAlias("a") };
        var @using = new TableFactor.Table("basket") { Alias = new TableAlias("b") };
        var binaryOp = new BinaryOp(
            new CompoundIdentifier(new Ident[] { "a", "id" }),
            BinaryOperator.Lt,
            new CompoundIdentifier(new Ident[] { "b", "id" }));

        Assert.Equal(table, ((FromTable.WithFromKeyword)delete.DeleteOperation.From).From.First().Relation);
        Assert.Equal(@using, delete.DeleteOperation.Using);
        Assert.Equal(binaryOp, delete.DeleteOperation.Selection);
        Assert.Null(delete.DeleteOperation.Returning);
    }

    [Fact]
    public void Parse_Top_Level()
    {
        VerifiedStatement("SELECT 1");
        VerifiedStatement("(SELECT 1)");
        VerifiedStatement("((SELECT 1))");
        VerifiedStatement("VALUES (1)");
        VerifiedStatement("VALUES ROW(1, true, 'a'), ROW(2, false, 'b')");
    }

    [Fact]
    public void Parse_Simple_Select()
    {
        const string sql = "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT 5";
        var select = VerifiedOnlySelect(sql);
        Assert.Null(select.Distinct);
        Assert.Equal(3, select.Projection.Count);
        var query = VerifiedQuery(sql);
        Assert.Equal(new LiteralValue(Number("5")), query.Limit);
    }

    [Fact]
    public void Parse_Limit_Is_Not_Nn_Alias()
    {
        var ast = VerifiedQuery("SELECT id FROM customer LIMIT 1");
        Assert.Equal(new LiteralValue(Number("1")), ast.Limit);

        ast = VerifiedQuery("SELECT 1 LIMIT 5");
        Assert.Equal(new LiteralValue(Number("5")), ast.Limit);
    }

    [Fact]
    public void Parse_Select_Distinct()
    {
        const string sql = "SELECT DISTINCT name FROM customer";
        var select = VerifiedOnlySelect(sql);
        Assert.IsType<DistinctFilter.Distinct>(select.Distinct);
        Assert.Equal(new SelectItem.UnnamedExpression(new Identifier("name")), select.Projection.Single());
    }

    [Fact]
    public void Parse_Select_Distinct_Two_Fields()
    {
        const string sql = "SELECT DISTINCT name, id FROM customer";
        var select = VerifiedOnlySelect(sql);
        Assert.IsType<DistinctFilter.Distinct>(select.Distinct);
        Assert.Equal(new SelectItem.UnnamedExpression(new Identifier("name")), select.Projection.First());
        Assert.Equal(new SelectItem.UnnamedExpression(new Identifier("id")), select.Projection.Last());
    }

    [Fact]
    public void Parse_Select_Distinct_Tuple()
    {
        const string sql = "SELECT DISTINCT (name, id) FROM customer";
        var select = VerifiedOnlySelect(sql);
        var expected = new[]
        {
            new SelectItem.UnnamedExpression(new Expression.Tuple(new[]
            {
                new Identifier("name"),
                new Identifier("id")
            }))
        };
        Assert.Equal(expected, select.Projection);
    }

    [Fact]
    public void Parse_Select_Distinct_Missing_Paren()
    {
        foreach (var dialect in AllDialects)
        {
            var ex = Assert.Throws<ParserException>(() =>
                ParseSqlStatements("SELECT DISTINCT (name, id FROM customer", new[] { dialect }));
            Assert.Equal("Expected ), found FROM, Line: 1, Col: 27", ex.Message);
        }
    }

    [Fact]
    public void Parse_Select_All()
    {
        OneStatementParsesTo("SELECT ALL name FROM customer", "SELECT name FROM customer");
    }

    [Fact]
    public void Parse_Select_All_Distinct()
    {
        foreach (var dialect in AllDialects)
        {
            var ex = Assert.Throws<ParserException>(() =>
                ParseSqlStatements("SELECT ALL DISTINCT name FROM customer", new[] { dialect }));
            Assert.Equal("Cannot specify both ALL and DISTINCT", ex.Message);
        }
    }

    [Fact]
    public void Parse_Select_Into()
    {
        var sql = "SELECT * INTO table0 FROM table1";

        OneStatementParsesTo(sql, sql);
        VerifiedOnlySelect(sql);

        sql = "SELECT * INTO TEMPORARY UNLOGGED TABLE table0 FROM table1";
        OneStatementParsesTo(sql, sql);

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * INTO table0 asdf FROM table1"));
        Assert.Equal("Expected end of statement, found asdf, Line: 1, Col: 22", ex.Message);
    }

    [Fact]
    public void Parse_Select_Wildcard()
    {
        var select = VerifiedOnlySelect("SELECT * FROM foo");
        Assert.Equal(new SelectItem.Wildcard(new WildcardAdditionalOptions()), select.Projection.Single());

        select = VerifiedOnlySelect("SELECT foo.* FROM foo");
        Assert.Equal(new SelectItem.QualifiedWildcard("foo", new WildcardAdditionalOptions()),
            select.Projection.Single());

        select = VerifiedOnlySelect("SELECT myschema.mytable.* FROM myschema.mytable");
        Assert.Equal(
            new SelectItem.QualifiedWildcard(new ObjectName(new Ident[] { "myschema", "mytable" }),
                new WildcardAdditionalOptions()), select.Projection.Single());

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * + * FROM foo;"));
        Assert.Equal("Expected end of statement, found +, Line: 1, Col: 10", ex.Message);
    }

    [Fact]
    public void Parse_Count_Wildcard()
    {
        VerifiedOnlySelect("SELECT COUNT(*) FROM Order WHERE id = 10");
        VerifiedOnlySelect("SELECT COUNT(Employee.*) FROM Order JOIN Employee ON Order.employee = Employee.id");
    }

    [Fact]
    public void Parse_Column_Aliases()
    {
        const string sql = "SELECT a.col + 1 AS newname FROM foo AS a";
        var select = VerifiedOnlySelect(sql);

        var left = new CompoundIdentifier(new Ident[] { "a", "col" });
        var binOp = new BinaryOp(left, BinaryOperator.Plus, new LiteralValue(Number("1")));
        var expected = new SelectItem.ExpressionWithAlias(binOp, "newname");

        Assert.Equal(expected, select.Projection.Single());

        OneStatementParsesTo(sql, sql);
    }

    [Fact]
    public void Test_EoF_After_As()
    {
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT foo AS"));
        Assert.Equal("Expected an identifier after AS, found EOF", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT 1 FROM foo AS"));
        Assert.Equal("Expected an identifier after AS, found EOF", ex.Message);
    }

    [Fact]
    public void Test_No_Infix_Error()
    {
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT foo AS"));
        Assert.Contains("Expected an identifier after AS, found EOF", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT 1 FROM foo AS"));
        Assert.Contains("Expected an identifier after AS, found EOF", ex.Message);
    }

    [Fact]
    public void Parse_Select_Count_Wildcard()
    {
        var select = VerifiedOnlySelect("SELECT COUNT(*) FROM customer");

        Expression expected = new Function("COUNT")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(new FunctionArgExpression.Wildcard())
            ]))
        };

        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Select_Count_Distinct()
    {
        var select = VerifiedOnlySelect("SELECT COUNT(DISTINCT +x) FROM customer");

        Expression expected = new Function("COUNT")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(new UnaryOp(new Identifier("x"), UnaryOperator.Plus)))
            ], DuplicateTreatment.Distinct))
        };

        Assert.Equal(expected, select.Projection.Single().AsExpr());

        VerifiedStatement("SELECT COUNT(ALL +x) FROM customer");
        VerifiedStatement("SELECT COUNT(+x) FROM customer");

        var ex = Assert.Throws<ParserException>(
            () => ParseSqlStatements("SELECT COUNT(ALL DISTINCT + x) FROM customer"));
        Assert.Equal("Cannot specify both ALL and DISTINCT", ex.Message);
    }

    [Fact]
    public void Parse_Not()
    {
        VerifiedOnlySelect("SELECT id FROM customer WHERE NOT salary = ''");
    }

    [Fact]
    public void Parse_Invalid_Infix_Not()
    {
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT c FROM t WHERE c NOT ("));
        Assert.Equal("Expected end of statement, found NOT, Line: 1, Col: 25", ex.Message);
    }

    [Fact]
    public void Parse_Collate()
    {
        var select = VerifiedOnlySelect("SELECT (name) COLLATE \"de_DE\" FROM customer");
        Assert.True(select.Projection.Single() is SelectItem.UnnamedExpression { Expression: Collate });
    }

    [Fact]
    public void Parse_Collate_After_Parens()
    {
        var select = VerifiedOnlySelect("SELECT (name) COLLATE \"de_DE\" FROM customer");
        Assert.True(select.Projection.Single() is SelectItem.UnnamedExpression { Expression: Collate });
    }

    [Fact]
    public void Parse_Select_String_Predicate()
    {
        VerifiedOnlySelect("SELECT id, fname, lname FROM customer WHERE salary <> 'Not Provided' AND salary <> ''");
    }

    [Fact]
    public void Parse_Projection_Nested_Type()
    {
        VerifiedOnlySelect("SELECT customer.address.state FROM foo");
    }

    [Fact]
    public void Parse_Null_In_Select()
    {
        var select = VerifiedOnlySelect("SELECT NULL");
        Assert.Equal(new LiteralValue(new Value.Null()), select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Exponent_In_Select()
    {
        // all except Hive, as it allows numbers to start an identifier
        var dialects = AllDialects.Where(dialect => dialect is not HiveDialect);

        var query = (Query)ParseSqlStatements("SELECT 10e-20, 1e3, 1e+3, 1e3a, 1e, 0.5e2", dialects)[0]!;
        var body = (SetExpression.SelectExpression)query.Body;
        var select = body.Select;

        Assert.Equal(new SelectItem.UnnamedExpression(new LiteralValue(Number("10e-20"))), select.Projection[0]);
        Assert.Equal(new SelectItem.UnnamedExpression(new LiteralValue(Number("1e3"))), select.Projection[1]);
        Assert.Equal(new SelectItem.UnnamedExpression(new LiteralValue(Number("1e+3"))), select.Projection[2]);
        Assert.Equal(new SelectItem.ExpressionWithAlias(new LiteralValue(Number("1e3")), "a"), select.Projection[3]);
        Assert.Equal(new SelectItem.ExpressionWithAlias(new LiteralValue(Number("1")), "e"), select.Projection[4]);
        Assert.Equal(new SelectItem.UnnamedExpression(new LiteralValue(Number("0.5e2"))), select.Projection[5]);
    }

    [Fact]
    public void Parse_Select_With_Date_Column_Name()
    {
        var select = VerifiedOnlySelect("SELECT date");

        Assert.Equal(new Identifier("date"), select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Escaped_Single_Quote_String_Predicate_With_Escape()
    {
        var select = VerifiedOnlySelect("SELECT id, fname, lname FROM customer WHERE salary <> 'Jim''s salary'",
            unescape: true);

        var expected = new BinaryOp(
            new Identifier("salary"),
            BinaryOperator.NotEq,
            new LiteralValue(new Value.SingleQuotedString("Jim's salary")));

        Assert.Equal(expected, select.Selection);
    }

    [Fact]
    public void Parse_Escaped_Single_Quote_String_Predicate_With_No_Escape()
    {
        var select = VerifiedOnlySelect("SELECT id, fname, lname FROM customer WHERE salary <> 'Jim''s salary'",
            new[] { new MySqlDialect() }, unescape: false);

        var expected = new BinaryOp(
            new Identifier("salary"),
            BinaryOperator.NotEq,
            new LiteralValue(new Value.SingleQuotedString("Jim''s salary")));

        Assert.Equal(expected, select.Selection);
    }

    [Fact]
    public void Parse_Number()
    {
        var expr = VerifiedExpr("1.0");
        Assert.Equal(new LiteralValue(Number("1.0")), expr);
    }

    [Fact]
    public void Parse_Compound_Expr_1()
    {
        var expr = VerifiedExpr("a + b * c");

        var multiply = new BinaryOp(
            new Identifier("b"),
            BinaryOperator.Multiply,
            new Identifier("c"));

        var expected = new BinaryOp(
            new Identifier("a"),
            BinaryOperator.Plus,
            multiply);

        Assert.Equal(expected, expr);
    }

    [Fact]
    public void Parse_Compound_Expr_2()
    {
        var expr = VerifiedExpr("a * b + c");

        var multiply = new BinaryOp(
            new Identifier("a"),
            BinaryOperator.Multiply,
            new Identifier("b"));

        var expected = new BinaryOp(
            multiply,
            BinaryOperator.Plus,
            new Identifier("c"));

        Assert.Equal(expected, expr);
    }

    [Fact]
    public void Parse_Unary_Math_With_Plus()
    {
        var expr = VerifiedExpr("-a + -b");

        var left = new UnaryOp(new Identifier("a"), UnaryOperator.Minus);
        var right = new UnaryOp(new Identifier("b"), UnaryOperator.Minus);

        var expected = new BinaryOp(left, BinaryOperator.Plus, right);

        Assert.Equal(expected, expr);
    }

    [Fact]
    public void Parse_Unary_Math_With_Multiply()
    {
        var expr = VerifiedExpr("-a * -b");

        var left = new UnaryOp(new Identifier("a"), UnaryOperator.Minus);
        var right = new UnaryOp(new Identifier("b"), UnaryOperator.Minus);

        var expected = new BinaryOp(left, BinaryOperator.Multiply, right);

        Assert.Equal(expected, expr);
    }

    [Fact]
    public void Parse_Is_Null()
    {
        Assert.Equal(new IsNull(new Identifier("a")), VerifiedExpr("a IS NULL"));
    }

    [Fact]
    public void Parse_Is_Not_Null()
    {
        Assert.Equal(new IsNotNull(new Identifier("a")), VerifiedExpr("a IS NOT NULL"));
    }

    [Fact]
    public void Parse_Is_Distinct_From()
    {
        Assert.Equal(new IsDistinctFrom(new Identifier("a"), new Identifier("b")),
            VerifiedExpr("a IS DISTINCT FROM b"));
    }

    [Fact]
    public void Parse_Is_Not_Distinct_From()
    {
        Assert.Equal(new IsNotDistinctFrom(new Identifier("a"), new Identifier("b")),
            VerifiedExpr("a IS NOT DISTINCT FROM b"));
    }

    [Fact]
    public void Parse_Not_Precedence()
    {
        var expr = VerifiedExpr("NOT true OR true");
        Assert.IsType<BinaryOp>(expr);
        Assert.Equal(BinaryOperator.Or, expr.AsBinaryOp().Op);

        expr = VerifiedExpr("NOT a IS NULL");
        Assert.IsType<UnaryOp>(expr);
        Assert.Equal(UnaryOperator.Not, expr.AsUnaryOp().Op);

        expr = VerifiedExpr("NOT 1 NOT BETWEEN 1 AND 2");
        Assert.IsType<UnaryOp>(expr);
        Assert.Equal(UnaryOperator.Not, expr.AsUnaryOp().Op);

        var between = new Between(
            new LiteralValue(Number("1")),
            true,
            new LiteralValue(Number("1")),
            new LiteralValue(Number("2")));
        var unary = new UnaryOp(between, UnaryOperator.Not);
        Assert.Equal(unary, expr);


        expr = VerifiedExpr("NOT 'a' NOT LIKE 'b'");
        var like = new Like(
            new LiteralValue(new Value.SingleQuotedString("a")),
            true,
            new LiteralValue(new Value.SingleQuotedString("b"))
        );
        unary = new UnaryOp(like, UnaryOperator.Not);
        Assert.Equal(unary, expr);

        expr = VerifiedExpr("NOT a NOT IN ('a')");

        var inList = new InList(new Identifier("a"),
            new[]
            {
                new LiteralValue(new Value.SingleQuotedString("a"))
            },
            true);
        unary = new UnaryOp(inList, UnaryOperator.Not);
        Assert.Equal(unary, expr);
    }

    [Fact]
    public void Parse_Null_Like()
    {
        const string sql = "SELECT column1 LIKE NULL AS col_null, NULL LIKE column1 AS null_col FROM customers";

        var select = VerifiedOnlySelect(sql);

        var alias = new SelectItem.ExpressionWithAlias(
            new Like(
                new Identifier("column1"),
                false,
                new LiteralValue(new Value.Null())
            ),
            "col_null");
        Assert.Equal(alias, select.Projection[0]);

        alias = new SelectItem.ExpressionWithAlias(
            new Like(
                new LiteralValue(new Value.Null()),
                false,
                new Identifier("column1")
            ),
            "null_col");
        Assert.Equal(alias, select.Projection[1]);
    }

    [Fact]
    public void Parse_ILike()
    {
        Test(true);
        Test(false);
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : "";
            var sql = $"SELECT * FROM customers WHERE name {negation}ILIKE '%a'";

            var select = VerifiedOnlySelect(sql);
            var iLike = new ILike(
                new Identifier("name"),
                negated,
                new LiteralValue(new Value.SingleQuotedString("%a"))
            );
            Assert.Equal(iLike, select.Selection);

            // Test with escape char
            sql = $"SELECT * FROM customers WHERE name {negation}ILIKE '%a' ESCAPE '^'";
            select = VerifiedOnlySelect(sql);
            iLike = new ILike(
                new Identifier("name"),
                negated,
                new LiteralValue(new Value.SingleQuotedString("%a")),
                Symbols.Caret
            );
            Assert.Equal(iLike, select.Selection);

            // This statement tests that ILIKE and NOT ILIKE have the same precedence.
            sql = $"SELECT * FROM customers WHERE name {negation}ILIKE '%a' IS NULL";
            select = VerifiedOnlySelect(sql);
            var isNull = new IsNull(
                new ILike(
                    new Identifier("name"),
                    negated,
                    new LiteralValue(new Value.SingleQuotedString("%a"))
                ));
            Assert.Equal(isNull, select.Selection);
        }
    }

    [Fact]
    public void Parse_In_List()
    {
        Test(true);
        Test(true);
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : "";
            var sql = $"SELECT * FROM customers WHERE segment {negation}IN ('HIGH', 'MED')";
            var select = VerifiedOnlySelect(sql);

            var inList = new InList(
                new Identifier("segment"),
                new[]
                {
                    new LiteralValue(new Value.SingleQuotedString("HIGH")),
                    new LiteralValue(new Value.SingleQuotedString("MED"))
                },
                negated
            );

            Assert.Equal(inList, select.Selection);
        }
    }

    [Fact]
    public void Parse_In_Subquery()
    {
        var select = VerifiedOnlySelect("SELECT * FROM customers WHERE segment IN (SELECT segm FROM bar)");

        var expected = new InSubquery(
            VerifiedQuery("SELECT segm FROM bar"),
            false,
            new Identifier("segment"));

        Assert.Equal(expected, select.Selection);
    }

    [Fact]
    public void Parse_In_Unset()
    {
        Test(true);
        Test(false);
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : "";
            var sql = $"SELECT * FROM customers WHERE segment {negation}IN UNNEST(expr)";

            var select = VerifiedOnlySelect(sql);
            var unset = new InUnnest(
                new Identifier("segment"),
                VerifiedExpr("expr"),
                negated
            );

            Assert.Equal(unset, select.Selection);
        }
    }

    [Fact]
    public void Parse_In_Error()
    {
        var ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT * FROM customers WHERE segment in segment"));
        Assert.Equal("Expected (, found segment, Line: 1, Col: 42", ex.Message);
    }

    [Fact]
    public void Parse_String_Arg()
    {
        var select = VerifiedOnlySelect("Select a || b");

        var expected = new SelectItem.UnnamedExpression(
            new BinaryOp(
                new Identifier("a"),
                BinaryOperator.StringConcat,
                new Identifier("b"))
        );

        Assert.Equal(expected, select.Projection.Single());
    }

    [Fact]
    public void Parse_Bitwise_Ops()
    {
        var bitwiseOps = new[]
        {
            (Symbols.Caret, BinaryOperator.BitwiseXor, AllDialects.Where(d => d is not PostgreSqlDialect)),
            (Symbols.Pipe, BinaryOperator.BitwiseOr, AllDialects),
            (Symbols.Ampersand, BinaryOperator.BitwiseAnd, AllDialects)
        };

        foreach (var (symbol, op, dialect) in bitwiseOps)
        {
            var select = VerifiedOnlySelect($"select a {symbol} b", dialect);
            var expected = new SelectItem.UnnamedExpression(
                new BinaryOp(
                    new Identifier("a"),
                    op,
                    new Identifier("b"))
            );
            Assert.Equal(expected, select.Projection.Single());
        }
    }

    [Fact]
    public void Parse_Binary_Any()
    {
        var select = VerifiedOnlySelect("SELECT a = ANY(b)");

        var expected = new SelectItem.UnnamedExpression(
            new AnyOp(
                new Identifier("a"),
                BinaryOperator.Eq,
                new Identifier("b")));

        Assert.Equal(expected, select.Projection.Single());
    }

    [Fact]
    public void Parse_Binary_All()
    {
        var select = VerifiedOnlySelect("SELECT a = ALL(b)");

        var expected = new SelectItem.UnnamedExpression(
            new AllOp(
                new Identifier("a"),
                BinaryOperator.Eq,
                new Identifier("b"))
        );

        Assert.Equal(expected, select.Projection.Single());
    }

    [Fact]
    public void Parse_Logical_Xor()
    {
        var select = VerifiedOnlySelect("SELECT true XOR true, false XOR false, true XOR false, false XOR true");

        var expected = new SelectItem.UnnamedExpression(
            new BinaryOp(
                new LiteralValue(new Value.Boolean(true)),
                BinaryOperator.Xor,
                new LiteralValue(new Value.Boolean(true))
            )
        );
        Assert.Equal(expected, select.Projection[0]);

        expected = new SelectItem.UnnamedExpression(
            new BinaryOp(
                new LiteralValue(new Value.Boolean(false)),
                BinaryOperator.Xor,
                new LiteralValue(new Value.Boolean(false))
            )
        );
        Assert.Equal(expected, select.Projection[1]);

        expected = new SelectItem.UnnamedExpression(
            new BinaryOp(
                new LiteralValue(new Value.Boolean(true)),
                BinaryOperator.Xor,
                new LiteralValue(new Value.Boolean(false))
            )
        );
        Assert.Equal(expected, select.Projection[2]);

        expected = new SelectItem.UnnamedExpression(
            new BinaryOp(
                new LiteralValue(new Value.Boolean(false)),
                BinaryOperator.Xor,
                new LiteralValue(new Value.Boolean(true))
            )
        );
        Assert.Equal(expected, select.Projection[3]);
    }

    [Fact]
    public void Parse_Between()
    {
        Test(true);
        Test(false);
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : "";
            var sql = $"SELECT * FROM customers WHERE age {negation}BETWEEN 25 AND 32";
            var select = VerifiedOnlySelect(sql);

            var expected = new Between(
                new Identifier("age"),
                negated,
                new LiteralValue(Number("25")),
                new LiteralValue(Number("32"))
            );

            Assert.Equal(expected, select.Selection);
        }
    }

    [Fact]
    public void Parse_Between_With_Expr()
    {
        var select = VerifiedOnlySelect("SELECT * FROM t WHERE 1 BETWEEN 1 + 2 AND 3 + 4 IS NULL");

        var expected = new IsNull(
            new Between(
                new LiteralValue(Number("1")),
                false,
                new BinaryOp(
                    new LiteralValue(Number("1")),
                    BinaryOperator.Plus,
                    new LiteralValue(Number("2"))
                ),
                new BinaryOp(
                    new LiteralValue(Number("3")),
                    BinaryOperator.Plus,
                    new LiteralValue(Number("4"))
                )
            )
        );

        Assert.Equal(expected, select.Selection);
    }

    [Fact]
    public void Parse_Tuples()
    {
        var select = VerifiedOnlySelect("SELECT (1, 2), (1), ('foo', 3, baz)");

        var expected = new SelectItem[]
        {
            new SelectItem.UnnamedExpression(
                new Expression.Tuple(new[]
                {
                    new LiteralValue(Number("1")),
                    new LiteralValue(Number("2"))
                })),
            new SelectItem.UnnamedExpression(new Expression.Nested(new LiteralValue(Number("1")))),
            new SelectItem.UnnamedExpression(
                new Expression.Tuple(new Expression[]
                {
                    new LiteralValue(new Value.SingleQuotedString("foo")),
                    new LiteralValue(Number("3")),
                    new Identifier("baz")
                }))
        };

        Assert.Equal(expected, select.Projection);
    }

    [Fact]
    public void Parse_Tuple_Invalid()
    {
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("select (1"));
        Assert.Equal("Expected ), found EOF", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("select (), 2"));
        Assert.Equal("Expected an expression, found ), Line: 1, Col: 9", ex.Message);
    }

    [Fact]
    public void Parse_Select_Order_By()
    {
        Test("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC, id");
        // make sure ORDER is not treated as an alias
        Test("SELECT id, fname, lname FROM customer ORDER BY lname ASC, fname DESC, id");
        Test("SELECT 1 AS lname, 2 AS fname, 3 AS id, 4 ORDER BY lname ASC, fname DESC, id");

        void Test(string sql)
        {
            var select = VerifiedQuery(sql);

            var expected = new OrderByExpression[]
            {
                new(new Identifier("lname"), true),
                new(new Identifier("fname"), false),
                new(new Identifier("id"))
            };

            Assert.Equal(expected, select.OrderBy!.Expressions);
        }
    }

    [Fact]
    public void Parse_Select_Order_By_Limit()
    {
        var select =
            VerifiedQuery("SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC, fname DESC LIMIT 2");

        var expected = new OrderByExpression[]
        {
            new(new Identifier("lname"), true),
            new(new Identifier("fname"), false)
        };
        Assert.Equal(expected, select.OrderBy!.Expressions);
        Assert.Equal(new LiteralValue(Number("2")), select.Limit);
    }

    [Fact]
    public void Parse_Select_Order_By_Nulls_Order()
    {
        var select =
            VerifiedQuery(
                "SELECT id, fname, lname FROM customer WHERE id < 5 ORDER BY lname ASC NULLS FIRST, fname DESC NULLS LAST LIMIT 2");

        var expected = new OrderByExpression[]
        {
            new(new Identifier("lname"), true, true),
            new(new Identifier("fname"), false, false)
        };
        Assert.Equal(expected, select.OrderBy!.Expressions);
        Assert.Equal(new LiteralValue(Number("2")), select.Limit);
    }

    [Fact]
    public void Parse_Select_Group_By()
    {
        var select = VerifiedOnlySelect("SELECT id, fname, lname FROM customer GROUP BY lname, fname");

        var expected = new GroupByExpression.Expressions(new Identifier[]
        {
            new("lname"),
            new("fname")
        });

        Assert.Equal(expected, select.GroupBy!);
        OneStatementParsesTo(
            "SELECT id, fname, lname FROM customer GROUP BY (lname, fname)",
            "SELECT id, fname, lname FROM customer GROUP BY (lname, fname)"
        );
    }

    [Fact]
    public void Parse_Select_Group_By_Grouping_Sets()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new PostgreSqlDialect()
        };

        var select =
            VerifiedOnlySelect(
                "SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, GROUPING SETS ((brand), (size), ())",
                dialects);

        var expected = new GroupByExpression.Expressions([
            new Identifier("size"),
            new GroupingSets(new Sequence<Expression>[]
            {
                [new Identifier("brand")],
                [new Identifier("size")],
                []
            })
        ]);

        Assert.Equal(expected, select.GroupBy!);
    }

    [Fact]
    public void Parse_Select_Group_By_Rollup()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new PostgreSqlDialect()
        };

        var select =
            VerifiedOnlySelect("SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, ROLLUP (brand, size)",
                dialects);

        var expected = new GroupByExpression.Expressions([
            new Identifier("size"),
            new Rollup(new Sequence<Expression>[]
            {
                [new Identifier("brand")],
                [new Identifier("size")]
            })
        ]);

        Assert.Equal(expected, select.GroupBy!);
    }

    [Fact]
    public void Parse_Select_Group_By_Cube()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new PostgreSqlDialect()
        };

        var select =
            VerifiedOnlySelect("SELECT brand, size, sum(sales) FROM items_sold GROUP BY size, CUBE (brand, size)",
                dialects);

        var expected = new GroupByExpression.Expressions([
            new Identifier("size"),
            new Cube(new Sequence<Expression>[]
            {
                [new Identifier("brand")],
                [new Identifier("size")]
            })
        ]);

        Assert.Equal(expected, select.GroupBy!);
    }

    [Fact]
    public void Parse_Select_Having()
    {
        var select = VerifiedOnlySelect("SELECT foo FROM bar GROUP BY foo HAVING COUNT(*) > 1");
        var expected = new BinaryOp(
            new Function("COUNT")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.Wildcard())
                ]))
            },
            BinaryOperator.Gt,
            new LiteralValue(Number("1"))
        );

        Assert.Equal(expected, select.Having);

        select = VerifiedOnlySelect("SELECT 'foo' HAVING 1 = 1");

        expected = new BinaryOp(
            new LiteralValue(Number("1")),
            BinaryOperator.Eq,
            new LiteralValue(Number("1"))
        );
        Assert.Equal(expected, select.Having);
    }

    [Fact]
    public void Parse_Select_Qualify()
    {
        var select =
            VerifiedOnlySelect("SELECT i, p, o FROM qt QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1");
        var expected = new BinaryOp(
            new Function("ROW_NUMBER")
            {
                Over = new WindowType.WindowSpecType(new WindowSpec(
                    new[] { new Identifier("p") },
                    new OrderByExpression[]
                    {
                        new(new Identifier("o"))
                    })),
                Args = new FunctionArguments.List(FunctionArgumentList.Empty())
            },
            BinaryOperator.Eq,
            new LiteralValue(Number("1"))
        );

        Assert.Equal(expected, select.QualifyBy);

        select = VerifiedOnlySelect(
            "SELECT i, p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) AS row_num FROM qt QUALIFY row_num = 1");

        expected = new BinaryOp(
            new Identifier("row_num"),
            BinaryOperator.Eq,
            new LiteralValue(Number("1"))
        );
        Assert.Equal(expected, select.QualifyBy);
    }

    [Fact]
    public void Parse_Select_Accepts_All()
    {
        OneStatementParsesTo(
            "SELECT id, fname, lname FROM customer WHERE id = 1 LIMIT ALL",
            "SELECT id, fname, lname FROM customer WHERE id = 1");
    }

    [Fact]
    public void Parse_Cast()
    {
        var select = VerifiedOnlySelect("SELECT CAST(id AS BIGINT) FROM customer");
        var expected = new Cast(new Identifier("id"), new BigInt(), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS TINYINT) FROM customer");
        expected = new Cast(new Identifier("id"), new TinyInt(), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        OneStatementParsesTo(
            "SELECT CAST(id AS MEDIUMINT) FROM customer",
            "SELECT CAST(id AS MEDIUMINT) FROM customer");

        OneStatementParsesTo(
            "SELECT CAST(id AS BIGINT) FROM customer",
            "SELECT CAST(id AS BIGINT) FROM customer");

        VerifiedStatement("SELECT CAST(id AS NUMERIC) FROM customer");
        VerifiedStatement("SELECT CAST(id AS DEC) FROM customer");
        VerifiedStatement("SELECT CAST(id AS DECIMAL) FROM customer");

        select = VerifiedOnlySelect("SELECT CAST(id AS NVARCHAR(50)) FROM customer");
        expected = new Cast(new Identifier("id"), new Nvarchar(new CharacterLength.IntegerLength(50)), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS CLOB) FROM customer");
        expected = new Cast(new Identifier("id"), new Clob(), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS CLOB(50)) FROM customer");
        expected = new Cast(new Identifier("id"), new Clob(50), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS BINARY(50)) FROM customer");
        expected = new Cast(new Identifier("id"), new Binary(50), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS VARBINARY(50)) FROM customer");
        expected = new Cast(new Identifier("id"), new Varbinary(50), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS BLOB) FROM customer");
        expected = new Cast(new Identifier("id"), new Blob(), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(id AS BLOB(50)) FROM customer");
        expected = new Cast(new Identifier("id"), new Blob(50), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT CAST(details AS JSONB) FROM customer");
        expected = new Cast(new Identifier("details"), new JsonB(), CastKind.Cast);
        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Try_Cast()
    {
        var select = VerifiedOnlySelect("SELECT TRY_CAST(id AS BIGINT) FROM customer");

        var expected = new Cast(new Identifier("id"), new BigInt(), CastKind.TryCast);

        Assert.Equal(expected, select.Projection.Single().AsExpr());

        VerifiedStatement("SELECT TRY_CAST(id AS BIGINT) FROM customer");
        VerifiedStatement("SELECT TRY_CAST(id AS NUMERIC) FROM customer");
        VerifiedStatement("SELECT TRY_CAST(id AS DEC) FROM customer");
        VerifiedStatement("SELECT TRY_CAST(id AS DECIMAL) FROM customer");
    }

    [Fact]
    public void Parse_Extract()
    {
        var select = VerifiedOnlySelect("SELECT EXTRACT(YEAR FROM d)");

        var expected = new Extract(new Identifier("d"), new DateTimeField.Year(), ExtractSyntax.From);

        Assert.Equal(expected, select.Projection.Single().AsExpr());

        VerifiedStatement("SELECT EXTRACT(MONTH FROM d)");
        VerifiedStatement("SELECT EXTRACT(WEEK FROM d)");
        VerifiedStatement("SELECT EXTRACT(DAY FROM d)");
        VerifiedStatement("SELECT EXTRACT(DAYOFWEEK FROM d)");
        VerifiedStatement("SELECT EXTRACT(DAYOFYEAR FROM d)");
        VerifiedStatement("SELECT EXTRACT(DATE FROM d)");
        VerifiedStatement("SELECT EXTRACT(DATETIME FROM d)");
        VerifiedStatement("SELECT EXTRACT(HOUR FROM d)");
        VerifiedStatement("SELECT EXTRACT(MINUTE FROM d)");
        VerifiedStatement("SELECT EXTRACT(SECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(MILLISECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(MICROSECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(NANOSECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(CENTURY FROM d)");
        VerifiedStatement("SELECT EXTRACT(DECADE FROM d)");
        VerifiedStatement("SELECT EXTRACT(DOW FROM d)");
        VerifiedStatement("SELECT EXTRACT(DOY FROM d)");
        VerifiedStatement("SELECT EXTRACT(EPOCH FROM d)");
        VerifiedStatement("SELECT EXTRACT(ISODOW FROM d)");
        VerifiedStatement("SELECT EXTRACT(ISOWEEK FROM d)");
        VerifiedStatement("SELECT EXTRACT(ISOYEAR FROM d)");
        VerifiedStatement("SELECT EXTRACT(JULIAN FROM d)");
        VerifiedStatement("SELECT EXTRACT(MICROSECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(MICROSECONDS FROM d)");
        VerifiedStatement("SELECT EXTRACT(MILLENIUM FROM d)");
        VerifiedStatement("SELECT EXTRACT(MILLENNIUM FROM d)");
        VerifiedStatement("SELECT EXTRACT(MILLISECOND FROM d)");
        VerifiedStatement("SELECT EXTRACT(MILLISECONDS FROM d)");
        VerifiedStatement("SELECT EXTRACT(QUARTER FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIMEZONE FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIMEZONE_ABBR FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIMEZONE_HOUR FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIMEZONE_MINUTE FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIMEZONE_REGION FROM d)");
        VerifiedStatement("SELECT EXTRACT(TIME FROM d)");

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT EXTRACT(JIFFY FROM d)"));

        Assert.Equal("Expected date/time field, found JIFFY, Line: 1, Col: 16", ex.Message);

        VerifiedStatement("SELECT EXTRACT(JIFFY FROM d)", AllDialects.Where(d => d.AllowExtractCustom));
    }

    [Fact]
    public void Parse_Ceil_Number()
    {
        VerifiedStatement("SELECT CEIL(1.5)");
        VerifiedStatement("SELECT CEIL(float_column) FROM my_table");
    }

    [Fact]
    public void Parse_Ceil_Number_Scale()
    {
        VerifiedStatement("SELECT CEIL(1.5, 1)");
        VerifiedStatement("SELECT CEIL(float_column, 3) FROM my_table");
    }

    [Fact]
    public void Parse_Floor_Number()
    {
        VerifiedStatement("SELECT FLOOR(1.5)");
        VerifiedStatement("SELECT FLOOR(float_column) FROM my_table");
    }

    [Fact]
    public void Parse_Ceil_Datetime()
    {
        var select = VerifiedOnlySelect("SELECT CEIL(d TO DAY)");
        var expected = new Ceil(new Identifier("d"), new CeilFloorKind.DateTimeFieldKind(new DateTimeField.Day()));

        Assert.Equal(expected, select.Projection.Single().AsExpr());

        OneStatementParsesTo("SELECT CEIL(d to day)", "SELECT CEIL(d TO DAY)");

        VerifiedStatement("SELECT CEIL(d TO HOUR) FROM df");
        VerifiedStatement("SELECT CEIL(d TO MINUTE) FROM df");
        VerifiedStatement("SELECT CEIL(d TO SECOND) FROM df");
        VerifiedStatement("SELECT CEIL(d TO MILLISECOND) FROM df");

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT CEIL(d TO JIFFY) FROM df"));
        Assert.Equal("Expected date/time field, found JIFFY, Line: 1, Col: 18", ex.Message);

        VerifiedStatement("SELECT CEIL(d TO JIFFY) FROM df", AllDialects.Where(d => d.AllowExtractCustom));
    }

    [Fact]
    public void Parse_Ceil_Scale()
    {
        var select = VerifiedOnlySelect("SELECT CEIL(d, 2)");

        var expected = new Ceil(new Identifier("d"), new CeilFloorKind.Scale(new Value.Number("2")));
        Assert.Equal(expected, select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Floor_Scale()
    {
        var select = VerifiedOnlySelect("SELECT FLOOR(d, 2)");

        var expected = new Floor(new Identifier("d"), new CeilFloorKind.Scale(new Value.Number("2")));
        Assert.Equal(expected, select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Floor_Number_Scale()
    {
        VerifiedStatement("SELECT FLOOR(1.5, 1)");
        VerifiedStatement("SELECT FLOOR(float_column, 3) FROM my_table");
    }

    [Fact]
    public void Parse_Floor_Datetime()
    {
        var select = VerifiedOnlySelect("SELECT FLOOR(d TO DAY)");
        var expected = new Floor(new Identifier("d"), new CeilFloorKind.DateTimeFieldKind(new DateTimeField.Day()));

        Assert.Equal(expected, select.Projection.Single().AsExpr());

        OneStatementParsesTo("SELECT FLOOR(d to day)", "SELECT FLOOR(d TO DAY)");

        VerifiedStatement("SELECT FLOOR(d TO HOUR) FROM df");
        VerifiedStatement("SELECT FLOOR(d TO MINUTE) FROM df");
        VerifiedStatement("SELECT FLOOR(d TO SECOND) FROM df");
        VerifiedStatement("SELECT FLOOR(d TO MILLISECOND) FROM df");

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT FLOOR(d TO JIFFY) FROM df"));
        Assert.Equal("Expected date/time field, found JIFFY, Line: 1, Col: 19", ex.Message);

        VerifiedStatement("SELECT FLOOR(d TO JIFFY) FROM df", [new SnowflakeDialect(), new GenericDialect()]);
    }

    [Fact]
    public void Parse_Listagg()
    {
        var select =
            VerifiedOnlySelect(
                "SELECT LISTAGG(DISTINCT dateid, ', ' ON OVERFLOW TRUNCATE '%' WITHOUT COUNT) WITHIN GROUP (ORDER BY id, username)");

        VerifiedStatement("SELECT LISTAGG(sellerid) WITHIN GROUP (ORDER BY dateid)");
        VerifiedStatement("SELECT LISTAGG(dateid)");
        VerifiedStatement("SELECT LISTAGG(DISTINCT dateid)");
        VerifiedStatement("SELECT LISTAGG(dateid ON OVERFLOW ERROR)");
        VerifiedStatement("SELECT LISTAGG(dateid ON OVERFLOW TRUNCATE N'...' WITH COUNT)");
        VerifiedStatement("SELECT LISTAGG(dateid ON OVERFLOW TRUNCATE X'deadbeef' WITH COUNT)");

        var expected = new Function("LISTAGG")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(
                [
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("dateid"))),
                    new FunctionArg.Unnamed(
                        new FunctionArgExpression.FunctionExpression(
                            new LiteralValue(new Value.SingleQuotedString(", "))))
                ],
                DuplicateTreatment.Distinct,
                [
                    new FunctionArgumentClause.OnOverflow(new ListAggOnOverflow.Truncate
                    {
                        Filler = new LiteralValue(new Value.SingleQuotedString(Symbols.Percent.ToString()))
                    })
                ])),
            WithinGroup = new OrderByExpression[]
            {
                new(new Identifier("id")),
                new(new Identifier("username"))
            },
        };

        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Array_Arg_Func()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new DuckDbDialect(),
            new PostgreSqlDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new HiveDialect()
        };

        var sql = new[]
        {
            "SELECT ARRAY_AGG(x ORDER BY x) AS a FROM T",
            "SELECT ARRAY_AGG(x ORDER BY x LIMIT 2) FROM tbl",
            "SELECT ARRAY_AGG(DISTINCT x ORDER BY x LIMIT 2) FROM tbl",
        };

        foreach (var query in sql)
        {
            VerifiedStatement(query, dialects);
        }
    }

    [Fact]
    public void Parse_Create_Table()
    {
        const string sql = """
                           CREATE TABLE uk_cities (name VARCHAR(100) NOT NULL,
                            lat DOUBLE NULL,
                            lng DOUBLE,
                            constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0),
                            ref INT REFERENCES othertable (a, b),
                            ref2 INT references othertable2 on delete cascade on update no action,
                            constraint fkey foreign key (lat) references othertable3 (lat) on delete restrict,
                            constraint fkey2 foreign key (lat) references othertable4(lat) on delete no action on update restrict,
                            foreign key (lat) references othertable4(lat) on update set default on delete cascade,
                            FOREIGN KEY (lng) REFERENCES othertable4 (longitude) ON UPDATE SET NULL)
                           """;

        const string canonical = """
                                 CREATE TABLE uk_cities (name VARCHAR(100) NOT NULL,
                                  lat DOUBLE NULL,
                                  lng DOUBLE,
                                  constrained INT NULL CONSTRAINT pkey PRIMARY KEY NOT NULL UNIQUE CHECK (constrained > 0),
                                  ref INT REFERENCES othertable (a, b),
                                  ref2 INT REFERENCES othertable2 ON DELETE CASCADE ON UPDATE NO ACTION,
                                  CONSTRAINT fkey FOREIGN KEY (lat) REFERENCES othertable3(lat) ON DELETE RESTRICT,
                                  CONSTRAINT fkey2 FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE NO ACTION ON UPDATE RESTRICT,
                                  FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT,
                                  FOREIGN KEY (lng) REFERENCES othertable4(longitude) ON UPDATE SET NULL)
                                 """;

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical);
        var element = create.Element;

        var columns = new ColumnDef[]
        {
            new("name", new Varchar(new CharacterLength.IntegerLength(100)),
                Options: new ColumnOptionDef[] { new(new ColumnOption.NotNull()) }),
            new("lat", new DataType.Double(), Options: new ColumnOptionDef[] { new(Option: new ColumnOption.Null()) }),
            new("lng", new DataType.Double()),
            new("constrained", new Int(), Options: new ColumnOptionDef[]
            {
                new(new ColumnOption.Null()),
                new(new ColumnOption.Unique(true), "pkey"),
                new(new ColumnOption.NotNull()),
                new(new ColumnOption.Unique(false)),
                new(new ColumnOption.Check(VerifiedExpr("constrained > 0"))),
            }),
            new("ref", new Int(), Options: new[]
            {
                new ColumnOptionDef(new ColumnOption.ForeignKey("othertable", new Ident[] { "a", "b" }))
            }),
            new("ref2", new Int(), Options: new[]
            {
                new ColumnOptionDef(new ColumnOption.ForeignKey("othertable2",
                    OnDeleteAction: ReferentialAction.Cascade, OnUpdateAction: ReferentialAction.NoAction))
            })
        };

        var constraints = new TableConstraint.ForeignKey[]
        {
            new("othertable3", new Ident[] { "lat" })
            {
                Name = "fkey",
                OnDelete = ReferentialAction.Restrict,
                ReferredColumns = new Ident[] { "lat" }
            },
            new("othertable4", new Ident[] { "lat" })
            {
                Name = "fkey2",
                ReferredColumns = new Ident[] { "lat" },
                OnDelete = ReferentialAction.NoAction,
                OnUpdate = ReferentialAction.Restrict
            },
            new("othertable4", new Ident[] { "lat" })
            {
                ReferredColumns = new Ident[] { "lat" },
                OnDelete = ReferentialAction.Cascade,
                OnUpdate = ReferentialAction.SetDefault
            },
            new("othertable4", new Ident[] { "lng" })
            {
                ReferredColumns = new Ident[] { "longitude" },
                OnUpdate = ReferentialAction.SetNull
            }
        };

        Assert.Equal("uk_cities", element.Name);
        Assert.Equal(columns[0], element.Columns[0]);
        Assert.Equal(columns[1], element.Columns[1]);
        Assert.Equal(columns[2], element.Columns[2]);
        Assert.Equal(columns[3], element.Columns[3]);
        Assert.Equal(columns[4], element.Columns[4]);
        Assert.Equal(columns[5], element.Columns[5]);

        Assert.Equal(columns, element.Columns);
        Assert.Equal(constraints, element.Constraints!);
    }

    [Fact]
    public void Parse_Create_Table_With_Constraint_Characteristics()
    {
        const string sql = """
                           CREATE TABLE uk_cities (
                           name VARCHAR(100) NOT NULL,
                            lat DOUBLE NULL,
                            lng DOUBLE,
                            constraint fkey foreign key (lat) references othertable3 (lat) on delete restrict deferrable initially deferred,
                            constraint fkey2 foreign key (lat) references othertable4(lat) on delete no action on update restrict deferrable initially immediate,
                            foreign key (lat) references othertable4(lat) on update set default on delete cascade not deferrable initially deferred not enforced,
                            FOREIGN KEY (lng) REFERENCES othertable4 (longitude) ON UPDATE SET NULL enforced not deferrable initially immediate
                           )
                           """;

        const string canonical = """
                                 CREATE TABLE uk_cities (
                                 name VARCHAR(100) NOT NULL,
                                  lat DOUBLE NULL,
                                  lng DOUBLE,
                                  CONSTRAINT fkey FOREIGN KEY (lat) REFERENCES othertable3(lat) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
                                  CONSTRAINT fkey2 FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE NO ACTION ON UPDATE RESTRICT DEFERRABLE INITIALLY IMMEDIATE,
                                  FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT NOT DEFERRABLE INITIALLY DEFERRED NOT ENFORCED,
                                  FOREIGN KEY (lng) REFERENCES othertable4(longitude) ON UPDATE SET NULL NOT DEFERRABLE INITIALLY IMMEDIATE ENFORCED)
                                 """;

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical);
        var element = create.Element;

        var columns = new ColumnDef[]
        {
            new("name", new Varchar(new CharacterLength.IntegerLength(100)),
                Options: new ColumnOptionDef[] { new(new ColumnOption.NotNull()) }),
            new("lat", new DataType.Double(), Options: new ColumnOptionDef[] { new(Option: new ColumnOption.Null()) }),
            new("lng", new DataType.Double())
        };

        var constraints = new TableConstraint.ForeignKey[]
        {
            new("othertable3", new Ident[] { "lat" })
            {
                Name = "fkey",
                OnDelete = ReferentialAction.Restrict,
                ReferredColumns = new Ident[] { "lat" },
                Characteristics = new ConstraintCharacteristics
                {
                    Deferrable = true,
                    Initially = DeferrableInitial.Deferred
                }
            },
            new("othertable4", new Ident[] { "lat" })
            {
                Name = "fkey2",
                ReferredColumns = new Ident[] { "lat" },
                OnDelete = ReferentialAction.NoAction,
                OnUpdate = ReferentialAction.Restrict,
                Characteristics = new ConstraintCharacteristics
                {
                    Deferrable = true,
                    Initially = DeferrableInitial.Immediate
                }
            },
            new("othertable4", new Ident[] { "lat" })
            {
                ReferredColumns = new Ident[] { "lat" },
                OnDelete = ReferentialAction.Cascade,
                OnUpdate = ReferentialAction.SetDefault,
                Characteristics = new ConstraintCharacteristics
                {
                    Deferrable = false,
                    Initially = DeferrableInitial.Deferred,
                    Enforced = false
                }
            },
            new("othertable4", new Ident[] { "lng" })
            {
                ReferredColumns = new Ident[] { "longitude" },
                OnUpdate = ReferentialAction.SetNull,
                Characteristics = new ConstraintCharacteristics
                {
                    Deferrable = false,
                    Initially = DeferrableInitial.Immediate,
                    Enforced = true
                }
            }
        };

        Assert.Equal("uk_cities", element.Name);
        Assert.Equal(columns[0], element.Columns[0]);
        Assert.Equal(columns[1], element.Columns[1]);
        Assert.Equal(columns[2], element.Columns[2]);

        Assert.Equal(columns, element.Columns);
        Assert.Equal(constraints, element.Constraints!);

        Assert.Throws<ParserException>(() => ParseSqlStatements(
            """
            CREATE TABLE t (
            a int NOT NULL,
                FOREIGN KEY (a) REFERENCES othertable4(a) ON DELETE CASCADE ON UPDATE SET DEFAULT DEFERRABLE INITIALLY IMMEDIATE NOT DEFERRABLE, \
            )
            """));
        Assert.Throws<ParserException>(() => ParseSqlStatements(
            """
            CREATE TABLE t (
            a int NOT NULL,
             FOREIGN KEY (a) REFERENCES othertable4(a) ON DELETE CASCADE ON UPDATE SET DEFAULT NOT ENFORCED INITIALLY DEFERRED ENFORCED, \
            )
            """));
        Assert.Throws<ParserException>(() => ParseSqlStatements(
            """
            CREATE TABLE t (
            a int NOT NULL,
            FOREIGN KEY (lat) REFERENCES othertable4(lat) ON DELETE CASCADE ON UPDATE SET DEFAULT INITIALLY DEFERRED INITIALLY IMMEDIATE, \
            )
            """));
    }

    [Fact]
    public void Parse_Create_Table_Column_Constraint_Characteristics()
    {
        foreach (var deferrable in new bool?[] { null, false, true })
        {
            foreach (var initially in new DeferrableInitial?[]
                         { null, DeferrableInitial.Immediate, DeferrableInitial.Deferred })
            {
                foreach (var enforced in new bool?[] { null, false, true })
                {
                    var deferrableText = string.Empty;
                    if (deferrable.HasValue)
                    {
                        deferrableText = deferrable.Value ? "DEFERRABLE" : "NOT DEFERRABLE";
                    }

                    var initiallyText = string.Empty;
                    if (initially.HasValue)
                    {
                        initiallyText = initially.Value == DeferrableInitial.Immediate
                            ? "INITIALLY IMMEDIATE"
                            : "INITIALLY DEFERRED";
                    }

                    var enforcedText = string.Empty;
                    if (enforced.HasValue)
                    {
                        enforcedText = enforced.Value ? "ENFORCED" : "NOT ENFORCED";
                    }

                    var parts = new[] { deferrableText, initiallyText, enforcedText }.Where(t =>
                        !string.IsNullOrEmpty(t));

                    var syntax = string.Join(" ", parts);

                    Test(syntax, deferrable, initially, enforced);
                }
            }
        }

        return;

        void Test(string syntax, bool? deferrable, DeferrableInitial? initially, bool? enforced)
        {
            var sql = $"CREATE TABLE t (a int UNIQUE {syntax})";
            var expectedClause = string.IsNullOrWhiteSpace(syntax) ? "" : $" {syntax}";

            var expected = $"CREATE TABLE t (a INT UNIQUE{expectedClause})";
            var ast = (Statement.CreateTable)OneStatementParsesTo(sql, expected);

            var expectedValue = deferrable.HasValue || initially.HasValue || enforced.HasValue
                ? new ConstraintCharacteristics
                {
                    Initially = initially,
                    Deferrable = deferrable,
                    Enforced = enforced
                }
                : null;

            var columnDef = new Sequence<ColumnDef>
            {
                new("a", new Int(), Options: new Sequence<ColumnOptionDef>
                {
                    new(new ColumnOption.Unique(false)
                    {
                        Characteristics = expectedValue
                    })
                })
            };

            Assert.Equal(columnDef, ast.Element.Columns);
        }
    }

    [Fact]
    public void Parse_Create_Table_Hive_Array()
    {
        // Parsing [] type arrays does not work in MsSql since [ is used in IsDelimitedIdentifierStart
        var dialects = new List<(Dialect Dialect, bool AngleBracketSyntax)>
        {
            (new PostgreSqlDialect(), false),
            (new HiveDialect(), true)
        };


        foreach (var (dialect, angleBracketSyntax) in dialects)
        {
            var syntax = angleBracketSyntax ? "ARRAY<INT>" : "INT[]";
            var sql = $"CREATE TABLE IF NOT EXISTS something (name INT, val {syntax})";

            var create = (Statement.CreateTable)OneStatementParsesTo(sql, sql, [dialect]);

            ArrayElementTypeDef expected = new ArrayElementTypeDef.SquareBracket(new Int());
            if (angleBracketSyntax)
            {
                expected = new ArrayElementTypeDef.AngleBracket(new Int());
            }


            var columns = new ColumnDef[]
            {
                new(new Ident("name"), new Int()),
                new(new Ident("val"), new DataType.Array(expected))
            };

            Assert.True(create.Element.IfNotExists);
            Assert.Equal((ObjectName)"something", create.Element.Name);
            Assert.Equal(columns, create.Element.Columns);
        }

        var testDialects = new Dialect[]
        {
            new PostgreSqlDialect(),
            new HiveDialect(),
            new MySqlDialect()
        };

        var ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("CREATE TABLE IF NOT EXISTS something (name int, val array<int)", testDialects));
        Assert.Equal("Expected >, found ), Line: 1, Col: 62", ex.Message);
    }

    [Fact]
    public void Parse_Create_Table_With_Multiple_On_Delete_In_Constraint_Fails()
    {
        const string sql = """
                           create table X (
                               y_id int,
                               foreign key (y_id) references Y (id) on delete cascade on update cascade on delete no action
                           )
                           """;

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
        Assert.Equal("Expected ',' or ')' after column definition, found on, Line: 3, Col: 78", ex.Message);
    }

    [Fact]
    public void Parse_Create_Table_With_Multiple_On_Delete_Fails()
    {
        const string sql = """
                           create table X (
                               y_id int references Y (id)
                               on delete cascade on update cascade on delete no action
                           )
                           """;

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
        Assert.Equal("Expected ',' or ')' after column definition, found on, Line: 3, Col: 41", ex.Message);
    }

    [Fact]
    public void Parse_Assert()
    {
        var statement = OneStatementParsesTo<Statement.Assert>(
            "ASSERT (SELECT COUNT(*) FROM my_table) > 0",
            "ASSERT (SELECT COUNT(*) FROM my_table) > 0");

        Assert.Null(statement.Message);
    }

    [Fact]
    public void Parse_Assert_Message()
    {
        var statement = OneStatementParsesTo<Statement.Assert>(
            "ASSERT (SELECT COUNT(*) FROM my_table) > 0 AS 'No rows in my_table'",
            "ASSERT (SELECT COUNT(*) FROM my_table) > 0 AS 'No rows in my_table'");

        var expected = new LiteralValue(new Value.SingleQuotedString("No rows in my_table"));
        Assert.Equal(expected, statement.Message);
    }

    [Fact]
    public void Parse_Create_Schema()
    {
        var schema = VerifiedStatement<Statement.CreateSchema>("CREATE SCHEMA X");
        Assert.IsType<SchemaName.Simple>(schema.Name);
        Assert.Equal("X", schema.Name.ToSql());
    }

    [Fact]
    public void Parse_Create_Schema_With_Authorization()
    {
        var schema = VerifiedStatement<Statement.CreateSchema>("CREATE SCHEMA AUTHORIZATION Y");
        Assert.IsType<SchemaName.UnnamedAuthorization>(schema.Name);
        Assert.Equal("AUTHORIZATION Y", schema.Name.ToSql());
    }

    [Fact]
    public void Parse_Create_Schema_With_Name_And_Authorization()
    {
        var schema = VerifiedStatement<Statement.CreateSchema>("CREATE SCHEMA X AUTHORIZATION Y");
        Assert.IsType<SchemaName.NamedAuthorization>(schema.Name);
        Assert.Equal("X AUTHORIZATION Y", schema.Name.ToSql());
    }

    [Fact]
    public void Parse_Drop_Schema()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP SCHEMA X");

        Assert.Equal(ObjectType.Schema, drop.ObjectType);
    }

    [Fact]
    public void Parse_Create_Table_As()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t AS SELECT * FROM a");
        var element = create.Element;

        Assert.Equal("t", element.Name);
        Assert.Equal(VerifiedQuery("SELECT * FROM a"), element.Query);

        // BigQuery allows specifying table schema in CTAS
        create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a");
        element = create.Element;
        Assert.Equal(2, element.Columns.Count);
        Assert.Equal("a INT", element.Columns[0].ToSql());
        Assert.Equal("b INT", element.Columns[1].ToSql());
        Assert.Equal(VerifiedQuery("SELECT 1 AS b, 2 AS a"), element.Query);
    }

    [Fact]
    public void Parse_Create_Table_As_Table()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE new_table AS TABLE old_table");
        var element = create.Element;

        var expected = new Query(new SetExpression.TableExpression(new Table("old_table")));

        Assert.Equal((ObjectName)"new_table", element.Name);
        Assert.Equal(expected, element.Query);

        expected = new Query(new SetExpression.TableExpression(new Table("old_table", "schema_name")));

        create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE new_table AS TABLE schema_name.old_table");
        element = create.Element;
        Assert.Equal((ObjectName)"new_table", element.Name);
        Assert.Equal(expected, element.Query);
    }

    [Fact]
    public void Parse_Create_Table_On_Cluster()
    {
        DefaultDialects = new List<Dialect> { new GenericDialect() };
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t ON CLUSTER '{cluster}' (a INT, b INT)");

        var expected = new Statement.CreateTable(new CreateTable("t", new ColumnDef[]
        {
            new("a", new Int()),
            new("b", new Int()),
        })
        {
            OnCluster = new Ident("{cluster}", Symbols.SingleQuote)
        });

        Assert.Equal(expected, create);

        create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t ON CLUSTER my_cluster (a INT, b INT)");

        expected = new Statement.CreateTable(new CreateTable("t", new[]
        {
            new ColumnDef("a", new Int()),
            new ColumnDef("b", new Int()),
        })
        {
            OnCluster = "my_cluster"
        });

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Parse_Create_Or_Replace_Table()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE OR REPLACE TABLE t (a INT)");
        var element = create.Element;
        Assert.Equal("t", element.Name);
        Assert.True(element.OrReplace);

        create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE t (a INT, b INT) AS SELECT 1 AS b, 2 AS a");
        var expected = new Statement.CreateTable(new CreateTable("t", new ColumnDef[]
        {
            new("a", new Int()),
            new("b", new Int()),
        })
        {
            Query = VerifiedQuery("SELECT 1 AS b, 2 AS a")
        });

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Parse_Create_Table_With_OnDelete_OnUpdate_2in_Any_Order()
    {
        const string prefix = "create table X (y_id int references Y (id) ";
        ParseSqlStatements($"{prefix}on update cascade on delete no action)");
        ParseSqlStatements($"{prefix}on delete cascade on update cascade)");
        ParseSqlStatements($"{prefix}on update no action)");
        ParseSqlStatements($"{prefix}on delete restrict)");
    }

    [Fact]
    public void Parse_Create_Table_With_Options()
    {
        DefaultDialects = new List<Dialect> { new GenericDialect() };
        const string prefix = "create table X (y_id int references Y (id) ";
        ParseSqlStatements($"{prefix}on update cascade on delete no action)");
        ParseSqlStatements($"{prefix}on delete cascade on update cascade)");
        ParseSqlStatements($"{prefix}on update no action)");
        ParseSqlStatements($"{prefix}on delete restrict)");
    }

    [Fact]
    public void Parse_Create_Table_Clone()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE OR REPLACE TABLE a CLONE a_tmp");

        Assert.Equal("a", create.Element.Name);
        Assert.Equal((ObjectName)"a_tmp", create.Element.CloneClause);
    }

    [Fact]
    public void Parse_Create_Table_Trailing_Comma()
    {
        DefaultDialects = [new DuckDbDialect()];
        OneStatementParsesTo(
            "CREATE TABLE foo (bar int,)",
            "CREATE TABLE foo (bar INT)");
    }

    [Fact]
    public void Parse_Create_External_Table()
    {
        const string sql = """
                           CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NULL, lng DOUBLE)
                           STORED AS TEXTFILE LOCATION '/tmp/example.csv'
                           """;

        const string canonical = """
                                 CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NULL, lng DOUBLE)
                                  STORED AS TEXTFILE LOCATION '/tmp/example.csv'
                                 """;

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical);

        var columns = new ColumnDef[]
        {
            new("name", new Varchar(new CharacterLength.IntegerLength(100)), Options: new ColumnOptionDef[]
            {
                new(Option: new ColumnOption.NotNull())
            }),
            new("lat", new DataType.Double(), Options: new ColumnOptionDef[]
            {
                new(Option: new ColumnOption.Null())
            }),
            new("lng", new DataType.Double())
        };

        Assert.Equal("uk_cities", create.Element.Name);
        Assert.Equal(columns, create.Element.Columns);
        Assert.True(create.Element.External);
        Assert.Equal("/tmp/example.csv", create.Element.Location);
        Assert.False(create.Element.IfNotExists);
    }

    [Fact]
    public void Parse_Create_Or_Replace_External_Table()
    {
        const string sql =
            "CREATE OR REPLACE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL) STORED AS TEXTFILE LOCATION '/tmp/example.csv'";

        const string canonical =
            "CREATE OR REPLACE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL) STORED AS TEXTFILE LOCATION '/tmp/example.csv'";

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical);

        var columns = new ColumnDef[]
        {
            new("name", new Varchar(new CharacterLength.IntegerLength(100)), Options: new Sequence<ColumnOptionDef>
            {
                new(Option: new ColumnOption.NotNull())
            }),
        };

        Assert.Equal("uk_cities", create.Element.Name);
        Assert.Equal(columns, create.Element.Columns);
        Assert.True(create.Element.External);
        Assert.Equal(FileFormat.TextFile, create.Element.FileFormat);
        Assert.Equal("/tmp/example.csv", create.Element.Location);
        Assert.False(create.Element.IfNotExists);
        Assert.True(create.Element.OrReplace);
    }

    [Fact]
    public void Parse_Create_External_Table_Lowercase()
    {
        const string sql =
            "create external table uk_cities (name varchar(100) not null, lat double null, lng double)stored as parquet location '/tmp/example.csv'";

        const string canonical =
            "CREATE EXTERNAL TABLE uk_cities (name VARCHAR(100) NOT NULL, lat DOUBLE NULL, lng DOUBLE) STORED AS PARQUET LOCATION '/tmp/example.csv'";

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, canonical);

        var columns = new ColumnDef[]
        {
            new("name", new Varchar(new CharacterLength.IntegerLength(100)), Options: new ColumnOptionDef[]
            {
                new(Option: new ColumnOption.NotNull())
            }),
            new("lat", new DataType.Double(), Options: new ColumnOptionDef[]
            {
                new(Option: new ColumnOption.Null())
            }),
            new("lng", new DataType.Double())
        };

        Assert.Equal("uk_cities", create.Element.Name);
        Assert.Equal(columns, create.Element.Columns);
        Assert.Equal("/tmp/example.csv", create.Element.Location);
    }

    [Fact]
    public void Parse_Alter_Table()
    {
        var alter = OneStatementParsesTo<Statement.AlterTable>(
            "ALTER TABLE tab ADD COLUMN foo TEXT;",
            "ALTER TABLE tab ADD COLUMN foo TEXT");

        var addColumn = (AlterTableOperation.AddColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.True(addColumn.ColumnKeyword);
        Assert.False(addColumn.IfNotExists);
        Assert.Equal("foo", addColumn.ColumnDef.Name);
        Assert.IsType<Text>(addColumn.ColumnDef.DataType);

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab RENAME TO new_tab");
        var alterColumn = (AlterTableOperation.RenameTable)alter.Operations.First();
        Assert.Equal("tab", alter.Name);
        Assert.Equal("new_tab", alterColumn.Name);

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab RENAME COLUMN foo TO new_foo");
        var renameColumn = (AlterTableOperation.RenameColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("foo", renameColumn.OldColumnName);
        Assert.Equal("new_foo", renameColumn.NewColumnName);
    }

    [Fact]
    public void Parse_Alter_Index()
    {
        var alter = VerifiedStatement<Statement.AlterIndex>("ALTER INDEX idx RENAME TO new_idx");
        var index = (AlterIndexOperation.RenameIndex)alter.Operation;

        Assert.Equal("idx", alter.Name);
        Assert.Equal("new_idx", index.Name);
    }

    [Fact]
    public void Parse_Alter_Table_Add_Column()
    {
        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD foo TEXT");
        var op = (AlterTableOperation.AddColumn)alter.Operations.First();

        Assert.False(op.ColumnKeyword);

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD COLUMN foo TEXT");
        op = (AlterTableOperation.AddColumn)alter.Operations.First();
        Assert.True(op.ColumnKeyword);
    }

    [Fact]
    public void Parse_Alter_Table_Add_Column_If_Not_Exists()
    {
        var dialects = new Dialect[]
        {
            new PostgreSqlDialect(),
            new BigQueryDialect(),
            new GenericDialect(),
            new DuckDbDialect()
        };

        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD IF NOT EXISTS foo TEXT", dialects);
        var op = (AlterTableOperation.AddColumn)alter.Operations.First();
        Assert.True(op.IfNotExists);

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD COLUMN IF NOT EXISTS foo TEXT", dialects);
        op = (AlterTableOperation.AddColumn)alter.Operations.First();
        Assert.True(op.ColumnKeyword);
        Assert.True(op.IfNotExists);
    }

    [Fact]
    public void Parse_Alter_Table_Constraints()
    {
        Test("CONSTRAINT address_pkey PRIMARY KEY (address_id)");
        Test("CONSTRAINT uk_task UNIQUE (report_date, task_id)");
        Test("CONSTRAINT customer_address_id_fkey FOREIGN KEY (address_id) REFERENCES public.address(address_id)");
        Test("CONSTRAINT ck CHECK (rtrim(ltrim(REF_CODE)) <> '')");
        Test("PRIMARY KEY (foo, bar)");
        Test("UNIQUE (id)");
        Test("FOREIGN KEY (foo, bar) REFERENCES AnotherTable(foo, bar)");
        Test("CHECK (end_date > start_date OR end_date IS NULL)");
        return;

        void Test(string constrain)
        {
            var sql = $"ALTER TABLE tab ADD {constrain}";

            var alterTable = AlterTableOp(VerifiedStatement(sql));

            if (alterTable is AlterTableOperation.AddConstraint addConstraint)
            {
                Assert.Equal(constrain, addConstraint.TableConstraint.ToSql());
            }
            else
            {
                VerifiedStatement($"CREATE TABLE foo (id INT, {constrain})");
            }
        }
    }

    [Fact]
    public void Parse_Alter_Table_Drop_Column()
    {
        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE");
        var constraint = (AlterTableOperation.DropColumn)alter.Operations.First();
        Assert.Equal("tab", alter.Name);
        Assert.Equal("is_active", constraint.Name);
        Assert.True(constraint.IfExists);
        Assert.True(constraint.Cascade);

        OneStatementParsesTo(
            "ALTER TABLE tab DROP IF EXISTS is_active CASCADE",
            "ALTER TABLE tab DROP COLUMN IF EXISTS is_active CASCADE");

        OneStatementParsesTo(
            "ALTER TABLE tab DROP is_active CASCADE",
            "ALTER TABLE tab DROP COLUMN is_active CASCADE");
    }

    [Fact]
    public void Parse_Alter_Table_Alter_Column()
    {
        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ALTER COLUMN is_active SET NOT NULL");
        var op = (AlterTableOperation.AlterColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("is_active", op.ColumnName);
        Assert.IsType<AlterColumnOperation.SetNotNull>(op.Operation);

        OneStatementParsesTo(
            "ALTER TABLE tab ALTER is_active DROP NOT NULL",
            "ALTER TABLE tab ALTER COLUMN is_active DROP NOT NULL");

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ALTER COLUMN is_active SET DEFAULT false");
        op = (AlterTableOperation.AlterColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("is_active", op.ColumnName);
        Assert.Equal(new AlterColumnOperation.SetDefault(new LiteralValue(new Value.Boolean(false))), op.Operation);


        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ALTER COLUMN is_active DROP DEFAULT");
        op = (AlterTableOperation.AlterColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("is_active", op.ColumnName);
        Assert.Equal(new AlterColumnOperation.DropDefault(), op.Operation);
    }

    [Fact]
    public void Parse_Alter_Table_Alter_Column_Type()
    {
        var alter = VerifiedStatement<Statement.AlterTable>(
            "ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT");
        var op = (AlterTableOperation.AlterColumn)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("is_active", op.ColumnName);
        Assert.IsType<AlterColumnOperation.SetDataType>(op.Operation);

        var ex = Assert.Throws<ParserException>(() =>
            new Parser().ParseSql("ALTER TABLE tab ALTER COLUMN is_active TYPE TEXT"));
        Assert.Equal(
            "Expected SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN, found TYPE, Line: 1, Col: 40",
            ex.Message);

        ex = Assert.Throws<ParserException>(() =>
            new Parser().ParseSql("ALTER TABLE tab ALTER COLUMN is_active SET DATA TYPE TEXT USING 'text'"));
        Assert.Equal("Expected end of statement, found USING, Line: 1, Col: 59", ex.Message);
    }

    [Fact]
    public void Parse_Alter_Table_Drop_Constraint()
    {
        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab DROP CONSTRAINT constraint_name CASCADE");
        var op = (AlterTableOperation.DropConstraint)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("constraint_name", op.Name);
        Assert.False(op.IfExists);
        Assert.True(op.Cascade);


        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab DROP CONSTRAINT IF EXISTS constraint_name");
        op = (AlterTableOperation.DropConstraint)alter.Operations.First();

        Assert.Equal("tab", alter.Name);
        Assert.Equal("constraint_name", op.Name);
        Assert.True(op.IfExists);
        Assert.False(op.Cascade);

        var ex = Assert.Throws<ParserException>(() =>
            new Parser().ParseSql("ALTER TABLE tab DROP CONSTRAINT is_active TEXT"));
        Assert.Equal("Expected end of statement, found TEXT, Line: 1, Col: 43", ex.Message);
    }

    [Fact]
    public void Pars_Bad_Constraint()
    {
        var ex = Assert.Throws<ParserException>(() => new Parser().ParseSql("ALTER TABLE tab ADD"));
        Assert.Equal("Expected identifier, found EOF", ex.Message);

        ex = Assert.Throws<ParserException>(() => new Parser().ParseSql("CREATE TABLE tab (foo int,"));
        Assert.Equal("Expected column name or constraint definition, found EOF", ex.Message);
    }

    [Fact]
    public void Parse_Scalar_Function_In_Projection()
    {
        var names = new[] { "sqrt", "foo" };

        foreach (var fnName in names)
        {
            var sql = $"SELECT {fnName}(id) FROM foo";
            var select = VerifiedOnlySelect(sql);
            var expected = new Function(fnName)
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("id")))
                ]))
            };

            Assert.Equal(expected, select.Projection.Single().AsExpr());
        }
    }

    [Fact]
    public void Parse_Explain_Table()
    {
        Test("EXPLAIN test_identifier", DescribeAlias.Explain);
        Test("DESCRIBE test_identifier", DescribeAlias.Describe);
        Test("DESC test_identifier", DescribeAlias.Desc);

        return;

        void Test(string sql, DescribeAlias expected, bool hasTable = false)
        {
            var explain = VerifiedStatement<Statement.ExplainTable>(sql);
            Assert.Equal(expected, explain.DescribeAlias);
            Assert.Equal("test_identifier", explain.Name);
            Assert.Equal(hasTable, explain.HasTableKeyword);
        }
    }

    [Fact]
    public void Explain_Describe()
    {
        VerifiedStatement("DESCRIBE test.table");
    }

    [Fact]
    public void Explain_Desc()
    {
        VerifiedStatement("DESC test.table");
    }

    [Fact]
    public void Parse_Explain_Analyze_With_Simple_Select()
    {
        TestExplain("DESCRIBE SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None);
        TestExplain("EXPLAIN SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None);
        TestExplain("EXPLAIN VERBOSE SELECT sqrt(id) FROM foo", true, false, AnalyzeFormat.None);
        TestExplain("EXPLAIN ANALYZE SELECT sqrt(id) FROM foo", false, true, AnalyzeFormat.None);
        TestExplain("EXPLAIN ANALYZE VERBOSE SELECT sqrt(id) FROM foo", true, true, AnalyzeFormat.None);
        TestExplain("EXPLAIN ANALYZE FORMAT GRAPHVIZ SELECT sqrt(id) FROM foo", false, true, AnalyzeFormat.Graphviz);
        TestExplain("EXPLAIN ANALYZE VERBOSE FORMAT JSON SELECT sqrt(id) FROM foo", true, true, AnalyzeFormat.Json);
        TestExplain("EXPLAIN VERBOSE FORMAT TEXT SELECT sqrt(id) FROM foo", true, false, AnalyzeFormat.Text);
    }

    [Fact]
    public void Parse_Named_Argument_Function()
    {
        var select = VerifiedOnlySelect("SELECT FUN(a => '1', b => '2') FROM foo");
        var expected = new Function("FUN")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Named(
                    "a",
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("1"))),
                    new FunctionArgOperator.RightArrow()),
                new FunctionArg.Named(
                    "b",
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("2"))),
                    new FunctionArgOperator.RightArrow())
            ]))
        };
        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Named_Argument_Function_With_Eq_Operator()
    {
        DefaultDialects = AllDialects.Where(d => d.SupportsNamedFunctionArgsWithEqOperator);

        var select = VerifiedOnlySelect("SELECT FUN(a = '1', b = '2') FROM foo");
        var expected = new Function("FUN")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(
            [
                new FunctionArg.Named(
                    "a",
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("1"))),
                    new FunctionArgOperator.Equal()),
                new FunctionArg.Named(
                    "b",
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("2"))),
                    new FunctionArgOperator.Equal())
            ]))
        };
        Assert.Equal(expected, select.Projection.Single().AsExpr());


        var dialects = AllDialects.Where(d => !d.SupportsNamedFunctionArgsWithEqOperator).ToList();

        expected = new Function("foo")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(
            [
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                    new BinaryOp(
                        new Identifier("bar"),
                        BinaryOperator.Eq,
                        new LiteralValue(new Value.Number("42"))
                    )))
            ]))
        };
        var actual = VerifiedExpr("foo(bar = 42)", dialects);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Parse_Window_Functions()
    {
        const string sql = """
                           SELECT row_number() OVER (ORDER BY dt DESC),
                            sum(foo) OVER (PARTITION BY a, b ORDER BY c, d
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                            avg(bar) OVER (ORDER BY a
                            RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING),
                            sum(bar) OVER (ORDER BY a
                            RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '1 MONTH' FOLLOWING),
                            COUNT(*) OVER (ORDER BY a
                            RANGE BETWEEN INTERVAL '1 DAY' PRECEDING AND INTERVAL '1 DAY' FOLLOWING),
                            max(baz) OVER (ORDER BY a
                            ROWS UNBOUNDED PRECEDING),
                            sum(qux) OVER (ORDER BY a
                            GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
                            FROM foo
                           """;

        var select = VerifiedOnlySelect(sql, AllDialects.Where(d => !d.RequireIntervalQualifier));

        var expected = new Function("row_number")
        {
            Over = new WindowType.WindowSpecType(new WindowSpec(OrderBy: new OrderByExpression[]
            {
                new(new Identifier("dt"), false)
            })),
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        };
        Assert.Equal(7, select.Projection.Count);
        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Named_Window_Functions()
    {
        var sql = """
                  SELECT row_number() OVER (w ORDER BY dt DESC),
                   sum(foo) OVER (win PARTITION BY a, b ORDER BY c, d
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                   FROM foo
                   WINDOW w AS (PARTITION BY x), win AS (ORDER BY y)
                  """;

        List<Dialect> dialects =
            [new GenericDialect(), new PostgreSqlDialect(), new MySqlDialect(), new BigQueryDialect()];
        VerifiedStatement(sql, dialects);

        var select = VerifiedOnlySelect(sql);

        Assert.Equal(2, select.Projection.Count);
        var expectedWindowNames = new Sequence<string> { "w", "win" };

        foreach (var spec in select.Projection
                     .Select(projection => (Function)projection.AsExpr())
                     .Select(fn => (WindowType.WindowSpecType)fn.Over!))
        {
            Assert.Contains(spec.Spec.WindowName!.Value, expectedWindowNames);
        }

        sql = """
              SELECT
               FIRST_VALUE(x) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first,
               FIRST_VALUE(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last,
               SUM(y) OVER (win PARTITION BY x) AS last
               FROM EMPLOYEE
               WINDOW w AS (PARTITION BY x), win AS (w ORDER BY y)
              """;

        VerifiedStatement(sql, dialects);
    }

    [Fact]
    public void Parse_Window_Clause()
    {
        var sql = """
                  SELECT *
                   FROM mytable
                   WINDOW
                   window1 AS (ORDER BY 1 ASC, 2 DESC, 3 NULLS FIRST),
                   window2 AS (window1),
                   window3 AS (PARTITION BY a, b, c),
                   window4 AS (ROWS UNBOUNDED PRECEDING),
                   window5 AS (window1 PARTITION BY a),
                   window6 AS (window1 ORDER BY a),
                   window7 AS (window1 ROWS UNBOUNDED PRECEDING),
                   window8 AS (window1 PARTITION BY a ORDER BY b ROWS UNBOUNDED PRECEDING)
                   ORDER BY C3
                  """;

        VerifiedOnlySelect(sql);

        sql = "SELECT from mytable WINDOW window1 AS window2";

        var dialects = AllDialects.Where(d => !d.SupportsWindowClauseNamedWindowReference).ToList();
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql, dialects));
    }

    [Fact]
    public void Parse_Window_Clause_Named_Window()
    {
        const string sql = "SELECT * FROM mytable WINDOW window1 AS window2";
        var dialects = AllDialects.Where(d => d.SupportsWindowClauseNamedWindowReference).ToList();
        var window = VerifiedOnlySelect(sql, dialects: dialects).NamedWindow;
        var expected = new Sequence<NamedWindowDefinition>
        {
            new("window1", new NamedWindowExpression.NamedWindow("window2"))
        };

        Assert.Equal(expected, window);
    }

    [Fact]
    public void Test_Parse_Named_Window()
    {
        const string sql = """
                           SELECT
                            MIN(c12) OVER window1 AS min1,
                            MAX(c12) OVER window2 AS max1
                            FROM aggregate_test_100
                            WINDOW window1 AS (ORDER BY C12),
                            window2 AS (PARTITION BY C11)
                            ORDER BY C3
                           """;

        var actual = VerifiedOnlySelect(sql);

        var projection = new Sequence<SelectItem>
        {
            new SelectItem.ExpressionWithAlias(new Function("MIN")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("c12")))
                ])),
                Over = new WindowType.NamedWindow("window1")
            }, "min1"),

            new SelectItem.ExpressionWithAlias(new Function("MAX")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("c12")))
                ])),
                Over = new WindowType.NamedWindow("window2")
            }, "max1"),
        };

        var expected = new Select(projection)
        {
            From = new TableWithJoins[]
            {
                new(new TableFactor.Table("aggregate_test_100"))
            },
            NamedWindow = new Sequence<NamedWindowDefinition>
            {
                new("window1", new NamedWindowExpression.NamedWindowSpec(new WindowSpec(
                    OrderBy: new Sequence<OrderByExpression>
                    {
                        new(new Identifier("C12"))
                    }))),

                new("window2", new NamedWindowExpression.NamedWindowSpec(new WindowSpec(new Sequence<Expression>
                {
                    new Identifier("C11")
                })))
            },
            WindowBeforeQualify = true
        };

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Parse_Window_And_Qualify_Clause()
    {
        var sql = """
                  SELECT
                   MIN(c12) OVER window1 AS min1
                   FROM aggregate_test_100
                   QUALIFY ROW_NUMBER() OVER my_window
                   WINDOW window1 AS (ORDER BY C12),
                   window2 AS (PARTITION BY C11)
                   ORDER BY C3
                  """;

        VerifiedOnlySelect(sql);


        sql = """
              SELECT
               MIN(c12) OVER window1 AS min1
               FROM aggregate_test_100
               WINDOW window1 AS (ORDER BY C12),
               window2 AS (PARTITION BY C11)
               QUALIFY ROW_NUMBER() OVER my_window
               ORDER BY C3
              """;

        VerifiedOnlySelect(sql);
    }

    [Fact]
    public void Parse_Aggregate_With_Group_By()
    {
        VerifiedOnlySelect("SELECT a, COUNT(1), MIN(b), MAX(b) FROM foo GROUP BY a");
    }

    [Fact]
    public void Parse_Literal_Decimal()
    {
        // These numbers were explicitly chosen to not roundtrip if represented as
        // f64s (i.e., as 64-bit binary floating point numbers).
        var select = VerifiedOnlySelect("SELECT 0.300000000000000004, 9007199254740993.0");
        Assert.Equal(2, select.Projection.Count);
        Assert.Equal(new LiteralValue(Number("0.300000000000000004")), select.Projection[0].AsExpr());
        Assert.Equal(new LiteralValue(Number("9007199254740993.0")), select.Projection[1].AsExpr());
    }

    [Fact]
    public void Parse_Literal_String()
    {
        var select = VerifiedOnlySelect("SELECT 'one', N'national string', X'deadBEEF'");
        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedString("one")), select.Projection[0].AsExpr());
        Assert.Equal(new LiteralValue(new Value.NationalStringLiteral("national string")),
            select.Projection[1].AsExpr());
        Assert.Equal(new LiteralValue(new Value.HexStringLiteral("deadBEEF")), select.Projection[2].AsExpr());

        OneStatementParsesTo("SELECT x'deadBEEF'", "SELECT X'deadBEEF'");
        OneStatementParsesTo("SELECT n'national string'", "SELECT N'national string'");
    }

    [Fact]
    public void Parse_Literal_Date()
    {
        var select = VerifiedOnlySelect("SELECT DATE '1999-01-01'");
        Assert.Equal(new TypedString("1999-01-01", new Date()), select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Literal_Time()
    {
        var select = VerifiedOnlySelect("SELECT TIME '01:23:34'");
        Assert.Equal(new TypedString("01:23:34", new Time(TimezoneInfo.None)), select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Literal_DateTime()
    {
        var select = VerifiedOnlySelect("SELECT DATETIME '1999-01-01 01:23:34.45'");
        Assert.Equal(new TypedString("1999-01-01 01:23:34.45", new Datetime()), select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Literal_Timestamp_Without_Time_Zone()
    {
        var select = VerifiedOnlySelect("SELECT TIMESTAMP '1999-01-01 01:23:34'");
        Assert.Equal(new TypedString("1999-01-01 01:23:34", new Timestamp(TimezoneInfo.None)),
            select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Literal_Timestamp_With_Time_Zone()
    {
        var select = VerifiedOnlySelect("SELECT TIMESTAMPTZ '1999-01-01 01:23:34Z'");
        Assert.Equal(new TypedString("1999-01-01 01:23:34Z", new Timestamp(TimezoneInfo.Tz)),
            select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Interval_All()
    {
        var select = VerifiedOnlySelect("SELECT INTERVAL '1-1' YEAR TO MONTH");
        var interval = new Expression.Interval(
            new LiteralValue(new Value.SingleQuotedString("1-1")),
            new DateTimeField.Year(),
            new DateTimeField.Month());
        Assert.Equal(interval, select.Projection[0].AsExpr());

        select = VerifiedOnlySelect("SELECT INTERVAL '01:01.01' MINUTE (5) TO SECOND (5)");
        interval = new Expression.Interval(
            new LiteralValue(new Value.SingleQuotedString("01:01.01")),
            new DateTimeField.Minute(),
            new DateTimeField.Second())
        {
            LeadingPrecision = 5,
            FractionalSecondsPrecision = 5
        };
        Assert.Equal(interval, select.Projection[0].AsExpr());

        select = VerifiedOnlySelect("SELECT INTERVAL '10' HOUR");
        interval = new Expression.Interval(
            new LiteralValue(new Value.SingleQuotedString("10")),
            new DateTimeField.Hour());
        Assert.Equal(interval, select.Projection[0].AsExpr());


        select = VerifiedOnlySelect("SELECT INTERVAL 5 DAY");
        interval = new Expression.Interval(
            new LiteralValue(Number("5")),
            new DateTimeField.Day());
        Assert.Equal(interval, select.Projection[0].AsExpr());

        select = VerifiedOnlySelect("SELECT INTERVAL '10' HOUR (1)");
        interval = new Expression.Interval(
            new LiteralValue(new Value.SingleQuotedString("10")),
            new DateTimeField.Hour())
        {
            LeadingPrecision = 1
        };
        Assert.Equal(interval, select.Projection[0].AsExpr());

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT INTERVAL '1' SECOND TO SECOND"));
        Assert.Equal("Expected end of statement, found SECOND, Line: 1, Col: 31", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT INTERVAL '10' HOUR (1) TO HOUR (2)"));
        Assert.Equal("Expected end of statement, found (, Line: 1, Col: 39", ex.Message);


        VerifiedOnlySelect("SELECT INTERVAL '1' YEAR");
        VerifiedOnlySelect("SELECT INTERVAL '1' MONTH");
        VerifiedOnlySelect("SELECT INTERVAL '1' DAY");
        VerifiedOnlySelect("SELECT INTERVAL '1' HOUR");
        VerifiedOnlySelect("SELECT INTERVAL '1' MINUTE");
        VerifiedOnlySelect("SELECT INTERVAL '1' SECOND");
        VerifiedOnlySelect("SELECT INTERVAL '1' YEAR TO MONTH");
        VerifiedOnlySelect("SELECT INTERVAL '1' DAY TO HOUR");
        VerifiedOnlySelect("SELECT INTERVAL '1' DAY TO MINUTE");
        VerifiedOnlySelect("SELECT INTERVAL '1' DAY TO SECOND");
        VerifiedOnlySelect("SELECT INTERVAL '1' HOUR TO MINUTE");
        VerifiedOnlySelect("SELECT INTERVAL '1' HOUR TO SECOND");
        VerifiedOnlySelect("SELECT INTERVAL '1' MINUTE TO SECOND");
    }

    [Fact]
    public void Parse_Interval_And_Or_Xor()
    {
        var dialects = AllDialects.Where(d => !d.RequireIntervalQualifier).ToList();
        const string sql = """
                           SELECT col FROM test
                           WHERE d3_date > d1_date + INTERVAL '5 days'
                           AND d2_date > d1_date + INTERVAL '3 days'
                           """;
        var statements = ParseSqlStatements(sql, dialects);

        var body = new SetExpression.SelectExpression(new Select(new[]
        {
            new SelectItem.UnnamedExpression(new Identifier("col"))
        })
        {
            Distinct = null,
            From = new TableWithJoins[]
            {
                new(new TableFactor.Table("test"))
            },
            Selection = new BinaryOp(
                new BinaryOp(
                    new Identifier("d3_date"),
                    BinaryOperator.Gt,
                    new BinaryOp(
                        new Identifier("d1_date"),
                        BinaryOperator.Plus,
                        new Expression.Interval(new LiteralValue(new Value.SingleQuotedString("5 days")))
                    )),
                BinaryOperator.And,
                new BinaryOp(
                    new Identifier("d2_date"),
                    BinaryOperator.Gt,
                    new BinaryOp(
                        new Identifier("d1_date"),
                        BinaryOperator.Plus,
                        new Expression.Interval(new LiteralValue(new Value.SingleQuotedString("3 days")))
                    )
                )
            )
        });
        var expected = new[]
        {
            new Statement.Select(new Query(body))
        };

        Assert.Equal(expected, statements);

        VerifiedStatement(
            "SELECT col FROM test WHERE d3_date > d1_date + INTERVAL '5 days' AND d2_date > d1_date + INTERVAL '3 days'",
            dialects);
        VerifiedStatement(
            "SELECT col FROM test WHERE d3_date > d1_date + INTERVAL '5 days' OR d2_date > d1_date + INTERVAL '3 days'",
            dialects);
        VerifiedStatement(
            "SELECT col FROM test WHERE d3_date > d1_date + INTERVAL '5 days' XOR d2_date > d1_date + INTERVAL '3 days'",
            dialects);
    }

    [Fact]
    public void Parse_At_Timezone()
    {
        var zero = new LiteralValue(Number("0"));
        var select = VerifiedOnlySelect("SELECT FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00' FROM t");

        var expected = new AtTimeZone(new Function("FROM_UNIXTIME")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(zero))
            ]))
        }, new LiteralValue(new Value.SingleQuotedString("UTC-06:00")));

        Assert.Equal(expected, select.Projection.Single().AsExpr());


        select = VerifiedOnlySelect(
            "SELECT DATE_FORMAT(FROM_UNIXTIME(0) AT TIME ZONE 'UTC-06:00', '%Y-%m-%dT%H') AS \"hour\" FROM t");
        var expr = new SelectItem.ExpressionWithAlias(new Function("DATE_FORMAT")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                    new AtTimeZone(new Function("FROM_UNIXTIME")
                    {
                        Args = new FunctionArguments.List(new FunctionArgumentList([
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(zero))
                        ]))
                    }, new LiteralValue(new Value.SingleQuotedString("UTC-06:00"))))
                ),
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(
                        new LiteralValue(new Value.SingleQuotedString("%Y-%m-%dT%H"))))
            ]))
        }, new Ident("hour", Symbols.DoubleQuote));

        Assert.Equal(expr, select.Projection.Single());
    }

    [Fact]
    public void Parse_Json()
    {
        const string sql = """
                           SELECT JSON '{
                             "id": 10,
                             "type": "fruit",
                             "name": "apple",
                             "on_menu": true,
                             "recipes":
                               {
                                 "salads":
                                 [
                                   { "id": 2001, "type": "Walnut Apple Salad" },
                                   { "id": 2002, "type": "Apple Spinach Salad" }
                                 ],
                                 "desserts":
                                 [
                                   { "id": 3001, "type": "Apple Pie" },
                                   { "id": 3002, "type": "Apple Scones" },
                                   { "id": 3003, "type": "Apple Crumble" }
                                 ]
                               }
                           }'
                           """;
        const string json = """
                            {
                              "id": 10,
                              "type": "fruit",
                              "name": "apple",
                              "on_menu": true,
                              "recipes":
                                {
                                  "salads":
                                  [
                                    { "id": 2001, "type": "Walnut Apple Salad" },
                                    { "id": 2002, "type": "Apple Spinach Salad" }
                                  ],
                                  "desserts":
                                  [
                                    { "id": 3001, "type": "Apple Pie" },
                                    { "id": 3002, "type": "Apple Scones" },
                                    { "id": 3003, "type": "Apple Crumble" }
                                  ]
                                }
                            }
                            """;

        var select = VerifiedOnlySelect(sql, preserveFormatting: true);

        var expected = new TypedString(json, new Json());
        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_BigNumeric_Keyword()
    {
        var select = VerifiedOnlySelect("SELECT BIGNUMERIC '0'");
        var expected = new TypedString("0", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT BIGNUMERIC '123456'");
        expected = new TypedString("123456", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT BIGNUMERIC '-3.14'");
        expected = new TypedString("-3.14", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT BIGNUMERIC '-0.54321'");
        expected = new TypedString("-0.54321", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT BIGNUMERIC '1.23456e05'");
        expected = new TypedString("1.23456e05", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());

        select = VerifiedOnlySelect("SELECT BIGNUMERIC '-9.876e-3'");
        expected = new TypedString("-9.876e-3", new BigNumeric(new ExactNumberInfo.None()));
        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Simple_Math_Expr_Plus()
    {
        VerifiedOnlySelect("SELECT a + b, 2 + a, 2.5 + a, a_f + b_f, 2 + a_f, 2.5 + a_f FROM c");
    }

    [Fact]
    public void Parse_Simple_Math_Expr_Minus()
    {
        VerifiedOnlySelect("SELECT a - b, 2 - a, 2.5 - a, a_f - b_f, 2 - a_f, 2.5 - a_f FROM c");
    }

    [Fact]
    public void Parse_Table_Function()
    {
        var select = VerifiedOnlySelect("SELECT * FROM TABLE(FUN('1')) AS a");

        var expected = new Function("FUN")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("1"))))
            ]))
        };

        var actual = (TableFactor.TableFunction)select.From!.Single().Relation!;
        Assert.Equal(expected, actual.Expression);

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM TABLE '1' AS a"));
        Assert.Equal("Expected (, found '1', Line: 1, Col: 21", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM TABLE (FUN(a) AS a"));
        Assert.Equal("Expected ), found AS, Line: 1, Col: 29", ex.Message);
    }

    [Fact]
    public void Parse_Unnest()
    {
        var dialects = new Dialect[]
        {
            new BigQueryDialect(),
            new GenericDialect()
        };

        // 1. both Alias and WITH OFFSET clauses.
        Test(true, true, false,
        [
            new TableWithJoins(new TableFactor.UnNest([new Identifier("expr")])
            {
                Alias = new TableAlias("numbers"),
                WithOffset = true
            })
        ]);

        // 2. neither Alias nor WITH OFFSET clause.
        Test(false, false, false,
        [
            new TableWithJoins(new TableFactor.UnNest([new Identifier("expr")]))
        ]);

        // 3. Alias but no WITH OFFSET clause.
        Test(false, true, false,
        [
            new TableWithJoins(new TableFactor.UnNest([new Identifier("expr")])
            {
                WithOffset = true
            })
        ]);

        // 4. WITH OFFSET but no Alias.
        Test(true, false, false,
        [
            new TableWithJoins(new TableFactor.UnNest([new Identifier("expr")])
            {
                Alias = new TableAlias("numbers"),
            })
        ]);
        return;

        void Test(bool alias, bool withOffset, bool withOffsetAlias, IEnumerable<TableWithJoins> expected)
        {
            var aliasQuery = string.Empty;
            var withOffsetQuery = string.Empty;
            var withOffsetAliasQuery = string.Empty;
            if (alias)
            {
                aliasQuery = " AS numbers";
            }

            if (withOffset)
            {
                withOffsetQuery = " WITH OFFSET";
            }

            if (withOffsetAlias)
            {
                withOffsetAliasQuery = " AS with_offset_alias";
            }

            var sql = $"SELECT * FROM UNNEST(expr){aliasQuery}{withOffsetQuery}{withOffsetAliasQuery}";

            var select = VerifiedOnlySelect(sql, dialects);

            Assert.Equal(expected, select.From!);
        }
    }

    [Fact]
    public void Parse_Parens()
    {
        var expression = VerifiedExpr("(a + b) - (c + d)");

        var expected = new BinaryOp(
            new Expression.Nested(new BinaryOp(
                new Identifier("a"),
                BinaryOperator.Plus,
                new Identifier("b")
            )),
            BinaryOperator.Minus,
            new Expression.Nested(new BinaryOp(
                new Identifier("c"),
                BinaryOperator.Plus,
                new Identifier("d")
            ))
        );
        Assert.Equal(expected, expression);
    }

    [Fact]
    public void Parse_Searched_Case_Expr()
    {
        var select =
            VerifiedOnlySelect(
                "SELECT CASE WHEN bar IS NULL THEN 'null' WHEN bar = 0 THEN '=0' WHEN bar >= 0 THEN '>=0' ELSE '<0' END FROM foo");

        var conditions = new Expression[]
        {
            new IsNull(new Identifier("bar")),
            new BinaryOp(
                new Identifier("bar"),
                BinaryOperator.Eq,
                new LiteralValue(Number("0"))
            ),
            new BinaryOp(
                new Identifier("bar"),
                BinaryOperator.GtEq,
                new LiteralValue(Number("0"))
            )
        };
        var results = new[]
        {
            new LiteralValue(new Value.SingleQuotedString("null")),
            new LiteralValue(new Value.SingleQuotedString("=0")),
            new LiteralValue(new Value.SingleQuotedString(">=0"))
        };

        var expected = new Case(conditions, results)
        {
            ElseResult = new LiteralValue(new Value.SingleQuotedString("<0"))
        };

        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Simple_Case_Expr()
    {
        // ANSI calls a CASE expression with an operand "<simple case>"
        var select = VerifiedOnlySelect("SELECT CASE foo WHEN 1 THEN 'Y' ELSE 'N' END");

        var expected = new Case(
            new[] { new LiteralValue(Number("1")) },
            new[] { new LiteralValue(new Value.SingleQuotedString("Y")) }
        )
        {
            Operand = new Identifier("foo"),
            ElseResult = new LiteralValue(new Value.SingleQuotedString("N"))
        };

        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_From_Advanced()
    {
        VerifiedOnlySelect("SELECT * FROM fn(1, 2) AS foo, schema.bar AS bar WITH (NOLOCK)");
    }

    [Fact]
    // ReSharper disable once IdentifierTypo
    public void Parse_Nullary_Table_Valued_Function()
    {
        VerifiedOnlySelect("SELECT * FROM fn()");
    }

    [Fact]
    public void Parse_Implicit_Join()
    {
        var select = VerifiedOnlySelect("SELECT * FROM t1, t2");
        var expected = new TableWithJoins[]
        {
            new(new TableFactor.Table("t1")),
            new(new TableFactor.Table("t2"))
        };
        Assert.Equal(expected, select.From!);


        select = VerifiedOnlySelect("SELECT * FROM t1a NATURAL JOIN t1b, t2a NATURAL JOIN t2b");
        expected =
        [
            new TableWithJoins(new TableFactor.Table("t1a"))
            {
                Joins = new Join[]
                {
                    new()
                    {
                        Relation = new TableFactor.Table("t1b"),
                        JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                    }
                }
            },
            new TableWithJoins(new TableFactor.Table("t2a"))
            {
                Joins = new Join[]
                {
                    new()
                    {
                        Relation = new TableFactor.Table("t2b"),
                        JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                    }
                }
            }
        ];
        Assert.Equal(expected, select.From!);
    }

    [Fact]
    public void Parse_Cross_Join()
    {
        var select = VerifiedOnlySelect("SELECT * FROM t1 CROSS JOIN t2");
        var expected = new Join(new TableFactor.Table("t2"), new JoinOperator.CrossJoin());
        Assert.Equal(expected, select.From!.Single().Joins!.Single());
    }

    [Fact]
    public void Parse_Joins_On()
    {
        //var select = VerifiedOnlySelect("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2");
        //var expected = Test("t2", null, jc => new JoinOperator.LeftOuter(jc));
        //Assert.Equal(expected, select.From!.Single().Joins!.Single());

        //// Test parsing of aliases
        //expected = Test("t2", new TableAlias("foo"), jc => new JoinOperator.Inner(jc));
        //var actual = VerifiedOnlySelect("SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //OneStatementParsesTo(
        //    "SELECT * FROM t1 JOIN t2 foo ON c1 = c2",
        //    "SELECT * FROM t1 JOIN t2 AS foo ON c1 = c2"
        //);

        //// Test parsing of different join operators
        //expected = Test("t2", null, jc => new JoinOperator.Inner(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.LeftOuter(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.RightOuter(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.LeftSemi(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT SEMI JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.RightSemi(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT SEMI JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.LeftAnti(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT ANTI JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        //expected = Test("t2", null, jc => new JoinOperator.RightAnti(jc));
        //actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT ANTI JOIN t2 ON c1 = c2").From!.Single().Joins;
        //Assert.Equal(new[] { expected }, actual!);

        var expected = Test("t2", null, jc => new JoinOperator.FullOuter(jc), true);
        var actual = VerifiedOnlySelect("SELECT * FROM t1 GLOBAL FULL JOIN t2 ON c1 = c2").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);
        return;

        static Join Test(string relation, TableAlias? alias, Func<JoinConstraint, JoinOperator> fn, bool global = false)
        {
            var joinOperator = fn(new JoinConstraint.On(new BinaryOp(
                new Identifier("c1"),
                BinaryOperator.Eq,
                new Identifier("c2")
            )));

            return new Join(new TableFactor.Table(relation) { Alias = alias }, joinOperator, global);
        }
    }

    [Fact]
    public void Parse_Joins_Using()
    {
        // Test parsing of aliases
        var expected = Test("t2", new TableAlias("foo"), jc => new JoinOperator.Inner(jc));
        var actual = VerifiedOnlySelect("SELECT * FROM t1 JOIN t2 AS foo USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        OneStatementParsesTo(
            "SELECT * FROM t1 JOIN t2 foo USING(c1)",
            "SELECT * FROM t1 JOIN t2 AS foo USING(c1)"
        );

        // Test parsing of different join operators
        expected = Test("t2", null, jc => new JoinOperator.Inner(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.LeftOuter(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.RightOuter(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.LeftSemi(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT SEMI JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.RightSemi(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT SEMI JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.LeftAnti(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 LEFT ANTI JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.RightAnti(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 RIGHT ANTI JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        expected = Test("t2", null, jc => new JoinOperator.FullOuter(jc));
        actual = VerifiedOnlySelect("SELECT * FROM t1 FULL JOIN t2 USING(c1)").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);
        return;

        static Join Test(string relation, TableAlias? alias, Func<JoinConstraint, JoinOperator> fn)
        {
            var joinOperator = fn(new JoinConstraint.Using(new Ident[] { "c1" }));

            return new Join(new TableFactor.Table(relation) { Alias = alias }, joinOperator);
        }
    }

    [Fact]
    public void Parse_Natural_Join()
    {
        // if not specified, inner join as default
        var expected = Test(jc => new JoinOperator.Inner(jc), null);
        var actual = VerifiedOnlySelect("SELECT * FROM t1 NATURAL JOIN t2").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        // left join explicitly
        expected = Test(jc => new JoinOperator.LeftOuter(jc), null);
        actual = VerifiedOnlySelect("SELECT * FROM t1 NATURAL LEFT JOIN t2").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        // right join explicitly
        expected = Test(jc => new JoinOperator.RightOuter(jc), null);
        actual = VerifiedOnlySelect("SELECT * FROM t1 NATURAL RIGHT JOIN t2").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        // full join explicitly
        expected = Test(jc => new JoinOperator.FullOuter(jc), null);
        actual = VerifiedOnlySelect("SELECT * FROM t1 NATURAL FULL JOIN t2").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);


        // natural join another table with alias
        expected = Test(jc => new JoinOperator.Inner(jc), new TableAlias("t3"));
        actual = VerifiedOnlySelect("SELECT * FROM t1 NATURAL JOIN t2 AS t3").From!.Single().Joins;
        Assert.Equal(new[] { expected }, actual!);

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM t1 natural"));
        Assert.Equal("Expected a join type after NATURAL, found EOF", ex.Message);

        return;

        static Join Test(Func<JoinConstraint, JoinOperator> fn, TableAlias? alias)
        {
            return new Join
            {
                Relation = new TableFactor.Table("t2")
                {
                    Alias = alias
                },
                JoinOperator = fn(new JoinConstraint.Natural())
            };
        }
    }

    [Fact]
    public void Parse_Complex_Join()
    {
        VerifiedOnlySelect(
            "SELECT c1, c2 FROM t1, t4 JOIN t2 ON t2.c = t1.c LEFT JOIN t3 USING(q, c) WHERE t4.c = t1.c");
    }

    [Fact]
    public void Parse_Join_Nesting()
    {
        const string sql =
            "SELECT * FROM a NATURAL JOIN (b NATURAL JOIN (c NATURAL JOIN d NATURAL JOIN e)) NATURAL JOIN (f NATURAL JOIN (g NATURAL JOIN h))";
        var actual = VerifiedOnlySelect(sql).From!.Single().Joins;
        var expected = new Join[]
        {
            new()
            {
                Relation = new TableFactor.NestedJoin
                {
                    TableWithJoins = new TableWithJoins(new TableFactor.Table("b"))
                    {
                        Joins = new Join[]
                        {
                            new()
                            {
                                Relation = new TableFactor.NestedJoin
                                {
                                    TableWithJoins = new TableWithJoins(new TableFactor.Table("c"))
                                    {
                                        Joins = new Join[]
                                        {
                                            new(new TableFactor.Table("d"))
                                            {
                                                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                                            },
                                            new(new TableFactor.Table("e"))
                                            {
                                                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                                            }
                                        }
                                    }
                                },
                                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                            }
                        }
                    }
                },
                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
            },

            new()
            {
                Relation = new TableFactor.NestedJoin
                {
                    TableWithJoins = new TableWithJoins(new TableFactor.Table("f"))
                    {
                        Joins = new Join[]
                        {
                            new()
                            {
                                Relation = new TableFactor.NestedJoin
                                {
                                    TableWithJoins = new TableWithJoins(new TableFactor.Table("g"))
                                    {
                                        Joins = new Join[]
                                        {
                                            new(new TableFactor.Table("h"))
                                            {
                                                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                                            }
                                        }
                                    }
                                },
                                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                            }
                        }
                    }
                },
                JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
            }
        };

        Assert.Equal(expected, actual!);
    }

    [Fact]
    public void Parse_Join_Syntax_Variants()
    {
        OneStatementParsesTo(
            "SELECT c1 FROM t1 INNER JOIN t2 USING(c1)",
            "SELECT c1 FROM t1 JOIN t2 USING(c1)"
        );
        OneStatementParsesTo(
            "SELECT c1 FROM t1 LEFT OUTER JOIN t2 USING(c1)",
            "SELECT c1 FROM t1 LEFT JOIN t2 USING(c1)"
        );
        OneStatementParsesTo(
            "SELECT c1 FROM t1 RIGHT OUTER JOIN t2 USING(c1)",
            "SELECT c1 FROM t1 RIGHT JOIN t2 USING(c1)"
        );
        OneStatementParsesTo(
            "SELECT c1 FROM t1 FULL OUTER JOIN t2 USING(c1)",
            "SELECT c1 FROM t1 FULL JOIN t2 USING(c1)"
        );

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM a OUTER JOIN b ON 1"));
        Assert.Equal("Expected APPLY, found JOIN, Line: 1, Col: 23", ex.Message);
    }

    [Fact]
    // ReSharper disable once IdentifierTypo
    public void Parse_Ctes()
    {
        // Top-level CTE
        var cteSql = new[] { "SELECT 1 AS foo", "SELECT 2 AS bar" };
        var with = $"WITH a AS ({cteSql[0]}), b AS ({cteSql[1]}) SELECT foo + bar FROM a, b";
        Test(cteSql, VerifiedQuery(with));

        // CTE in a subquery
        var sql = $"SELECT ({with})";
        var select = VerifiedOnlySelect(sql);
        var proj = (SelectItem.UnnamedExpression)select.Projection.Single();
        var subQuery = (Subquery)proj.AsExpr();
        Test(cteSql, subQuery.Query);

        // CTE in a derived table
        sql = $"SELECT * FROM ({with})";
        select = VerifiedOnlySelect(sql);
        var derived = (TableFactor.Derived)select.From!.Single().Relation!;
        Test(cteSql, derived.SubQuery);

        // CTE in a view
        sql = $"CREATE VIEW v AS {with}";
        var view = (Statement.CreateView)VerifiedStatement(sql);
        Test(cteSql, view.Query);

        // CTE in a CTE...
        sql = $"WITH outer_cte AS ({with}) SELECT * FROM outer_cte";
        var query = VerifiedQuery(sql);
        Test(cteSql, query.With!.CteTables.Single().Query);
        return;

        static void Test(IReadOnlyList<string> expected, Query q)
        {
            for (var i = 0; i < expected.Count; i++)
            {
                var actual = q.With!.CteTables[i];
                Assert.Equal(expected[i], actual.Query.ToSql());
                Assert.Equal(i == 0 ? "a" : "b", actual.Alias.Name);
                Assert.Null(actual.Alias.Columns);
            }
        }
    }

    [Fact]
    public void Parse_Cte_Renamed_Column()
    {
        var query = VerifiedQuery("WITH cte (col1, col2) AS (SELECT foo, bar FROM baz) SELECT * FROM cte");
        Assert.Equal(new Ident[] { "col1", "col2" }, query.With!.CteTables.First().Alias.Columns!);
    }

    [Fact]
    public void Parse_Recursive_Cte()
    {
        const string cteSql = "SELECT 1 UNION ALL SELECT val + 1 FROM nums WHERE val < 10";
        var sql = $"WITH RECURSIVE nums (val) AS ({cteSql}) SELECT * FROM nums";
        var cteQuery = VerifiedQuery(cteSql);
        var query = VerifiedQuery(sql);

        var expected = new CommonTableExpression(new TableAlias("nums")
        {
            Columns = new Ident[]
            {
                "val"
            }
        }, cteQuery);

        Assert.True(query.With!.Recursive);
        Assert.Single(query.With.CteTables);
        Assert.Equal(expected, query.With.CteTables.First());
    }

    [Fact]
    public void Parse_Derived_Tables()
    {
        VerifiedOnlySelect("SELECT a.x, b.y FROM (SELECT x FROM foo) AS a CROSS JOIN (SELECT y FROM bar) AS b");

        VerifiedOnlySelect("SELECT a.x, b.y FROM (SELECT x FROM foo) AS a (x) CROSS JOIN (SELECT y FROM bar) AS b (y)");

        VerifiedOnlySelect("SELECT * FROM (((SELECT 1)))");

        var select = VerifiedOnlySelect("SELECT * FROM (((SELECT 1) UNION (SELECT 2)) AS t1 NATURAL JOIN t2)");
        var expected = new TableFactor.NestedJoin
        {
            TableWithJoins = new TableWithJoins(new TableFactor.Derived(VerifiedQuery("(SELECT 1) UNION (SELECT 2)"))
            {
                Alias = new TableAlias("t1")
            })
            {
                Joins = new Join[]
                {
                    new(new TableFactor.Table("t2"))
                    {
                        JoinOperator = new JoinOperator.Inner(new JoinConstraint.Natural())
                    }
                }
            }
        };
        Assert.Equal(expected, select.From!.Single().Relation);
    }

    [Fact]
    public void Parse_Union_Except_Intersect()
    {
        VerifiedStatement("SELECT 1 UNION SELECT 2");
        VerifiedStatement("SELECT 1 UNION ALL SELECT 2");
        VerifiedStatement("SELECT 1 UNION DISTINCT SELECT 1");
        VerifiedStatement("SELECT 1 EXCEPT SELECT 2");
        VerifiedStatement("SELECT 1 EXCEPT ALL SELECT 2");
        VerifiedStatement("SELECT 1 EXCEPT DISTINCT SELECT 1");
        VerifiedStatement("SELECT 1 INTERSECT SELECT 2");
        VerifiedStatement("SELECT 1 INTERSECT ALL SELECT 2");
        VerifiedStatement("SELECT 1 INTERSECT DISTINCT SELECT 1");
        VerifiedStatement("SELECT 1 UNION SELECT 2 UNION SELECT 3");
        VerifiedStatement("SELECT 1 EXCEPT SELECT 2 UNION SELECT 3");
        VerifiedStatement("SELECT 1 INTERSECT (SELECT 2 EXCEPT SELECT 3)");
        VerifiedStatement("WITH cte AS (SELECT 1 AS foo) (SELECT foo FROM cte ORDER BY 1 LIMIT 1)");
        VerifiedStatement("SELECT 1 UNION (SELECT 2 ORDER BY 1 LIMIT 1)");
        VerifiedStatement("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3");
        VerifiedStatement("SELECT foo FROM tab UNION SELECT bar FROM TAB");
        VerifiedStatement(
            "(SELECT * FROM new EXCEPT SELECT * FROM old) UNION ALL (SELECT * FROM old EXCEPT SELECT * FROM new) ORDER BY 1");
        VerifiedStatement(
            "(SELECT * FROM new EXCEPT DISTINCT SELECT * FROM old) UNION DISTINCT (SELECT * FROM old EXCEPT DISTINCT SELECT * FROM new) ORDER BY 1");
        VerifiedStatement("SELECT 1 AS x, 2 AS y EXCEPT BY NAME SELECT 9 AS y, 8 AS x");
        VerifiedStatement("SELECT 1 AS x, 2 AS y EXCEPT ALL BY NAME SELECT 9 AS y, 8 AS x");
        VerifiedStatement("SELECT 1 AS x, 2 AS y EXCEPT DISTINCT BY NAME SELECT 9 AS y, 8 AS x");
        VerifiedStatement("SELECT 1 AS x, 2 AS y INTERSECT BY NAME SELECT 9 AS y, 8 AS x");
        VerifiedStatement("SELECT 1 AS x, 2 AS y INTERSECT ALL BY NAME SELECT 9 AS y, 8 AS x");
        VerifiedStatement("SELECT 1 AS x, 2 AS y INTERSECT DISTINCT BY NAME SELECT 9 AS y, 8 AS x");
    }

    [Fact]
    public void Parse_Values()
    {
        VerifiedStatement("SELECT * FROM (VALUES (1), (2), (3))");
        VerifiedStatement("SELECT * FROM (VALUES (1), (2), (3)), (VALUES (1, 2, 3))");
        VerifiedStatement("SELECT * FROM (VALUES (1)) UNION VALUES (1)");
        VerifiedStatement("SELECT * FROM (VALUES ROW(1, true, 'a'), ROW(2, false, 'b')) AS t (a, b, c)");
    }

    [Fact]
    public void Parse_Multiple_Statements()
    {
        Test("SELECT foo", "SELECT", " bar");
        //// ensure that SELECT/WITH is not parsed as a table or column alias if ';'
        //// separating the statements is omitted:
        Test("SELECT foo FROM baz", "SELECT", " bar");
        Test("SELECT foo", "WITH", " cte AS (SELECT 1 AS s) SELECT bar");
        Test("SELECT foo FROM baz", "WITH", " cte AS (SELECT 1 AS s) SELECT bar");
        Test("DELETE FROM foo", "SELECT", " bar");
        Test("INSERT INTO foo VALUES (1)", "SELECT", " bar");
        Test("CREATE TABLE foo (baz INT)", "SELECT", " bar");
        // Make sure that empty statements do not cause an error:
        Assert.Empty(ParseSqlStatements(";;"));
        return;

        void Test(string sql1, string sql2Kw, string sql2Rest)
        {
            var rest = $"{sql2Kw}{sql2Rest}";

            // Check that a string consisting of two statements delimited by a semicolon
            var res = ParseSqlStatements($"{sql1};{rest}");

            var first = OneStatementParsesTo(sql1, "");
            var second = OneStatementParsesTo($"{rest}", "");

            Assert.Equal(new[] { first, second }, res);

            // Check that extra semicolon at the end is stripped by normalization:
            OneStatementParsesTo($"{sql1};", sql1);

            Assert.Throws<ParserException>(() => ParseSqlStatements($"{sql1} {rest}"));
        }
    }

    [Fact]
    public void Parse_Scalar_SubQueries()
    {
        var expr = VerifiedExpr("(SELECT 1) + (SELECT 2)");
        Assert.True(expr is BinaryOp { Op: BinaryOperator.Plus });
    }

    [Fact]
    public void Parse_Substring()
    {
        var supported = AllDialects.Where(d => d is not MsSqlDialect).ToList();
        var msSql = new[] { new MsSqlDialect() };

        DefaultDialects = supported;
        OneStatementParsesTo("SELECT SUBSTRING('1')", "SELECT SUBSTRING('1')", supported);

        OneStatementParsesTo("SELECT SUBSTRING('1' FROM 1)", "SELECT SUBSTRING('1' FROM 1)", supported);

        OneStatementParsesTo("SELECT SUBSTRING('1' FROM 1 FOR 3)", "SELECT SUBSTRING('1' FROM 1 FOR 3)", supported);

        OneStatementParsesTo("SELECT SUBSTRING('1' FOR 3)", "SELECT SUBSTRING('1' FOR 3)", supported);

        DefaultDialects = msSql;
        OneStatementParsesTo("SELECT SUBSTRING('1', 1, 3)", "SELECT SUBSTRING('1', 1, 3)", msSql);
    }

    [Fact]
    public void Parse_Overlay()
    {
        OneStatementParsesTo(
            "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3)",
            "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3)"
        );
        OneStatementParsesTo(
            "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3 FOR 12)",
            "SELECT OVERLAY('abccccde' PLACING 'abc' FROM 3 FOR 12)"
        );

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT OVERLAY('abccccde' FROM 3)"));
        Assert.Equal("Expected PLACING, found FROM, Line: 1, Col: 27", ex.Message);

        var select = VerifiedOnlySelect("SELECT OVERLAY('abcdef' PLACING name FROM 3 FOR id + 1) FROM CUSTOMERS");

        var expected = new Overlay(
            new LiteralValue(new Value.SingleQuotedString("abcdef")),
            new Identifier("name"),
            new LiteralValue(Number("3")),
            new BinaryOp(
                new Identifier("id"),
                BinaryOperator.Plus,
                new LiteralValue(Number("1"))
            )
        );

        Assert.Equal(expected, select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Trim()
    {
        OneStatementParsesTo(
            "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')",
            "SELECT TRIM(BOTH 'xyz' FROM 'xyzfooxyz')"
        );

        OneStatementParsesTo(
            "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')",
            "SELECT TRIM(LEADING 'xyz' FROM 'xyzfooxyz')");

        OneStatementParsesTo(
            "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')",
            "SELECT TRIM(TRAILING 'xyz' FROM 'xyzfooxyz')");

        OneStatementParsesTo(
            "SELECT TRIM('xyz' FROM 'xyzfooxyz')",
            "SELECT TRIM('xyz' FROM 'xyzfooxyz')");

        OneStatementParsesTo(
            "SELECT TRIM('   foo   ')",
            "SELECT TRIM('   foo   ')");

        OneStatementParsesTo(
            "SELECT TRIM(LEADING '   foo   ')",
            "SELECT TRIM(LEADING '   foo   ')");

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT TRIM(FOO 'xyz' FROM 'xyzfooxyz')"));
        Assert.Equal("Expected ), found 'xyz', Line: 1, Col: 17", ex.Message);
    }

    [Fact]
    public void Parse_Exists_Subquery()
    {
        var select = VerifiedOnlySelect("SELECT * FROM t WHERE EXISTS (SELECT 1)");
        Assert.Equal(new Exists(VerifiedQuery("SELECT 1")), select.Selection);

        select = VerifiedOnlySelect("SELECT * FROM t WHERE NOT EXISTS (SELECT 1)");
        Assert.Equal(new Exists(VerifiedQuery("SELECT 1"), true), select.Selection);

        VerifiedStatement("SELECT * FROM t WHERE EXISTS (WITH u AS (SELECT 1) SELECT * FROM u)");
        VerifiedStatement("SELECT EXISTS (SELECT 1)");

        var dialects = AllDialects.Where(d => d is not DatabricksDialect);

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT EXISTS (", dialects));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found EOF", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT EXISTS (NULL)", dialects));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found NULL, Line: 1, Col: 16",
            ex.Message);
    }

    [Fact]
    public void Parse_Create_Database()
    {
        var create = VerifiedStatement<Statement.CreateDatabase>("CREATE DATABASE mydb");

        Assert.Equal("mydb", create.Name);
        Assert.False(create.IfNotExists);
        Assert.Null(create.Location);
        Assert.Null(create.ManagedLocation);
    }

    [Fact]
    public void Parse_Create_Database_If_Not_Exists()
    {
        var create = VerifiedStatement<Statement.CreateDatabase>("CREATE DATABASE IF NOT EXISTS mydb");

        Assert.Equal("mydb", create.Name);
        Assert.True(create.IfNotExists);
        Assert.Null(create.Location);
        Assert.Null(create.ManagedLocation);
    }

    [Fact]
    public void Parse_Create_View()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE VIEW myschema.myview AS SELECT foo FROM bar");

        Assert.Equal("myschema.myview", create.Name);
        Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
        Assert.False(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.False(create.WithNoSchemaBinding);
        Assert.False(create.IfNotExists);
        Assert.False(create.Temporary);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_View_With_Columns()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE VIEW v (has, cols) AS SELECT 1, 2");

        Assert.Equal("v", create.Name);
        Assert.Equal([new("has"), new("cols")], create.Columns);
        Assert.Equal("SELECT 1, 2", create.Query.Query.ToSql());
        Assert.False(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.False(create.WithNoSchemaBinding);
        Assert.False(create.IfNotExists);
        Assert.False(create.Temporary);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_View_With_Options()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE VIEW v WITH (foo = 'bar', a = 123) AS SELECT 1");
        var expected = new CreateTableOptions.With([
            new SqlOption.KeyValue("foo", new LiteralValue(new Value.SingleQuotedString("bar"))),
            new SqlOption.KeyValue("a", new LiteralValue(Number("123")))
        ]);
        Assert.Equal(expected, create.Options);
    }

    [Fact]
    public void Parse_Create_Or_Replace_View()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE OR REPLACE VIEW v AS SELECT 1");
        Assert.Equal("v", create.Name);
        Assert.False(create.Materialized);
        Assert.True(create.OrReplace);
        Assert.Equal("SELECT 1", create.Query.Query.ToSql());
        Assert.False(create.WithNoSchemaBinding);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_View_Temporary()
    {
        var create =
            VerifiedStatement<Statement.CreateView>("CREATE TEMPORARY VIEW myschema.myview AS SELECT foo FROM bar");

        Assert.Equal("myschema.myview", create.Name);
        Assert.Null(create.Columns);
        Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
        Assert.False(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.False(create.WithNoSchemaBinding);
        Assert.False(create.IfNotExists);
        Assert.True(create.Temporary);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_Or_Replace_Materialized_View()
    {
        var create = VerifiedStatement<Statement.CreateView>("CREATE OR REPLACE MATERIALIZED VIEW v AS SELECT 1");
        Assert.Equal("v", create.Name);
        Assert.True(create.Materialized);
        Assert.True(create.OrReplace);
        Assert.Equal("SELECT 1", create.Query.Query.ToSql());
        Assert.False(create.WithNoSchemaBinding);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_Materialized_View()
    {
        var create =
            VerifiedStatement<Statement.CreateView>("CREATE MATERIALIZED VIEW myschema.myview AS SELECT foo FROM bar");
        Assert.Equal("myschema.myview", create.Name);
        Assert.True(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
        Assert.False(create.WithNoSchemaBinding);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Create_Materialized_View_With_Cluster_By()
    {
        var create =
            VerifiedStatement<Statement.CreateView>(
                "CREATE MATERIALIZED VIEW myschema.myview CLUSTER BY (foo) AS SELECT foo FROM bar");
        Assert.Equal("myschema.myview", create.Name);
        Assert.True(create.Materialized);
        Assert.False(create.OrReplace);
        Assert.Equal(new Ident[] { "foo" }, create.ClusterBy!);
        Assert.Equal("SELECT foo FROM bar", create.Query.Query.ToSql());
        Assert.False(create.WithNoSchemaBinding);
        Assert.Null(create.Comment);
    }

    [Fact]
    public void Parse_Drop_Table()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP TABLE foo");

        Assert.False(drop.IfExists);
        Assert.False(drop.Cascade);
        Assert.Equal(ObjectType.Table, drop.ObjectType);
        Assert.Equal(new ObjectName[] { new(["foo"]) }, drop.Names);
    }

    [Fact]
    public void Parse_Drop_View()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP VIEW myschema.myview");

        Assert.Equal(ObjectType.View, drop.ObjectType);
        Assert.Equal(new ObjectName[] { new(["myschema", "myview"]) }, drop.Names);
    }

    [Fact]
    public void Parse_Invalid_SubQuery_Without_Parens()
    {
        var ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT SELECT 1 FROM bar WHERE 1=1 FROM baz"));
        Assert.Equal("Expected end of statement, found 1, Line: 1, Col: 15", ex.Message);
    }

    [Fact]
    public void Parse_Offset()
    {
        var expected = new Offset(new LiteralValue(Number("2")), OffsetRows.Rows);

        var query = VerifiedQuery("SELECT foo FROM bar OFFSET 2 ROWS");
        Assert.Equal(expected, query.Offset);

        query = VerifiedQuery("SELECT foo FROM bar WHERE foo = 4 OFFSET 2 ROWS");
        Assert.Equal(expected, query.Offset);

        query = VerifiedQuery("SELECT foo FROM bar ORDER BY baz OFFSET 2 ROWS");
        Assert.Equal(expected, query.Offset);

        query = VerifiedQuery("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS");
        Assert.Equal(expected, query.Offset);

        query = VerifiedQuery("SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS) OFFSET 2 ROWS");
        Assert.Equal(expected, query.Offset);

        var body = (SetExpression.SelectExpression)query.Body;
        var subQuery = (TableFactor.Derived)body.Select.From!.Single().Relation!;
        Assert.Equal(expected, subQuery.SubQuery.Offset);

        expected = new Offset(new LiteralValue(Number("0")), OffsetRows.Rows);
        query = VerifiedQuery("SELECT 'foo' OFFSET 0 ROWS");
        Assert.Equal(expected, query.Offset);

        expected = new Offset(new LiteralValue(Number("1")), OffsetRows.Row);
        query = VerifiedQuery("SELECT 'foo' OFFSET 1 ROW");
        Assert.Equal(expected, query.Offset);

        expected = new Offset(new LiteralValue(Number("1")), OffsetRows.None);
        query = VerifiedQuery("SELECT 'foo' OFFSET 1");
        Assert.Equal(expected, query.Offset);
    }

    [Fact]
    public void Parse_Fetch()
    {
        var firstTwoRows = new Fetch(new LiteralValue(Number("2")));

        var query = VerifiedQuery("SELECT foo FROM bar FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);

        query = VerifiedQuery("SELECT 'foo' FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar FETCH FIRST ROWS ONLY");
        Assert.Equal(new Fetch(), query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar WHERE foo = 4 FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar ORDER BY baz FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz FETCH FIRST 2 ROWS WITH TIES");
        Assert.Equal(new Fetch(new LiteralValue(Number("2")), true), query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar FETCH FIRST 50 PERCENT ROWS ONLY");
        Assert.Equal(new Fetch(new LiteralValue(Number("50")), false, true), query.Fetch);

        query = VerifiedQuery("SELECT foo FROM bar WHERE foo = 4 ORDER BY baz OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);
        Assert.Equal(new Offset(new LiteralValue(Number("2")), OffsetRows.Rows), query.Offset);

        query = VerifiedQuery(
            "SELECT foo FROM (SELECT * FROM bar OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY) OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY");
        Assert.Equal(firstTwoRows, query.Fetch);
        var body = (SetExpression.SelectExpression)query.Body;
        var subQuery = (TableFactor.Derived)body.Select.From!.Single().Relation!;
        var expected = new Offset(new LiteralValue(Number("2")), OffsetRows.Rows);
        Assert.Equal(expected, subQuery.SubQuery.Offset);
    }

    [Fact]
    public void Parse_Fetch_Variations()
    {
        OneStatementParsesTo(
            "SELECT foo FROM bar FETCH FIRST 10 ROW ONLY",
            "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY");

        OneStatementParsesTo(
            "SELECT foo FROM bar FETCH NEXT 10 ROW ONLY",
            "SELECT foo FROM bar FETCH FIRST 10 ROWS ONLY");

        OneStatementParsesTo(
            "SELECT foo FROM bar FETCH NEXT 10 ROWS WITH TIES",
            "SELECT foo FROM bar FETCH FIRST 10 ROWS WITH TIES");

        OneStatementParsesTo(
            "SELECT foo FROM bar FETCH NEXT ROWS WITH TIES",
            "SELECT foo FROM bar FETCH FIRST ROWS WITH TIES");

        OneStatementParsesTo(
            "SELECT foo FROM bar FETCH FIRST ROWS ONLY",
            "SELECT foo FROM bar FETCH FIRST ROWS ONLY");
    }

    [Fact]
    public void Lateral_Derived()
    {
        Test(false);
        Test(true);

        var ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT * FROM LATERAL UNNEST ([10,20,30]) as numbers WITH OFFSET;"));
        Assert.Equal("Expected end of statement, found WITH, Line: 1, Col: 54", ex.Message);

        ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT * FROM a LEFT JOIN LATERAL (b CROSS JOIN c)"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found b, Line: 1, Col: 36", ex.Message);
        return;

        void Test(bool lateralIn)
        {
            var lateral = lateralIn ? "LATERAL " : null;
            var sql =
                $"SELECT * FROM customer LEFT JOIN {lateral}(SELECT * FROM order WHERE order.customer = customer.id LIMIT 3) AS order ON true";
            var select = VerifiedOnlySelect(sql);
            var from = select.From!.Single();
            Assert.Single(from.Joins!);
            var join = from.Joins!.Single();
            var expected = new JoinOperator.LeftOuter(new JoinConstraint.On(new LiteralValue(new Value.Boolean(true))));
            Assert.Equal(expected, join.JoinOperator);

            if (join.Relation is TableFactor.Derived derived)
            {
                Assert.Equal(lateralIn, derived.Lateral);
                Assert.Equal("order", derived.Alias!.Name);
                Assert.Equal("SELECT * FROM order WHERE order.customer = customer.id LIMIT 3",
                    derived.SubQuery.ToSql());
            }
        }
    }

    [Fact]
    public void Test_Function()
    {
        const string sql = "SELECT * FROM customer LEFT JOIN LATERAL generate_series(1, customer.id)";
        var select = VerifiedOnlySelect(sql);

        var expected = new Select([new SelectItem.Wildcard(new WildcardAdditionalOptions())])
        {
            From =
            [
                new TableWithJoins(new TableFactor.Table("customer"))
                {
                    Joins =
                    [
                        new Join(new TableFactor.Function(true, "generate_series", [
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                                new LiteralValue(new Value.Number("1")))),
                            new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                                new CompoundIdentifier(["customer", "id"])))

                        ]), new JoinOperator.LeftOuter(new JoinConstraint.None()))
                    ]
                }
            ]
        };

        Assert.Equal(expected, select);
    }

    [Fact]
    public void Parse_Start_Transaction()
    {
        var transaction =
            VerifiedStatement<Statement.StartTransaction>(
                "START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE");
        var modes = new TransactionMode[]
        {
            new TransactionMode.AccessMode(TransactionAccessMode.ReadOnly),
            new TransactionMode.AccessMode(TransactionAccessMode.ReadWrite),
            new TransactionMode.IsolationLevel(TransactionIsolationLevel.Serializable)
        };
        Assert.Equal(modes, transaction.Modes!);

        transaction = OneStatementParsesTo<Statement.StartTransaction>(
            "START TRANSACTION READ ONLY READ WRITE ISOLATION LEVEL SERIALIZABLE",
            "START TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE");
        Assert.Equal(modes, transaction.Modes!);

        VerifiedStatement("START TRANSACTION");
        OneStatementParsesTo("BEGIN", "BEGIN TRANSACTION");
        OneStatementParsesTo("BEGIN WORK", "BEGIN TRANSACTION");
        OneStatementParsesTo("BEGIN TRANSACTION", "BEGIN TRANSACTION");
        VerifiedStatement("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        VerifiedStatement("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        VerifiedStatement("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        VerifiedStatement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");

        // Regression test for https://github.com/sqlparser-rs/sqlparser-rs/pull/139,
        Assert.Equal(
            ParseSqlStatements("START TRANSACTION; SELECT 1"),
            new[]
            {
                VerifiedStatement("START TRANSACTION"),
                VerifiedStatement("SELECT 1"),
            });

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("START TRANSACTION ISOLATION LEVEL BAD"));
        Assert.Equal("Expected isolation level, found BAD, Line: 1, Col: 35", ex.Message);
        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("START TRANSACTION BAD"));
        Assert.Equal("Expected end of statement, found BAD, Line: 1, Col: 19", ex.Message);
        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("START TRANSACTION READ ONLY,"));
        Assert.Equal("Expected transaction mode, found EOF", ex.Message);
    }

    [Fact]
    public void Parse_Set_Transaction()
    {
        var transaction =
            VerifiedStatement<Statement.SetTransaction>(
                "SET TRANSACTION READ ONLY, READ WRITE, ISOLATION LEVEL SERIALIZABLE");
        var modes = new TransactionMode[]
        {
            new TransactionMode.AccessMode(TransactionAccessMode.ReadOnly),
            new TransactionMode.AccessMode(TransactionAccessMode.ReadWrite),
            new TransactionMode.IsolationLevel(TransactionIsolationLevel.Serializable)
        };

        Assert.Equal(modes, transaction.Modes!);
        Assert.False(transaction.Session);
        Assert.Null(transaction.Snapshot);
    }

    [Fact]
    public void Parse_Set_Variable()
    {
        var variable = VerifiedStatement<Statement.SetVariable>("SET SOMETHING = '1'");
        Assert.False(variable.Local);
        Assert.False(variable.HiveVar);
        Assert.Equal(new OneOrManyWithParens<ObjectName>.One("SOMETHING"), variable.Variables);
        Assert.Equal(new[]
        {
            new LiteralValue(new Value.SingleQuotedString("1"))
        }, variable.Value);

        OneStatementParsesTo("SET SOMETHING TO '1'", "SET SOMETHING = '1'");

        var multiVariableDialects = AllDialects.Where(d => d.SupportsParenthesizedSetVariables).ToList();

        variable = VerifiedStatement<Statement.SetVariable>("SET (a, b, c) = (1, 2, 3)", multiVariableDialects);
        Assert.False(variable.Local);
        Assert.False(variable.HiveVar);
        Assert.Equal(new OneOrManyWithParens<ObjectName>.Many(["a", "b", "c"]), variable.Variables);
        Assert.Equal(
        [
            new LiteralValue(new Value.Number("1")),
            new LiteralValue(new Value.Number("2")),
            new LiteralValue(new Value.Number("3"))
        ], variable.Value);

        OneStatementParsesTo(
            "SET (a) = (SELECT 22 FROM tbl1)",
            "SET (a) = ((SELECT 22 FROM tbl1))",
            multiVariableDialects);

        OneStatementParsesTo(
            "SET (a) = (SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2))",
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
            multiVariableDialects);

        OneStatementParsesTo(
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
            "SET (a) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)))",
            multiVariableDialects);

        OneStatementParsesTo(
            "SET (a, b) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)), SELECT 33 FROM tbl3)",
            "SET (a, b) = ((SELECT 22 FROM tbl1, (SELECT 1 FROM tbl2)), (SELECT 33 FROM tbl3))",
            multiVariableDialects);

        Assert.Throws<ParserException>(() => ParseSqlStatements("SET (a, b, c) = (1, 2, 3", multiVariableDialects));
        Assert.Throws<ParserException>(() => ParseSqlStatements("SET (a, b, c) = 1, 2, 3", multiVariableDialects));
        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SET (a) = ((SELECT 22 FROM tbl1)", multiVariableDialects));
        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SET (a) = ((SELECT 22 FROM tbl1) (SELECT 22 FROM tbl1))", multiVariableDialects));

        OneStatementParsesTo("SET SOMETHING TO '1'", "SET SOMETHING = '1'");
    }

    [Fact]
    public void Parse_Set_Time_Zone()
    {
        var variable = VerifiedStatement<Statement.SetVariable>("SET TIMEZONE = 'UTC'");
        Assert.False(variable.Local);
        Assert.False(variable.HiveVar);
        Assert.Equal(new OneOrManyWithParens<ObjectName>.One("TIMEZONE"), variable.Variables);
        Assert.Equal(new[]
        {
            new LiteralValue(new Value.SingleQuotedString("UTC"))
        }, variable.Value);

        OneStatementParsesTo("SET TIME ZONE TO 'UTC'", "SET TIMEZONE = 'UTC'");
    }

    [Fact]
    public void Parse_Set_Time_Zone_Alias()
    {
        var variable = VerifiedStatement<Statement.SetTimeZone>("SET TIME ZONE 'UTC'");
        Assert.False(variable.Local);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedString("UTC")), variable.Value);
    }

    [Fact]
    public void Parse_Commit()
    {
        var commit = VerifiedStatement<Statement.Commit>("COMMIT");
        Assert.Equal(new Statement.Commit(), commit);

        commit = VerifiedStatement<Statement.Commit>("COMMIT AND CHAIN");
        Assert.Equal(new Statement.Commit(true), commit);

        OneStatementParsesTo("COMMIT AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("COMMIT WORK AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("COMMIT TRANSACTION AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("COMMIT WORK AND CHAIN", "COMMIT AND CHAIN");
        OneStatementParsesTo("COMMIT TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
        OneStatementParsesTo("COMMIT WORK", "COMMIT");
        OneStatementParsesTo("COMMIT TRANSACTION", "COMMIT");
    }

    [Fact]
    public void Parse_Rollback()
    {
        var commit = VerifiedStatement<Statement.Rollback>("ROLLBACK");
        Assert.Equal(new Statement.Rollback(false), commit);

        commit = VerifiedStatement<Statement.Rollback>("ROLLBACK AND CHAIN");
        Assert.Equal(new Statement.Rollback(true), commit);

        commit = VerifiedStatement<Statement.Rollback>("ROLLBACK TO SAVEPOINT test1");
        Assert.Equal(new Statement.Rollback(false, "test1"), commit);

        commit = VerifiedStatement<Statement.Rollback>("ROLLBACK AND CHAIN TO SAVEPOINT test1");
        Assert.Equal(new Statement.Rollback(true, "test1"), commit);

        OneStatementParsesTo("ROLLBACK AND NO CHAIN", "ROLLBACK");
        OneStatementParsesTo("ROLLBACK WORK AND NO CHAIN", "ROLLBACK");
        OneStatementParsesTo("ROLLBACK TRANSACTION AND NO CHAIN", "ROLLBACK");
        OneStatementParsesTo("ROLLBACK WORK AND CHAIN", "ROLLBACK AND CHAIN");
        OneStatementParsesTo("ROLLBACK TRANSACTION AND CHAIN", "ROLLBACK AND CHAIN");
        OneStatementParsesTo("ROLLBACK WORK", "ROLLBACK");
        OneStatementParsesTo("ROLLBACK TRANSACTION", "ROLLBACK");
        OneStatementParsesTo("ROLLBACK TO test1", "ROLLBACK TO SAVEPOINT test1");
        OneStatementParsesTo("ROLLBACK AND CHAIN TO test1", "ROLLBACK AND CHAIN TO SAVEPOINT test1");
    }

    [Fact]
    public void Parse_Multiple_Dialects_Are_Tested()
    {
        // The SQL here must be parsed differently by different dialects.
        // At the time of writing, `@foo` is accepted as a valid identifier
        // by the Generic and the MSSQL dialect, but not by Postgres and ANSI.

        ParseSqlStatements("SELECT @foo", new[] { new GenericDialect() }); //, new GenericDialect()
    }

    [Fact]
    public void Parse_Create_Index()
    {
        var expected = new OrderByExpression[]
        {
            new(new Identifier("name")),
            new(new Identifier("age"), false)
        };

        var createIndex =
            VerifiedStatement<Statement.CreateIndex>(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test(name,age DESC)");

        Assert.Equal("idx_name", createIndex.Element.Name!);
        Assert.Equal("test", createIndex.Element.TableName);
        Assert.Equal(expected, createIndex.Element.Columns!);
        Assert.True(createIndex.Element.Unique);
        Assert.True(createIndex.Element.IfNotExists);
        Assert.False(createIndex.Element.Concurrently);
        Assert.Null(createIndex.Element.Include);
    }

    [Fact]
    public void Parse_Create_Index_With_Using_Function()
    {
        var expected = new OrderByExpression[]
        {
            new(new Identifier("name")),
            new(new Identifier("age"), false)
        };

        var createIndex =
            VerifiedStatement<Statement.CreateIndex>(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test USING btree(name,age DESC)");

        Assert.Equal("idx_name", createIndex.Element.Name!);
        Assert.Equal("test", createIndex.Element.TableName);
        Assert.Equal("btree", createIndex.Element.Using!);
        Assert.Equal(expected, createIndex.Element.Columns!);
        Assert.True(createIndex.Element.Unique);
        Assert.True(createIndex.Element.IfNotExists);
    }

    [Fact]
    public void Parse_Drop_Index()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP INDEX idx_a");

        Assert.Equal(new ObjectName[] { "idx_a" }, drop.Names);
    }

    [Fact]
    public void Parse_Create_Role()
    {
        var role = VerifiedStatement<Statement.CreateRole>("CREATE ROLE consultant");

        Assert.Equal(new ObjectName[] { "consultant" }, role.Names);

        role = VerifiedStatement<Statement.CreateRole>("CREATE ROLE IF NOT EXISTS mysql_a, mysql_b");

        Assert.Equal(new ObjectName[] { "mysql_a", "mysql_b" }, role.Names);
        Assert.True(role.IfNotExists);
    }

    [Fact]
    public void Parse_Drop_Role()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP ROLE abc");

        Assert.Equal(new ObjectName[] { "abc" }, drop.Names);
        Assert.Equal(ObjectType.Role, drop.ObjectType);
        Assert.False(drop.IfExists);

        drop = VerifiedStatement<Statement.Drop>("DROP ROLE IF EXISTS def, magician, quaternion");

        Assert.Equal(new ObjectName[] { "def", "magician", "quaternion" }, drop.Names);
        Assert.Equal(ObjectType.Role, drop.ObjectType);
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void Parse_Grant()
    {
        var grant = VerifiedStatement<Statement.Grant>(
            "GRANT SELECT, INSERT, UPDATE (shape, size), USAGE, DELETE, TRUNCATE, REFERENCES, TRIGGER, CONNECT, CREATE, EXECUTE, TEMPORARY ON abc, def TO xyz, m WITH GRANT OPTION GRANTED BY jj");
        var privileges = new Privileges.Actions(new Action[]
        {
            new Action.Select(),
            new Action.Insert(),
            new Action.Update(new Ident[] { "shape", "size" }),
            new Action.Usage(),
            new Action.Delete(),
            new Action.Truncate(),
            new Action.References(),
            new Action.Trigger(),
            new Action.Connect(),
            new Action.Create(),
            new Action.Execute(),
            new Action.Temporary(),
        });
        Assert.Equal(privileges, (Privileges.Actions)grant.Privileges);
        Assert.Equal(new ObjectName[] { "abc", "def" }, ((GrantObjects.Tables)grant.Objects!).Schemas);
        Assert.Equal(new Ident[] { "xyz", "m" }, grant.Grantees);
        Assert.True(grant.WithGrantOption);
        Assert.Equal("jj", grant.GrantedBy!);

        grant = VerifiedStatement<Statement.Grant>("GRANT INSERT ON ALL TABLES IN SCHEMA public TO browser");
        privileges = new Privileges.Actions(new[]
        {
            new Action.Insert()
        });
        Assert.Equal(privileges, (Privileges.Actions)grant.Privileges);
        Assert.Equal(new ObjectName[] { "public" }, ((GrantObjects.AllTablesInSchema)grant.Objects!).Schemas);
        Assert.Equal(new Ident[] { "browser" }, grant.Grantees);
        Assert.False(grant.WithGrantOption);

        grant = VerifiedStatement<Statement.Grant>("GRANT USAGE, SELECT ON SEQUENCE p TO u");
        privileges = new Privileges.Actions(new Action[]
        {
            new Action.Usage(),
            new Action.Select()
        });
        Assert.Equal(privileges, (Privileges.Actions)grant.Privileges);
        Assert.Equal(new ObjectName[] { "p" }, ((GrantObjects.Sequences)grant.Objects!).Schemas);
        Assert.Equal(new Ident[] { "u" }, grant.Grantees);

        grant = VerifiedStatement<Statement.Grant>("GRANT ALL PRIVILEGES ON aa, b TO z");
        Assert.True(((Privileges.All)grant.Privileges).WithPrivilegesKeyword);

        grant = VerifiedStatement<Statement.Grant>("GRANT ALL ON SCHEMA aa, b TO z");
        Assert.False(((Privileges.All)grant.Privileges).WithPrivilegesKeyword);
        Assert.Equal(new ObjectName[] { "aa", "b" }, ((GrantObjects.Schema)grant.Objects!).Schemas);

        grant = VerifiedStatement<Statement.Grant>(
            "GRANT USAGE ON ALL SEQUENCES IN SCHEMA bus TO a, beta WITH GRANT OPTION");
        privileges = new Privileges.Actions(new[]
        {
            new Action.Usage()
        });
        Assert.Equal(privileges, (Privileges.Actions)grant.Privileges);
        Assert.Equal(new ObjectName[] { "bus" }, ((GrantObjects.AllSequencesInSchema)grant.Objects!).Schemas);
    }

    [Fact]
    public void Test_Revoke()
    {
        var revoke = VerifiedStatement<Statement.Revoke>("REVOKE ALL PRIVILEGES ON users, auth FROM analyst CASCADE");

        Assert.Equal(new Privileges.All(true), revoke.Privileges);
        Assert.Equal(new ObjectName[] { "users", "auth" }, ((GrantObjects.Tables)revoke.Objects).Schemas);
        Assert.Equal(new Ident[] { "analyst" }, revoke.Grantees);
        Assert.Null(revoke.GrantedBy);
    }

    [Fact]
    public void Parse_Merge()
    {
        var mergeInto = VerifiedStatement<Statement.Merge>("""
                                                           MERGE INTO s.bar AS dest USING (SELECT * FROM s.foo) AS stg ON dest.D = stg.D AND dest.E = stg.E
                                                            WHEN NOT MATCHED THEN INSERT (A, B, C) VALUES (stg.A, stg.B, stg.C)
                                                            WHEN MATCHED AND dest.A = 'a' THEN UPDATE SET dest.F = stg.F, dest.G = stg.G
                                                            WHEN MATCHED THEN DELETE
                                                           """);

        var mergeNoInto = VerifiedStatement<Statement.Merge>("""
                                                             MERGE s.bar AS dest USING (SELECT * FROM s.foo) AS stg ON dest.D = stg.D AND dest.E = stg.E
                                                              WHEN NOT MATCHED THEN INSERT (A, B, C) VALUES (stg.A, stg.B, stg.C)
                                                              WHEN MATCHED AND dest.A = 'a' THEN UPDATE SET dest.F = stg.F, dest.G = stg.G
                                                              WHEN MATCHED THEN DELETE
                                                             """);

        Assert.True(mergeInto.Into);
        Assert.False(mergeNoInto.Into);

        Assert.Equal(
            new TableFactor.Table(new ObjectName(new Ident[] { "s", "bar" })) { Alias = new TableAlias("dest") },
            mergeInto.Table);
        Assert.Equal(mergeInto.Table, mergeNoInto.Table);
        var body = new SetExpression.SelectExpression(new Select(new[]
        {
            new SelectItem.Wildcard(new WildcardAdditionalOptions())
        })
        {
            From = new TableWithJoins[]
            {
                new(new TableFactor.Table(new ObjectName(["s", "foo"])))
            }
        });
        Assert.Equal(
            new TableFactor.Derived(new Query(body))
            {
                Alias = new TableAlias("stg")
            },
            mergeInto.Source);

        Assert.Equal(mergeInto.Source, mergeNoInto.Source);

        Assert.Equal(
            new BinaryOp(
                new BinaryOp(
                    new CompoundIdentifier(new Ident[] { "dest", "D" }),
                    BinaryOperator.Eq,
                    new CompoundIdentifier(new Ident[] { "stg", "D" })
                ),
                BinaryOperator.And,
                new BinaryOp(
                    new CompoundIdentifier(new Ident[] { "dest", "E" }),
                    BinaryOperator.Eq,
                    new CompoundIdentifier(new Ident[] { "stg", "E" })
                )
            ),
            mergeInto.On);

        Assert.Equal(mergeInto.On, mergeNoInto.On);

        Assert.Equal(new MergeClause[]
            {
                new(MergeClauseKind.NotMatched, new MergeAction.Insert(
                        new MergeInsertExpression(["A", "B", "C"],
                            new MergeInsertKind.Values(new Values([
                                [
                                    new CompoundIdentifier(new Ident[] { "stg", "A" }),
                                    new CompoundIdentifier(new Ident[] { "stg", "B" }),
                                    new CompoundIdentifier(new Ident[] { "stg", "C" })
                                ]
                            ])))
                    )
                ),
                new(MergeClauseKind.Matched, new MergeAction.Update([
                        new(new AssignmentTarget.ColumnName(new ObjectName(["dest", "F"])),
                            new CompoundIdentifier(new Ident[] { "stg", "F" })),
                        new(new AssignmentTarget.ColumnName(new ObjectName(["dest", "G"])),
                            new CompoundIdentifier(new Ident[] { "stg", "G" })),
                    ]),
                    new BinaryOp(
                        new CompoundIdentifier(new Ident[] { "dest", "A" }),
                        BinaryOperator.Eq,
                        new LiteralValue(new Value.SingleQuotedString("a"))
                    )
                ),
                new(MergeClauseKind.Matched, new MergeAction.Delete())
            },
            mergeNoInto.Clauses);
    }

    [Fact]
    public void Test_Merge_Into_Using_Table()
    {
        VerifiedStatement("""
                          MERGE INTO target_table USING source_table
                           ON target_table.id = source_table.oooid
                           WHEN MATCHED THEN
                           UPDATE SET target_table.description = source_table.description
                           WHEN NOT MATCHED THEN
                           INSERT (ID, description) VALUES (source_table.id, source_table.description)
                          """);
    }

    [Fact]
    public void Test_Merge_With_Delimiter()
    {
        ParseSqlStatements("""
                           MERGE INTO target_table USING source_table
                           ON target_table.id = source_table.oooid
                           WHEN MATCHED THEN
                           UPDATE SET target_table.description = source_table.description
                           WHEN NOT MATCHED THEN
                           INSERT (ID, description) VALUES (source_table.id, source_table.description);
                           """);
    }

    [Fact]
    public void Test_Lock()
    {
        var query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR UPDATE");
        var @lock = query.Locks!.Single();
        Assert.Equal(LockType.Update, @lock.LockType);
        Assert.Null(@lock.Of);
        Assert.Equal(NonBlock.None, @lock.NonBlock);

        query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR SHARE");
        @lock = query.Locks!.Single();
        Assert.Equal(LockType.Share, @lock.LockType);
        Assert.Null(@lock.Of);
        Assert.Equal(NonBlock.None, @lock.NonBlock);
    }

    [Fact]
    public void Test_Lock_Table()
    {
        var query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR UPDATE OF school");

        var @lock = query.Locks!.Single();
        Assert.Equal(LockType.Update, @lock.LockType);
        Assert.Equal("school", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.None, @lock.NonBlock);

        query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR SHARE OF school");
        @lock = query.Locks!.Single();
        Assert.Equal(LockType.Share, @lock.LockType);
        Assert.Equal("school", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.None, @lock.NonBlock);

        query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR SHARE OF school FOR UPDATE OF student");
        Assert.Equal(2, query.Locks!.Count);
        @lock = query.Locks!.First();
        Assert.Equal(LockType.Share, @lock.LockType);
        Assert.Equal("school", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.None, @lock.NonBlock);

        @lock = query.Locks!.Last();
        Assert.Equal(LockType.Update, @lock.LockType);
        Assert.Equal("student", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.None, @lock.NonBlock);
    }

    [Fact]
    public void Test_Lock_NonBlock()
    {
        var query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR UPDATE OF school SKIP LOCKED");

        var @lock = query.Locks!.Single();
        Assert.Equal(LockType.Update, @lock.LockType);
        Assert.Equal("school", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.SkipLocked, @lock.NonBlock);

        query = VerifiedQuery("SELECT * FROM student WHERE id = '1' FOR SHARE OF school NOWAIT");

        @lock = query.Locks!.Single();
        Assert.Equal(LockType.Share, @lock.LockType);
        Assert.Equal("school", @lock.Of!.Values.First());
        Assert.Equal(NonBlock.Nowait, @lock.NonBlock);
    }

    [Fact]
    public void Test_Placeholder()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new DuckDbDialect(),
            new PostgreSqlDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new BigQueryDialect(),
            new SnowflakeDialect()
            // Note: `$` is the starting word for the HiveDialect identifier
        };

        var select = VerifiedOnlySelect("SELECT * FROM student WHERE id = $Id1", dialects);
        Assert.Equal(
            new BinaryOp(
                new Identifier("id"),
                BinaryOperator.Eq,
                new LiteralValue(new Value.Placeholder("$Id1"))),
            select.Selection);

        var query = VerifiedQuery("SELECT * FROM student LIMIT $1 OFFSET $2", dialects);
        Assert.Equal(new LiteralValue(new Value.Placeholder("$1")), query.Limit);
        Assert.Equal(new Offset(new LiteralValue(new Value.Placeholder("$2")), OffsetRows.None), query.Offset);

        dialects =
        [
            new GenericDialect(),
            new DuckDbDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new BigQueryDialect(),
            new SnowflakeDialect()
        ];

        select = VerifiedOnlySelect("SELECT * FROM student WHERE id = ?", dialects);
        Assert.Equal(
            new BinaryOp(
                new Identifier("id"),
                BinaryOperator.Eq,
                new LiteralValue(new Value.Placeholder("?")))
            , select.Selection);

        select = VerifiedOnlySelect("SELECT $fromage_français, :x, ?123", dialects);
        Assert.Equal(new[]
        {
            new SelectItem.UnnamedExpression(new LiteralValue(new Value.Placeholder("$fromage_français"))),
            new SelectItem.UnnamedExpression(new LiteralValue(new Value.Placeholder(":x"))),
            new SelectItem.UnnamedExpression(new LiteralValue(new Value.Placeholder("?123")))
        }, select.Projection);
    }

    [Fact]
    public void All_Keywords_Sorted()
    {
        var keywords = Keywords.All;

        var names = System.Enum.GetNames<Keyword>()
            .Where(n => n != nameof(Keyword.undefined))
            .ToArray();

        for (var i = 0; i < names.Length; i++)
        {
            var name = names[i];

            if (i == (int)Keyword.END_EXEC)
            {
                name = name.Replace("_", "-");
            }

            Assert.True(name == keywords[i]);
        }
    }

    [Fact]
    public void Parse_Offset_And_Limit()
    {
        var limit = new LiteralValue(Number("2"));
        var expected = new Offset(limit, OffsetRows.None);
        var query = VerifiedQuery("SELECT foo FROM bar LIMIT 2 OFFSET 2");

        Assert.Equal(expected, query.Offset);
        Assert.Equal(limit, query.Limit);

        // different order is OK
        OneStatementParsesTo(
            "SELECT foo FROM bar OFFSET 2 LIMIT 2",
            "SELECT foo FROM bar LIMIT 2 OFFSET 2");

        query = VerifiedQuery("SELECT foo FROM bar LIMIT 1 + 2 OFFSET 3 * 4");

        Assert.Equal(new BinaryOp(
            new LiteralValue(Number("1")),
            BinaryOperator.Plus,
            new LiteralValue(Number("2"))
        ), query.Limit);
        Assert.Equal(new Offset(
                new BinaryOp(
                    new LiteralValue(Number("3")),
                    BinaryOperator.Multiply,
                    new LiteralValue(Number("4"))
                ), OffsetRows.None)
            , query.Offset);

        // Can't repeat OFFSET / LIMIT
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT foo FROM bar OFFSET 2 OFFSET 2"));
        Assert.Equal("Expected end of statement, found OFFSET, Line: 1, Col: 30", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT foo FROM bar LIMIT 2 LIMIT 2"));
        Assert.Equal("Expected end of statement, found LIMIT, Line: 1, Col: 29", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT foo FROM bar OFFSET 2 LIMIT 2 OFFSET 2"));
        Assert.Equal("Expected end of statement, found OFFSET, Line: 1, Col: 38", ex.Message);
    }

    [Fact]
    public void Parse_Time_Functions()
    {
        TestTimeFunction("CURRENT_TIMESTAMP");
        TestTimeFunction("CURRENT_TIME");
        TestTimeFunction("CURRENT_DATE");
        TestTimeFunction("LOCALTIME");
        TestTimeFunction("LOCALTIMESTAMP");
        return;

        void TestTimeFunction(string timeFunction)
        {
            // Validating Parenthesis
            var sql = $"SELECT {timeFunction}()";
            var select = VerifiedOnlySelect(sql);

            var expected = new Function(timeFunction)
            {
                Args = new FunctionArguments.List(FunctionArgumentList.Empty())
            };
            Assert.Equal(expected, select.Projection.First().AsExpr());

            sql = $"SELECT {timeFunction}";
            select = VerifiedOnlySelect(sql);
            expected = new Function(timeFunction)
            {
                Args = new FunctionArguments.None()
            };
            Assert.Equal(expected, select.Projection.First().AsExpr());
        }
    }

    [Fact]
    public void Parse_Position()
    {
        Expression expected =
            new Position(new LiteralValue(new Value.SingleQuotedString("@")), new Identifier("field"));
        var position = VerifiedExpr("POSITION('@' IN field)", new[] { new PostgreSqlDialect() });
        Assert.Equal(expected, position);

        expected = new Function("position")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.SingleQuotedString("an")))),
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(
                        new LiteralValue(new Value.SingleQuotedString("banana")))),
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.Number("1")))),
            ]))
        };
        position = VerifiedExpr("position('an', 'banana', 1)");
        Assert.Equal(expected, position);
    }

    [Fact]
    public void Parse_Position_Negative()
    {
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT POSITION(foo IN) from bar"));
        Assert.Equal("Expected (, found ), Line: 1, Col: 23", ex.Message);
    }

    [Fact]
    public void Parse_Is_Boolean()
    {
        Assert.Equal(new IsTrue(new Identifier("a")), VerifiedExpr("a IS TRUE"));
        Assert.Equal(new IsNotTrue(new Identifier("a")), VerifiedExpr("a IS NOT TRUE"));
        Assert.Equal(new IsFalse(new Identifier("a")), VerifiedExpr("a IS FALSE"));
        Assert.Equal(new IsNotFalse(new Identifier("a")), VerifiedExpr("a IS NOT FALSE"));
        Assert.Equal(new IsUnknown(new Identifier("a")), VerifiedExpr("a IS UNKNOWN"));
        Assert.Equal(new IsNotUnknown(new Identifier("a")), VerifiedExpr("a IS NOT UNKNOWN"));

        VerifiedStatement("SELECT f FROM foo WHERE field IS TRUE");
        VerifiedStatement("SELECT f FROM foo WHERE field IS NOT TRUE");
        VerifiedStatement("SELECT f FROM foo WHERE field IS FALSE");
        VerifiedStatement("SELECT f FROM foo WHERE field IS NOT FALSE");
        VerifiedStatement("SELECT f FROM foo WHERE field IS UNKNOWN");
        VerifiedStatement("SELECT f FROM foo WHERE field IS NOT UNKNOWN");

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT f from foo where field is 0"));
        Assert.Equal("Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found 0, Line: 1, Col: 34",
            ex.Message);
    }

    [Fact]
    public void Parse_Discard()
    {
        var discard = VerifiedStatement<Statement.Discard>("DISCARD ALL");
        Assert.Equal(DiscardObject.All, discard.ObjectType);

        discard = VerifiedStatement<Statement.Discard>("DISCARD PLANS");
        Assert.Equal(DiscardObject.Plans, discard.ObjectType);

        discard = VerifiedStatement<Statement.Discard>("DISCARD SEQUENCES");
        Assert.Equal(DiscardObject.Sequences, discard.ObjectType);

        discard = VerifiedStatement<Statement.Discard>("DISCARD TEMP");
        Assert.Equal(DiscardObject.Temp, discard.ObjectType);
    }

    [Fact]
    public void Parse_Cursor()
    {
        var close = VerifiedStatement<Statement.Close>("CLOSE my_cursor");
        Assert.Equal(new CloseCursor.Specific("my_cursor"), close.Cursor);

        close = VerifiedStatement<Statement.Close>("CLOSE ALL");
        Assert.Equal(new CloseCursor.All(), close.Cursor);
    }

    [Fact]
    public void Parse_Show_Functions()
    {
        var show = VerifiedStatement<Statement.ShowFunctions>("SHOW FUNCTIONS LIKE 'pattern'");
        Assert.Equal(new Statement.ShowFunctions(new ShowStatementFilter.Like("pattern")), show);
    }

    [Fact]
    public void Parse_Cache_Table()
    {
        const string sql = "SELECT a, b, c FROM foo";
        const string cacheTableName = "cache_table_name";
        const string tableFlag = "flag";
        var query = VerifiedQuery(sql);

        var cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)));
        Assert.Equal(cache, VerifiedStatement<Statement.Cache>($"CACHE TABLE '{cacheTableName}'"));

        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            TableFlag = new ObjectName(tableFlag)
        };
        Assert.Equal(cache, VerifiedStatement<Statement.Cache>($"CACHE {tableFlag} TABLE '{cacheTableName}'"));


        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            TableFlag = new ObjectName(tableFlag),
            Options = new SqlOption[]
            {
                new SqlOption.KeyValue(new Ident("K1", Symbols.SingleQuote),
                    new LiteralValue(new Value.SingleQuotedString("V1"))),
                new SqlOption.KeyValue(new Ident("K2", Symbols.SingleQuote), new LiteralValue(Number("0.88")))
            }
        };
        Assert.Equal(cache,
            VerifiedStatement<Statement.Cache>(
                $"CACHE {tableFlag} TABLE '{cacheTableName}' OPTIONS('K1' = 'V1', 'K2' = 0.88)"));


        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            TableFlag = new ObjectName(tableFlag),
            Options = new SqlOption[]
            {
                new SqlOption.KeyValue(new Ident("K1", Symbols.SingleQuote),
                    new LiteralValue(new Value.SingleQuotedString("V1"))),
                new SqlOption.KeyValue(new Ident("K2", Symbols.SingleQuote), new LiteralValue(Number("0.88")))
            },
            Query = query
        };
        Assert.Equal(cache,
            VerifiedStatement<Statement.Cache>(
                $"CACHE {tableFlag} TABLE '{cacheTableName}' OPTIONS('K1' = 'V1', 'K2' = 0.88) {sql}"));


        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            HasAs = true,
            TableFlag = new ObjectName(tableFlag),
            Options = new SqlOption[]
            {
                new SqlOption.KeyValue(new Ident("K1", Symbols.SingleQuote),
                    new LiteralValue(new Value.SingleQuotedString("V1"))),
                new SqlOption.KeyValue(new Ident("K2", Symbols.SingleQuote), new LiteralValue(Number("0.88")))
            },
            Query = query
        };
        Assert.Equal(cache,
            VerifiedStatement<Statement.Cache>(
                $"CACHE {tableFlag} TABLE '{cacheTableName}' OPTIONS('K1' = 'V1', 'K2' = 0.88) AS {sql}"));


        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            TableFlag = new ObjectName(tableFlag),
            Query = query
        };
        Assert.Equal(cache, VerifiedStatement<Statement.Cache>($"CACHE {tableFlag} TABLE '{cacheTableName}' {sql}"));


        cache = new Statement.Cache(new ObjectName(new Ident(cacheTableName, Symbols.SingleQuote)))
        {
            HasAs = true,
            TableFlag = new ObjectName(tableFlag),
            Query = query
        };
        Assert.Equal(cache, VerifiedStatement<Statement.Cache>($"CACHE {tableFlag} TABLE '{cacheTableName}' AS {sql}"));


        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE TABLE 'table_name' foo"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found foo, Line: 1, Col: 26",
            ex.Message);


        ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("CACHE flag TABLE 'table_name' OPTIONS('K1'='V1') foo"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found foo, Line: 1, Col: 50",
            ex.Message);


        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE TABLE 'table_name' AS foo"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found foo, Line: 1, Col: 29",
            ex.Message);


        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE TABLE 'table_name' AS foo"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found foo, Line: 1, Col: 29",
            ex.Message);


        ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("CACHE flag TABLE 'table_name' OPTIONS('K1'='V1') AS foo"));
        Assert.Equal("Expected SELECT, VALUES, or a subquery in the query body, found foo, Line: 1, Col: 53",
            ex.Message);


        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE 'table_name'"));
        Assert.Equal("Expected a TABLE keyword, found 'table_name', Line: 1, Col: 7", ex.Message);


        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE 'table_name' OPTIONS('K1'='V1')"));
        Assert.Equal("Expected a TABLE keyword, found OPTIONS, Line: 1, Col: 20", ex.Message);


        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("CACHE flag 'table_name' OPTIONS('K1'='V1')"));
        Assert.Equal("Expected a TABLE keyword, found 'table_name', Line: 1, Col: 12", ex.Message);
    }

    [Fact]
    public void Parse_Uncache_Table()
    {
        var cache = VerifiedStatement<Statement.UNCache>("UNCACHE TABLE 'table_name'");
        var expected = new Statement.UNCache(new ObjectName(new Ident("table_name", Symbols.SingleQuote)));
        Assert.Equal(expected, cache);

        cache = VerifiedStatement<Statement.UNCache>("UNCACHE TABLE IF EXISTS 'table_name'");
        expected = new Statement.UNCache(new ObjectName(new Ident("table_name", Symbols.SingleQuote)), true);
        Assert.Equal(expected, cache);

        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("UNCACHE TABLE 'table_name' foo"));
        Assert.Equal("Expected end of statement, found foo, Line: 1, Col: 28", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("UNCACHE 'table_name' foo"));
        Assert.Equal("Expected TABLE, found 'table_name', Line: 1, Col: 9", ex.Message);

        ex = Assert.Throws<ParserException>(() => ParseSqlStatements("UNCACHE IF EXISTS 'table_name' foo"));
        Assert.Equal("Expected TABLE, found IF, Line: 1, Col: 9", ex.Message);
    }

    [Fact]
    public void Parse_Deeply_Nested_Parens_Hits_Recursion_Limits()
    {
        var sql = new string(Symbols.ParenOpen, 1000);
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
        Assert.Equal("Recursion limit exceeded.", ex.Message);
    }

    [Fact]
    public void Parse_Deeply_Nested_Expr_Hits_Recursion_Limits()
    {
        var dialect = new GenericDialect();
        var whereClause = new StringBuilder(new string(Symbols.ParenOpen, 99));

        for (var i = 0; i < 100; i++)
        {
            if (i > 0)
            {
                whereClause.Append(" OR ");
            }

            whereClause.Append($"user_id = {i})");
        }

        var sql = $"SELECT id, user_id FROM test WHERE {whereClause}";

        var parser = new Parser().TryWithSql(sql, dialect);
        var ex = Assert.Throws<ParserException>(() => parser.ParseSql(sql, dialect));
        Assert.Equal("Recursion limit exceeded.", ex.Message);
    }

    [Fact]
    public void Parse_Deeply_Nested_Subquery_Expr_Hits_Recursion_Limits()
    {
        var dialect = new GenericDialect();
        var whereClause = new StringBuilder(new string(Symbols.ParenOpen, 99));

        for (var i = 0; i < 100; i++)
        {
            if (i > 0)
            {
                whereClause.Append(" OR ");
            }

            whereClause.Append($"user_id = {i})");
        }

        var sql = $"SELECT id, user_id where id IN (select id from t WHERE {whereClause})";
        var parser = new Parser().TryWithSql(sql, dialect);
        var ex = Assert.Throws<ParserException>(() => parser.ParseSql(sql, dialect));
        Assert.Equal("Recursion limit exceeded.", ex.Message);
    }

    [Fact]
    public void Parse_With_Recursion_Limit()
    {
        var dialect = new GenericDialect();
        var whereClause = new StringBuilder(new string(Symbols.ParenOpen, 19));

        for (var i = 0; i < 20; i++)
        {
            if (i > 0)
            {
                whereClause.Append(" OR ");
            }

            whereClause.Append($"user_id = {i}");

            if (i < 19)
            {
                whereClause.Append(Symbols.ParenClose);
            }
        }

        var sql = $"SELECT id, user_id FROM test WHERE {whereClause}";
        var parser = new Parser().TryWithSql(sql, dialect);

        // Expect the statement to parse with default limit
        parser.ParseStatements();

        var options = new ParserOptions { RecursionLimit = 20 };
        var ex = Assert.Throws<ParserException>(() => parser.ParseSql(sql, dialect, options));
        Assert.Equal("Recursion limit exceeded.", ex.Message);
    }

    [Fact]
    public void Parse_Nested_Explain_Error()
    {
        var ex = Assert.Throws<ParserException>(() =>
            new Parser().ParseSql("EXPLAIN EXPLAIN SELECT 1", new GenericDialect()));
        Assert.Equal("Explain must be root of the plan.", ex.Message);
    }

    [Fact]
    public void Parse_Non_Latin_Identifiers()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new DuckDbDialect(),
            new PostgreSqlDialect(),
            new MySqlDialect(),
            new RedshiftDialect(),
            new MySqlDialect()
        };

        VerifiedStatement("SELECT a.説明 FROM test.public.inter01 AS a", dialects);
        VerifiedStatement("SELECT a.説明 FROM inter01 AS a, inter01_transactions AS b WHERE a.説明 = b.取引 GROUP BY a.説明",
            dialects);
        VerifiedStatement("SELECT 説明, hühnervögel, garçon, Москва, 東京 FROM inter01", dialects);

        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT 💝 FROM table1", dialects));
    }

    [Fact]
    public void Parse_Pivot_Table()
    {
        const string sql = """
                           SELECT * FROM monthly_sales AS a PIVOT(SUM(a.amount),
                            SUM(b.amount) AS t,
                            SUM(c.amount) AS u
                            FOR a.MONTH IN (1 AS x, 'two', three AS y)) AS p (c, d)
                            ORDER BY EMPID
                           """;

        var statement = VerifiedOnlySelect(sql);
        var from = statement.From![0];
        var relation = from.Relation;

        var functions = new Sequence<ExpressionWithAlias>
        {
            ExpectedFn("a", null),
            ExpectedFn("b", "t"),
            ExpectedFn("c", "u")
        };
        var table = new TableFactor.Table("monthly_sales")
        {
            Alias = new TableAlias("a")
        };
        var expected = new TableFactor.Pivot(table, functions, new Ident[] { "a", "MONTH" },
            new PivotValueSource.List([
                new ExpressionWithAlias(new LiteralValue(new Value.Number("1")), "x"),
                new ExpressionWithAlias(new LiteralValue(new Value.SingleQuotedString("two")), null),
                new ExpressionWithAlias(new Identifier("three"), "y"),
            ]),
            null,
            new TableAlias("p", new Ident[] { "c", "d" }));

        Assert.Equal(expected, relation);
        Assert.Equal(sql.Replace("\r", "").Replace("\n", ""), VerifiedStatement(sql).ToSql());

        ExpressionWithAlias ExpectedFn(string t, string? alias)
        {
            var expr = new Function("SUM")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                        new CompoundIdentifier([new Ident(t), new Ident("amount")])))
                ]))
            };
            return new ExpressionWithAlias(expr, alias != null ? new Ident(alias) : null);
        }
    }

    [Fact]
    public void Parse_Unpivot_Table()
    {
        const string sql = """
                           SELECT * FROM sales AS s
                            UNPIVOT(quantity FOR quarter IN (Q1, Q2, Q3, Q4)) AS u (product, quarter, quantity)
                           """;

        var statement = VerifiedOnlySelect(sql);
        var from = statement.From![0];
        var relation = from.Relation;

        var table = new TableFactor.Table("sales")
        {
            Alias = new TableAlias("s")
        };
        var expected = new TableFactor.Unpivot(table, "quantity", "quarter", ["Q1", "Q2", "Q3", "Q4"])
        {
            PivotAlias = new TableAlias("u", ["product", "quarter", "quantity"])
        };


        Assert.Equal(expected, relation);
    }

    [Fact]
    public void Parser_Manages_Recursion_Depth()
    {
        // No exception thrown guarantees recursion did not reach zero. 
        // The default depth is 50 which, if the scope is set correctly,
        // will never be reached.
        var range = Enumerable.Range(0, 100).Select(_ => "select * from tablename;");
        var query = string.Join(Environment.NewLine, range);
        new Parser().ParseSql(query);
    }

    [Fact]
    public void Parse_Arg_With_Order_By()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new PostgreSqlDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new HiveDialect()
        };

        var queries = new[]
        {
            "SELECT FIRST_VALUE(x ORDER BY x) AS a FROM T",
            "SELECT FIRST_VALUE(x ORDER BY x) FROM tbl",
            "SELECT LAST_VALUE(x ORDER BY x, y) AS a FROM T",
            "SELECT LAST_VALUE(x ORDER BY x ASC, y DESC) AS a FROM T"
        };

        foreach (var sql in queries)
        {
            VerifiedStatement(sql, dialects);
        }
    }

    [Fact]
    public void Parse_Create_Type()
    {
        var createType = VerifiedStatement("CREATE TYPE db.type_name AS (foo INT, bar TEXT COLLATE \"de_DE\")");

        var attributes = new Sequence<UserDefinedTypeCompositeAttributeDef>
        {
            new("foo", new Int()),
            new("bar", new Text(), new ObjectName(new Ident("de_DE", Symbols.DoubleQuote)))
        };

        var expected = new Statement.CreateType(new ObjectName(["db", "type_name"]),
            new UserDefinedTypeRepresentation.Composite(attributes));

        Assert.Equal(expected, createType);
    }

    [Fact]
    public void Parse_Alter_View()
    {
        const string sql = "ALTER VIEW myschema.myview AS SELECT foo FROM bar";

        var alter = (Statement.AlterView)VerifiedStatement(sql);

        Assert.Equal("myschema.myview", alter.Name);
        Assert.Empty(alter.Columns);
        Assert.Equal("SELECT foo FROM bar", alter.Query.ToSql());
        Assert.Empty(alter.WithOptions);
    }

    [Fact]
    public void Parse_Json_Ops_Without_Colon()
    {
        var pgAndGeneric = new Dialect[] { new PostgreSqlDialect(), new GenericDialect() };
        var operators = new List<Tuple<string, BinaryOperator, IEnumerable<Dialect>>>
        {
            new("->", BinaryOperator.Arrow, AllDialects.Where(d => d is not DatabricksDialect)),
            new("->>", BinaryOperator.LongArrow, AllDialects),
            new("#>", BinaryOperator.HashArrow, pgAndGeneric),
            new("#>>", BinaryOperator.HashLongArrow, pgAndGeneric),
            new("@>", BinaryOperator.AtArrow, AllDialects),
            new("<@", BinaryOperator.ArrowAt, AllDialects),
            new("#-", BinaryOperator.HashMinus, pgAndGeneric),
            new("@?", BinaryOperator.AtQuestion, AllDialects),
            new("@@", BinaryOperator.AtAt, AllDialects)
        };

        foreach (var (symbol, op, dialects) in operators)
        {
            var select = VerifiedOnlySelect($"SELECT a {symbol} b", dialects);

            var expected = new SelectItem.UnnamedExpression(new BinaryOp(
                new Identifier("a"),
                op,
                new Identifier("b")
            ));

            Assert.Equal(expected, select.Projection.First());
        }
    }

    [Fact]
    public void Parse_Create_Table_With_Alias()
    {
        const string sql = """
                           CREATE TABLE public.datatype_aliases
                           (
                               int8_col INT8,
                               int4_col INT4,
                               int2_col INT2,
                               float8_col FLOAT8,
                               float4_col FLOAT4,
                               bool_col BOOL
                           );
                           """;

        var statement =
            (Statement.CreateTable)OneStatementParsesTo(sql, "", [new GenericDialect(), new PostgreSqlDialect()]);
        var element = statement.Element;
        var expectedColumns = new Sequence<ColumnDef>
        {
            new("int8_col", new Int8()),
            new("int4_col", new Int4()),
            new("int2_col", new Int2()),
            new("float8_col", new Float8()),
            new("float4_col", new Float4()),
            new("bool_col", new Bool()),
        };

        Assert.Equal("public.datatype_aliases", element.Name);
        Assert.Equal(expectedColumns, element.Columns);
    }

    [Fact]
    public void Parse_Select_Group_By_All()
    {
        const string sql = "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL";
        var select = VerifiedOnlySelect(sql);

        Assert.Equal(new GroupByExpression.All(), select.GroupBy);

        OneStatementParsesTo(
            "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL",
            "SELECT id, fname, lname, SUM(order) FROM customer GROUP BY ALL"
        );
    }

    [Fact]
    public void Parse_Create_Table_Collate()
    {
        VerifiedStatement("CREATE TABLE tbl (foo INT, bar TEXT COLLATE \"de_DE\")",
            [new PostgreSqlDialect(), new GenericDialect()]);
    }

    [Fact]
    public void Parse_Window_Rank_Function()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(),
            new PostgreSqlDialect(),
            new MsSqlDialect(),
            new AnsiDialect(),
            new HiveDialect(),
            new SnowflakeDialect()
        };

        var queries = new[]
        {
            "SELECT column1, column2, FIRST_VALUE(column2) OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
            "SELECT column1, column2, FIRST_VALUE(column2) OVER (ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
            "SELECT col_1, col_2, LAG(col_2) OVER (ORDER BY col_1) FROM t1",
            "SELECT LAG(col_2, 1, 0) OVER (ORDER BY col_1) FROM t1",
            "SELECT LAG(col_2, 1, 0) OVER (PARTITION BY col_3 ORDER BY col_1)"
        };

        foreach (var sql in queries)
        {
            VerifiedStatement(sql, dialects);
        }

        var nullDialects = new Dialect[]
        {
            new MsSqlDialect(),
            new SnowflakeDialect()
        };

        queries =
        [
            "SELECT column1, column2, FIRST_VALUE(column2) IGNORE NULLS OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
            //"SELECT column1, column2, FIRST_VALUE(column2) RESPECT NULLS OVER (PARTITION BY column1 ORDER BY column2 NULLS LAST) AS column2_first FROM t1",
            //"SELECT LAG(col_2, 1, 0) IGNORE NULLS OVER (ORDER BY col_1) FROM t1",
            //"SELECT LAG(col_2, 1, 0) RESPECT NULLS OVER (ORDER BY col_1) FROM t1"
        ];

        foreach (var sql in queries)
        {
            VerifiedStatement(sql, nullDialects);
        }
    }

    [Fact]
    public void Parse_Binary_Operators_Without_Whitespace()
    {
        OneStatementParsesTo("SELECT field+1000 FROM tbl1", "SELECT field + 1000 FROM tbl1");
        OneStatementParsesTo(
            "SELECT tbl1.field+tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
            "SELECT tbl1.field + tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id");

        OneStatementParsesTo(
            "SELECT field-1000 FROM tbl1",
            "SELECT field - 1000 FROM tbl1");

        OneStatementParsesTo(
            "SELECT tbl1.field-tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
            "SELECT tbl1.field - tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id");

        OneStatementParsesTo(
            "SELECT field*1000 FROM tbl1",
            "SELECT field * 1000 FROM tbl1");

        OneStatementParsesTo(
            "SELECT tbl1.field*tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
            "SELECT tbl1.field * tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id"
        );

        // x / y
        OneStatementParsesTo(
            "SELECT field/1000 FROM tbl1",
            "SELECT field / 1000 FROM tbl1"
        );

        OneStatementParsesTo(
            "SELECT tbl1.field/tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
            "SELECT tbl1.field / tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id"
        );

        // x % y
        OneStatementParsesTo(
            "SELECT field%1000 FROM tbl1",
            "SELECT field % 1000 FROM tbl1"
        );

        OneStatementParsesTo(
            "SELECT tbl1.field%tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id",
            "SELECT tbl1.field % tbl2.field FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.entity_id"
        );
    }

    [Fact]
    public void Test_Savepoint()
    {
        var release = (Statement.Savepoint)VerifiedStatement("SAVEPOINT test1");

        var expected = new Statement.Savepoint("test1");
        Assert.Equal(expected, release);
    }

    [Fact]
    public void Test_Release_Savepoint()
    {
        var release = (Statement.ReleaseSavepoint)VerifiedStatement("RELEASE SAVEPOINT test1");

        var expected = new Statement.ReleaseSavepoint("test1");
        Assert.Equal(expected, release);
    }

    [Fact]
    public void Parse_Insert_Default_Values()
    {
        var insert = (Statement.Insert)VerifiedStatement("INSERT INTO test_table DEFAULT VALUES");
        var expected = new Statement.Insert(new InsertOperation("test_table", null) { Into = true });
        Assert.Equal(expected, insert);

        insert = (Statement.Insert)VerifiedStatement("INSERT INTO test_table DEFAULT VALUES RETURNING test_column");
        Assert.NotEmpty(insert.InsertOperation.Returning!);

        insert = (Statement.Insert)VerifiedStatement("INSERT INTO test_table DEFAULT VALUES ON CONFLICT DO NOTHING");
        Assert.NotNull(insert.InsertOperation.On);

        Assert.Throws<ParserException>(() => ParseSqlStatements("INSERT INTO test_table (test_col) DEFAULT VALUES"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("INSERT INTO test_table DEFAULT VALUES (some_column)"));
        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("INSERT INTO test_table DEFAULT VALUES PARTITION (some_column)"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("INSERT INTO test_table DEFAULT VALUES (1)"));
    }

    [Fact]
    public void Parse_End()
    {
        OneStatementParsesTo("END AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("END WORK AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("END TRANSACTION AND NO CHAIN", "COMMIT");
        OneStatementParsesTo("END WORK AND CHAIN", "COMMIT AND CHAIN");
        OneStatementParsesTo("END TRANSACTION AND CHAIN", "COMMIT AND CHAIN");
        OneStatementParsesTo("END WORK", "COMMIT");
        OneStatementParsesTo("END TRANSACTION", "COMMIT");
    }

    [Fact]
    public void Parse_Call()
    {
        VerifiedStatement("CALL my_procedure()");
        VerifiedStatement("CALL my_procedure(1, 'a')");
        VerifiedStatement("CALL my_procedure(1, 'a', $1)", [new PostgreSqlDialect(), new GenericDialect()]);
        VerifiedStatement("CALL my_procedure");

        var call = (Statement.Call)VerifiedStatement("CALL my_procedure('a')");
        var expected = new Statement.Call(new Function("my_procedure")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(
                        new LiteralValue(
                            new Value.SingleQuotedString("a"))))
            ]))
        });

        Assert.Equal(expected, call);
    }

    [Fact]
    public void Parse_Replace_Into()
    {
        var dialects = new[] { new PostgreSqlDialect() };

        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("REPLACE INTO public.customer (id, name, active) VALUES (1, 2, 3)", dialects: dialects));
    }

    [Fact]
    public void Parse_Insert_Select_Returning()
    {
        var statement = VerifiedStatement("INSERT INTO t SELECT x RETURNING x AS y");

        Assert.Single((statement as Statement.Insert)!.InsertOperation.Returning!);
    }

    [Fact]
    public void Parse_Returning_As_Column_Alias()
    {
        VerifiedStatement("SELECT 1 AS RETURNING");
    }

    [Fact]
    public void Parse_Json_Table_Is_Not_Reserve()
    {
        var dialects = new Dialect[] { new PostgreSqlDialect(), new GenericDialect() };
        var select = VerifiedOnlySelect("SELECT * FROM JSON_TABLE", dialects);

        Assert.Single(select.From!);
        Assert.Equal("JSON_TABLE", select.From![0].Relation!.AsTable().Name);
    }

    [Fact]
    public void Parse_Unload()
    {
        var unload =
            (Statement.Unload)VerifiedStatement("UNLOAD(SELECT cola FROM tab) TO 's3://...' WITH (format = 'AVRO')");

        var query = new Query(
            new SetExpression.SelectExpression(new Select(
                [new SelectItem.UnnamedExpression(new Identifier("cola"))])
            {
                From =
                [
                    new TableWithJoins(new TableFactor.Table("tab"))
                ]
            }));

        var to = new Ident("s3://...", Symbols.SingleQuote);

        var with = new Sequence<SqlOption>
        {
            new SqlOption.KeyValue("format", new LiteralValue(new Value.SingleQuotedString("AVRO")))
        };

        var expected = new Statement.Unload(query, to, with);

        Assert.Equal(expected, unload);
    }

    [Fact]
    public void Test_Comment_Hash_Syntax()
    {
        List<Dialect> dialects = [new BigQueryDialect(), new SnowflakeDialect()];

        const string sql = """
                           # comment
                           SELECT a, b, c # , d, e
                           FROM T
                           ####### comment #################
                           WHERE true
                           # comment
                           """;

        const string canonical = "SELECT a, b, c FROM T WHERE true";
        VerifiedOnlySelectWithCanonical(sql, canonical, dialects);
    }

    [Fact]
    public void Parse_Map_Access_Expr()
    {
        IEnumerable<Dialect> dialects = [new BigQueryDialect(), new ClickHouseDialect()];

        var expr = VerifiedExpr("users[-1][safe_offset(2)]", dialects);

        var expected = new MapAccess(
            new Identifier("users"),
            [
                new MapAccessKey(new UnaryOp(new LiteralValue(new Value.Number("1")), UnaryOperator.Minus),
                    MapAccessSyntax.Bracket),
                new MapAccessKey(new Function("safe_offset")
                {
                    Args = new FunctionArguments.List(new FunctionArgumentList([
                        new FunctionArg.Unnamed(
                            new FunctionArgExpression.FunctionExpression(new LiteralValue(new Value.Number("2"))))
                    ]))
                }, MapAccessSyntax.Bracket)
            ]
        );

        Assert.Equal(expected, expr);

        VerifiedExpr("users[1]");
        VerifiedExpr("a[array_length(b) - 1 + 2][c + 3][d * 4]");
    }

    [Fact]
    public void Test_Select_Wildcard_With_Replace()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(), new BigQueryDialect(), new ClickHouseDialect(), new SnowflakeDialect(),
            new DuckDbDialect()
        };

        var select = VerifiedOnlySelect("SELECT * REPLACE ('widget' AS item_name) FROM orders", dialects);
        var expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ReplaceOption = new ReplaceSelectItem(new ReplaceSelectElement[]
            {
                new(new LiteralValue(new Value.SingleQuotedString("widget")), "item_name", true)
            })
        });

        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT * REPLACE (quantity / 2 AS quantity, 3 AS order_id) FROM orders", dialects);
        expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ReplaceOption = new ReplaceSelectItem(new ReplaceSelectElement[]
            {
                new(new BinaryOp(
                    new Identifier("quantity"),
                    BinaryOperator.Divide,
                    new LiteralValue(Number("2"))
                ), "quantity", true),

                new(new LiteralValue(Number("3")), "order_id", true)
            })
        });

        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Parse_Select_Star_Replace()
    {
        var dialects = new Dialect[]
        {
            new GenericDialect(), new BigQueryDialect(), new ClickHouseDialect(), new SnowflakeDialect(),
            new DuckDbDialect()
        };

        VerifiedStatement("SELECT * REPLACE (i + 1 AS i) FROM columns_transformers", dialects);
    }

    [Fact]
    public void Parse_Like()
    {
        Test(true);
        Test(false);
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : null;

            var sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a'";
            var select = VerifiedOnlySelect(sql);
            var expected = new Like(new Identifier("name"), negated,
                new LiteralValue(new Value.SingleQuotedString("%a")));
            Assert.Equal(expected, select.Selection);

            // Test with escape char
            sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a' ESCAPE '^'";
            select = VerifiedOnlySelect(sql);
            expected = new Like(new Identifier("name"), negated, new LiteralValue(new Value.SingleQuotedString("%a")))
            {
                EscapeChar = '^'.ToString()
            };
            Assert.Equal(expected, select.Selection);

            // This statement tests that LIKE and NOT LIKE have the same precedence.
            // This was previously mishandled (#81).
            sql = $"SELECT * FROM customers WHERE name {negation}LIKE '%a' IS NULL";
            select = VerifiedOnlySelect(sql);
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
        return;

        void Test(bool negated)
        {
            var negation = negated ? "NOT " : null;

            var sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a'";

            var select = VerifiedOnlySelect(sql);
            var expected = new SimilarTo(
                new Identifier("name"),
                negated,
                new LiteralValue(new Value.SingleQuotedString("%a")));
            Assert.Equal(expected, select.Selection);


            // Test with escape char
            sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '^'";
            select = VerifiedOnlySelect(sql);
            expected = new SimilarTo(
                new Identifier("name"),
                negated,
                new LiteralValue(new Value.SingleQuotedString("%a")),
                '^');
            Assert.Equal(expected, select.Selection);


            // This statement tests that SIMILAR TO and NOT SIMILAR TO have the same precedence.
            sql = $"SELECT * FROM customers WHERE name {negation}SIMILAR TO '%a' ESCAPE '^' IS NULL";
            select = VerifiedOnlySelect(sql);
            var isNull = new IsNull(new SimilarTo(
                new Identifier("name"),
                negated,
                new LiteralValue(new Value.SingleQuotedString("%a")),
                '^'));
            Assert.Equal(isNull, select.Selection);
        }
    }

    [Fact]
    public void Test_Match_Recognize()
    {
        var table = new TableFactor.Table("my_table");

        var dialects = AllDialects.Where(d => d.SupportsMatchRecognize).ToList();

        var expected = new TableFactor.MatchRecognize(
            table,
            [new Identifier("company")],
            [new(new Identifier("price_date"))],
            [
                new(Call("MATCH_NUMBER", []), new Ident("match_number")),
                new(Call("FIRST", [new Identifier("price_date")]), "start_date"),
                new(Call("LAST", [new Identifier("price_date")]), new Ident("end_date"))
            ],
            new RowsPerMatch.OneRow(),
            new AfterMatchSkip.ToLast("row_with_price_increase"),
            new MatchRecognizePattern.Concat([
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("row_before_decrease")),

                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("row_with_price_decrease")),
                    new RepetitionQualifier.OneOrMore()),

                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("row_with_price_increase")),
                    new RepetitionQualifier.OneOrMore())
            ]),
            new Sequence<SymbolDefinition>([
                new SymbolDefinition("row_with_price_decrease", new BinaryOp(
                    new Identifier("price"),
                    BinaryOperator.Lt,
                    Call("LAG", [new Identifier("price")])
                )),
                new SymbolDefinition("row_with_price_increase", new BinaryOp(
                    new Identifier("price"),
                    BinaryOperator.Gt,
                    Call("LAG", [new Identifier("price")])
                ))
            ]),
            null
        );

        var options = string.Join(" ", [
            "PARTITION BY company",
            "ORDER BY price_date",
            "MEASURES",
            "MATCH_NUMBER() AS match_number,",
            "FIRST(price_date) AS start_date,",
            "LAST(price_date) AS end_date",
            "ONE ROW PER MATCH",
            "AFTER MATCH SKIP TO LAST row_with_price_increase",
            "PATTERN (row_before_decrease row_with_price_decrease+ row_with_price_increase+)",
            "DEFINE",
            "row_with_price_decrease AS price < LAG(price),",
            "row_with_price_increase AS price > LAG(price)"
        ]);

        var select = VerifiedOnlySelect($"SELECT * FROM my_table MATCH_RECOGNIZE({options})", dialects);

        Assert.Equal(select.From![0].Relation, expected);
        return;

        static Expression Call(string function, IList<Expression> args)
        {
            Sequence<FunctionArg>? functionArgs = null;

            if (args.Any())
            {
                functionArgs = new Sequence<FunctionArg>(args.Select(arg =>
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(arg))));
            }

            return new Function(new ObjectName(new Ident(function)))
            {
                Args = new FunctionArguments.List(new FunctionArgumentList(functionArgs!))
            };
        }
    }

    [Fact]
    public void Test_Match_Recognize_Patterns()
    {
        var dialects = AllDialects.Where(d => d.SupportsMatchRecognize).ToList();

        Check("FOO", new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("FOO")));

        Check("^ FOO $", new MatchRecognizePattern.Concat([
            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Start()),
            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("FOO")),
            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.End())
        ]));

        // exclusion
        Check("{- FOO -}", new MatchRecognizePattern.Exclude(new MatchRecognizeSymbol.Named("FOO")));

        Check("PERMUTE(A, B, C)",
            new MatchRecognizePattern.Permute([
                new MatchRecognizeSymbol.Named("A"),
                new MatchRecognizeSymbol.Named("B"),
                new MatchRecognizeSymbol.Named("C"),
            ]));

        // various identifiers
        Check(
            "FOO | \"BAR\" | baz42",
            new MatchRecognizePattern.Alternation([
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("FOO")),
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named(new Ident("BAR", Symbols.DoubleQuote))),
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("baz42"))

            ]));

        // concatenated basic quantifiers
        Check("S1* S2+ S3?",
            new MatchRecognizePattern.Concat([
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S1")),
                    new RepetitionQualifier.ZeroOrMore()),
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S2")),
                    new RepetitionQualifier.OneOrMore()),
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S3")),
                    new RepetitionQualifier.AtMostOne())
            ]));

        // double repetition
        Check("S2*?", new MatchRecognizePattern.Repetition(
            new MatchRecognizePattern.Repetition(
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S2")),
                new RepetitionQualifier.ZeroOrMore()),
            new RepetitionQualifier.AtMostOne()));

        // range quantifiers in an alternation
        Check("S1{1} | S2{2,3} | S3{4,} | S4{,5}",
            new MatchRecognizePattern.Alternation([
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S1")),
                    new RepetitionQualifier.Exactly(1)),
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S2")),
                    new RepetitionQualifier.Range(2, 3)),
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S3")),
                    new RepetitionQualifier.AtLeast(4)),
                new MatchRecognizePattern.Repetition(
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S4")),
                    new RepetitionQualifier.AtMost(5)),
            ]));

        // grouping case 1
        Check("S1 ( S2 )", new MatchRecognizePattern.Concat([
            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S1")),
            new MatchRecognizePattern.Group(new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S2")))
        ]));

        // grouping case 2
        Check("( {- S3 -} S4 )+", new MatchRecognizePattern.Repetition(
            new MatchRecognizePattern.Group(new MatchRecognizePattern.Concat([
                new MatchRecognizePattern.Exclude(new MatchRecognizeSymbol.Named("S3")),
                new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S4"))
            ])),
            new RepetitionQualifier.OneOrMore()));

        Check("^ S1 S2*? ( {- S3 -} S4 )+ | PERMUTE(S1, S2){1,2} $",
            new MatchRecognizePattern.Alternation([
                new MatchRecognizePattern.Concat([
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Start()),
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S1")),
                    new MatchRecognizePattern.Repetition(
                        new MatchRecognizePattern.Repetition(
                            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S2")),
                            new RepetitionQualifier.ZeroOrMore()
                        ),
                        new RepetitionQualifier.AtMostOne()
                    ),
                    new MatchRecognizePattern.Repetition(
                        new MatchRecognizePattern.Group(new MatchRecognizePattern.Concat([
                            new MatchRecognizePattern.Exclude(new MatchRecognizeSymbol.Named("S3")),
                            new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Named("S4")),
                        ])),
                        new RepetitionQualifier.OneOrMore()
                    )
                ]),
                new MatchRecognizePattern.Concat([
                    new MatchRecognizePattern.Repetition(
                        new MatchRecognizePattern.Permute([
                            new MatchRecognizeSymbol.Named("S1"),
                            new MatchRecognizeSymbol.Named("S2"),
                        ]),
                        new RepetitionQualifier.Range(1, 2)
                    ),
                    new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.End())
                ])
            ]));
        return;

        void Check(string pattern, MatchRecognizePattern expected)
        {
            var select =
                VerifiedOnlySelect($"SELECT * FROM my_table MATCH_RECOGNIZE(PATTERN ({pattern}) DEFINE DUMMY AS true)",
                    dialects);

            var actual = ((TableFactor.MatchRecognize)select.From![0].Relation!).Pattern;
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public void Parse_Size_List()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect(), new DuckDbDialect()];

        VerifiedStatement("CREATE TABLE embeddings (data FLOAT[1536])");
        VerifiedStatement("CREATE TABLE embeddings (data FLOAT[1536][3])");
        VerifiedStatement("SELECT data::FLOAT[1536] FROM embeddings");
    }

    [Fact]
    public void Insert_Into_With_Parentheses()
    {
        DefaultDialects = [new SnowflakeDialect(), new RedshiftDialect(), new GenericDialect()];

        VerifiedStatement("INSERT INTO t1 (id, name) (SELECT t2.id, t2.name FROM t2)");
    }

    [Fact]
    public void Test_Selective_Aggregation()
    {
        const string sql = """
                           SELECT
                            ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL),
                            ARRAY_AGG(name) FILTER (WHERE name LIKE 'a%') AS agg2
                            FROM region
                           """;

        DefaultDialects = AllDialects.Where(d => d.SupportsFilterDuringAggregation);

        var select = VerifiedOnlySelect(sql);

        var expected = new Sequence<SelectItem>
        {
            new SelectItem.UnnamedExpression(new Function("ARRAY_AGG")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                        new Identifier("name")
                    ))
                ])),
                Filter = new IsNotNull(new Identifier("name"))
            }),
            new SelectItem.ExpressionWithAlias(new Function("ARRAY_AGG")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(
                        new Identifier("name")
                    ))
                ])),
                Filter = new Like(new Identifier("name"), false, new LiteralValue(new Value.SingleQuotedString("a%")))
            }, "agg2"),
        };

        Assert.Equal(expected, select.Projection);
    }

    [Fact]
    public void Test_Group_By_Grouping_Sets()
    {
        const string sql = """
                           SELECT city, car_model, sum(quantity) AS sum
                            FROM dealer
                            GROUP BY GROUPING SETS ((city, car_model), (city), (car_model), ())
                            ORDER BY city
                           """;

        DefaultDialects = AllDialects.Where(d => d.SupportsGroupByExpression);

        var select = VerifiedOnlySelect(sql);

        var expected = new GroupByExpression.Expressions([
            new GroupingSets([
                [new Identifier("city"), new Identifier("car_model")],
                [new Identifier("city")],
                [new Identifier("car_model")],
                []
            ])
        ]);

        Assert.Equal(expected, select.GroupBy);
    }


    [Fact]
    public void Filter_During_Aggregation()
    {
        DefaultDialects = AllDialects.Where(d => d.SupportsFilterDuringAggregation);

        var sql = """
                  SELECT
                   ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL),
                   ARRAY_AGG(name) FILTER (WHERE name LIKE 'a%')
                   FROM region
                  """;
        if (sql == null) throw new ArgumentNullException(nameof(sql));
        VerifiedStatement(sql);
    }

    [Fact]
    public void Filter_During_Aggregation_Aliased()
    {
        DefaultDialects = AllDialects.Where(d => d.SupportsFilterDuringAggregation);

        const string sql = """
                           SELECT
                            ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL) AS agg1,
                            ARRAY_AGG(name) FILTER (WHERE name LIKE 'a%') AS agg2
                            FROM region
                           """;
        VerifiedStatement(sql);
    }

    [Fact]
    public void Test_Dictionary_Syntax()
    {
        var dialects = AllDialects.Where(d => d.SupportsDictionarySyntax).ToList();

        var expression = new Dictionary([
            new DictionaryField(new Ident("Alberta", Symbols.SingleQuote),
                new LiteralValue(new Value.SingleQuotedString("Edmonton"))),
            new DictionaryField(new Ident("Manitoba", Symbols.SingleQuote),
                new LiteralValue(new Value.SingleQuotedString("Winnipeg")))
        ]);
        Check("{'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg'}", expression);


        expression = new Dictionary([
            new DictionaryField(new Ident("start", Symbols.SingleQuote),
                new Cast(new LiteralValue(new Value.SingleQuotedString("2023-04-01")), new Timestamp(TimezoneInfo.None),
                    CastKind.Cast)),

            new DictionaryField(new Ident("end", Symbols.SingleQuote),
                new Cast(new LiteralValue(new Value.SingleQuotedString("2023-04-05")), new Timestamp(TimezoneInfo.None),
                    CastKind.Cast)),
        ]);
        Check("{'start': CAST('2023-04-01' AS TIMESTAMP), 'end': CAST('2023-04-05' AS TIMESTAMP)}", expression);

        return;

        void Check(string sql, Expression expected)
        {
            Assert.Equal(expected, VerifiedExpr(sql, dialects));
        }
    }

    [Fact]
    public void Parse_Connect_By()
    {
        var dialects = AllDialects.Where(d => d.SupportsConnectBy).ToList();

        var expected = new Select([
            new SelectItem.UnnamedExpression(new Identifier("employee_id")),
            new SelectItem.UnnamedExpression(new Identifier("manager_id")),
            new SelectItem.UnnamedExpression(new Identifier("title"))
        ])
        {
            From = [new TableWithJoins(new TableFactor.Table("employees"))],
            ConnectBy = new ConnectBy(
                new BinaryOp(
                    new Identifier("title"),
                    BinaryOperator.Eq,
                    new LiteralValue(new Value.SingleQuotedString("president"))
                ),
                [
                    new BinaryOp(
                        new Identifier("manager_id"),
                        BinaryOperator.Eq,
                        new Prior(new Identifier("employee_id"))
                    )
                ])
        };

        var connect1 = """
                       SELECT employee_id, manager_id, title FROM employees
                        START WITH title = 'president'
                        CONNECT BY manager_id = PRIOR employee_id
                        ORDER BY employee_id
                       """;

        Assert.Equal(expected, VerifiedOnlySelect(connect1, dialects));

        var connect2 = """
                       SELECT employee_id, manager_id, title FROM employees
                        CONNECT BY manager_id = PRIOR employee_id
                        START WITH title = 'president'
                        ORDER BY employee_id
                       """;

        Assert.Equal(expected, VerifiedOnlySelectWithCanonical(connect2, connect1, dialects));

        var connect3 = """
                       SELECT employee_id, manager_id, title FROM employees
                        WHERE employee_id <> 42
                        START WITH title = 'president'
                        CONNECT BY manager_id = PRIOR employee_id
                        ORDER BY employee_id
                       """;

        expected = new Select([
            new SelectItem.UnnamedExpression(new Identifier("employee_id")),
            new SelectItem.UnnamedExpression(new Identifier("manager_id")),
            new SelectItem.UnnamedExpression(new Identifier("title"))
        ])
        {
            From = [new TableWithJoins(new TableFactor.Table("employees"))],
            Selection = new BinaryOp(
                new Identifier("employee_id"),
                BinaryOperator.NotEq,
                new LiteralValue(new Value.Number("42"))
            ),
            ConnectBy = new ConnectBy(
                new BinaryOp(
                    new Identifier("title"),
                    BinaryOperator.Eq,
                    new LiteralValue(new Value.SingleQuotedString("president"))
                ),
                [
                    new BinaryOp(
                        new Identifier("manager_id"),
                        BinaryOperator.Eq,
                        new Prior(new Identifier("employee_id"))
                    )
                ])
        };

        Assert.Equal(expected, VerifiedOnlySelect(connect3, dialects));
    }

    [Fact]
    public void Tests_Select_Values_Without_Parens()
    {
        var dialects = new Dialect[] { new GenericDialect(), new SnowflakeDialect(), new DatabricksDialect() };

        const string sql = "SELECT * FROM VALUES (1, 2), (2,3) AS tbl (id, val)";
        const string canonical = "SELECT * FROM (VALUES (1, 2), (2, 3)) AS tbl (id, val)";

        VerifiedOnlySelectWithCanonical(sql, canonical, dialects);
    }

    [Fact]
    public void Tests_Select_Values_Without_Parens_And_Set_Op()
    {
        var dialects = new Dialect[] { new GenericDialect(), new SnowflakeDialect(), new DatabricksDialect() };

        const string sql =
            "SELECT id + 1, name FROM VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Orange') AS fruits (id, name) UNION ALL SELECT 5, 'Strawberry'";
        const string canonical =
            "SELECT id + 1, name FROM (VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Orange')) AS fruits (id, name) UNION ALL SELECT 5, 'Strawberry'";

        var query = VerifiedQueryWithCanonical(sql, canonical, dialects);

        Assert.Equal(SetOperator.Union, ((SetExpression.SetOperation)query.Body).Op);
    }

    [Fact]
    public void Parse_Select_Wildcard_With_Except()
    {
        var dialects = AllDialects.Where(d => d.SupportsSelectWildcardExcept).ToList();

        var select = VerifiedOnlySelect("SELECT * EXCEPT (col_a) FROM data", dialects);

        var expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExceptOption = new ExceptSelectItem("col_a", [])
        });
        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT * EXCEPT (department_id, employee_id) FROM employee_table", dialects);

        expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExceptOption = new ExceptSelectItem("department_id", ["employee_id"])
        });
        Assert.Equal(expected, select.Projection[0]);


        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * EXCEPT () FROM employee_table", dialects));
    }

    [Fact]
    public void Parse_Trailing_Comma()
    {
        // At the moment, DuckDB is the only dialect that allows
        // trailing commas anywhere in the query
        DefaultDialects = new[] { new DuckDbDialect() };

        OneStatementParsesTo("SELECT album_id, name, FROM track", "SELECT album_id, name FROM track", DefaultDialects);
        OneStatementParsesTo("SELECT * FROM track ORDER BY milliseconds,", "SELECT * FROM track ORDER BY milliseconds",
            DefaultDialects);
        OneStatementParsesTo("SELECT DISTINCT ON (album_id,) name FROM track",
            "SELECT DISTINCT ON (album_id) name FROM track", DefaultDialects);
        OneStatementParsesTo("CREATE TABLE employees (name text, age int,)",
            "CREATE TABLE employees (name TEXT, age INT)", DefaultDialects);
        OneStatementParsesTo("GRANT USAGE, SELECT, INSERT, ON p TO u", "GRANT USAGE, SELECT, INSERT ON p TO u",
            DefaultDialects);
        VerifiedStatement("SELECT album_id, name FROM track", DefaultDialects);
        VerifiedStatement("SELECT * FROM track ORDER BY milliseconds", DefaultDialects);
        VerifiedStatement("SELECT DISTINCT ON (album_id) name FROM track", DefaultDialects);
        OneStatementParsesTo("SELECT \"from\", FROM \"from\"", "SELECT \"from\" FROM \"from\"", DefaultDialects);

        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT name, age, from employees;", new List<Dialect> { new GenericDialect() }));
    }

    [Fact]
    public void Test_Map_Syntax()
    {
        Check("MAP {'Alberta': 'Edmonton', 'Manitoba': 'Winnipeg'}", new Expression.Map(new Map([
            new(new LiteralValue(new Value.SingleQuotedString("Alberta")),
                new LiteralValue(new Value.SingleQuotedString("Edmonton"))),
            new(new LiteralValue(new Value.SingleQuotedString("Manitoba")),
                new LiteralValue(new Value.SingleQuotedString("Winnipeg")))
        ])));

        Check("MAP {1: 10.0, 2: 20.0}", new Expression.Map(new Map([
            new(new LiteralValue(new Value.Number("1")), new LiteralValue(new Value.Number("10.0"))),
            new(new LiteralValue(new Value.Number("2")), new LiteralValue(new Value.Number("20.0")))
        ])));

        Check("MAP {[1, 2, 3]: 10.0, [4, 5, 6]: 20.0}", new Expression.Map(new Map([
            new(new Expression.Array(new ArrayExpression([
                new LiteralValue(new Value.Number("1")),
                new LiteralValue(new Value.Number("2")),
                new LiteralValue(new Value.Number("3"))
            ])), new LiteralValue(new Value.Number("10.0"))),

            new(new Expression.Array(new ArrayExpression([
                new LiteralValue(new Value.Number("4")),
                new LiteralValue(new Value.Number("5")),
                new LiteralValue(new Value.Number("6"))
            ])), new LiteralValue(new Value.Number("20.0")))
        ])));

        Check("MAP {'a': 10, 'b': 20}['a']", new Expression.Subscript(
            new Expression.Map(new Map([
                new(new LiteralValue(new Value.SingleQuotedString("a")), new LiteralValue(new Value.Number("10"))),
                new(new LiteralValue(new Value.SingleQuotedString("b")), new LiteralValue(new Value.Number("20")))
            ])),
            new Subscript.Index(new LiteralValue(new Value.SingleQuotedString("a")))));

        return;

        void Check(string sql, Expression expected)
        {
            var dialects = AllDialects.Where(d => d.SupportMapLiteralSyntax);
            Assert.Equal(expected, VerifiedExpr(sql, dialects));
        }
    }

    [Fact]
    public void Test_Group_By_Nothing()
    {
        var dialects = AllDialects.Where(d => d.SupportsGroupByExpression).ToList();

        var select = VerifiedOnlySelect("SELECT count(1) FROM t GROUP BY ()", dialects);
        var expected = new GroupByExpression.Expressions([new Expression.Tuple([])]);
        Assert.Equal(expected, select.GroupBy);

        select = VerifiedOnlySelect("SELECT name, count(1) FROM t GROUP BY name, ()", dialects);
        expected = new GroupByExpression.Expressions([
            new Identifier("name"),
            new Expression.Tuple([])
        ]);
        Assert.Equal(expected, select.GroupBy);
    }

    [Fact]
    public void Test_Alter_Table_With_On_Cluster()
    {
        var alter = VerifiedStatement<Statement.AlterTable>(
            "ALTER TABLE t ON CLUSTER 'cluster' ADD CONSTRAINT bar PRIMARY KEY (baz)");

        Assert.Equal("t", alter.Name);
        Assert.Equal(new Ident("cluster", Symbols.SingleQuote), alter.OnCluster);

        alter = VerifiedStatement<Statement.AlterTable>(
            "ALTER TABLE t ON CLUSTER cluster_name ADD CONSTRAINT bar PRIMARY KEY (baz)");

        Assert.Equal("t", alter.Name);
        Assert.Equal(new Ident("cluster_name"), alter.OnCluster);

        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("ALTER TABLE t ON CLUSTER 123 ADD CONSTRAINT bar PRIMARY KEY (baz)"));
    }

    [Fact]
    public void Test_Extract_Seconds_Ok()
    {
        var dialects = AllDialects.Where(d => d.AllowExtractCustom).ToList();
        var extract = VerifiedExpr("EXTRACT(seconds FROM '2 seconds'::INTERVAL)", dialects);

        var expected = new Extract(
            new Cast(
                new LiteralValue(new Value.SingleQuotedString("2 seconds")),
                new DataType.Interval(), CastKind.DoubleColon),
            new DateTimeField.Custom("seconds"),
            ExtractSyntax.From);

        Assert.Equal(expected, extract);
    }

    [Fact]
    public void Test_Extract_Seconds_Err()
    {
        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT EXTRACT(seconds FROM '2 seconds'::INTERVAL)",
            AllDialects.Where(d => !d.AllowExtractCustom)));
    }

    [Fact]
    public void Test_Extract_Seconds_Single_Quote_Err()
    {
        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT EXTRACT('seconds' FROM '2 seconds'::INTERVAL)",
            AllDialects.Where(d => !d.AllowExtractSingleQuotes)));
    }

    [Fact]
    public void Test_Create_Index_With_With_Clause()
    {
        const string sql = "CREATE UNIQUE INDEX title_idx ON films(title) WITH (fillfactor = 70, single_param)";

        var indexedColumns = new Sequence<OrderByExpression>
        {
            new(new Identifier("title"))
        };

        var withParameters = new Sequence<Expression>
        {
            new BinaryOp(
                new Identifier("fillfactor"),
                BinaryOperator.Eq,
                new LiteralValue(new Value.Number("70"))
            ),
            new Identifier("single_param")
        };

        var dialects = AllDialects.Where(d => d.SupportsCreateIndexWithClause);

        var statement = VerifiedStatement<Statement.CreateIndex>(sql, dialects).Element;

        Assert.Equal("title_idx", statement.Name!);
        Assert.Equal("films", statement.TableName);
        Assert.Equal(indexedColumns, statement.Columns!);
        Assert.Equal(withParameters, statement.With!);
        Assert.True(statement.Unique);
        Assert.False(statement.Concurrently);
        Assert.False(statement.IfNotExists);
        Assert.Null(statement.Include);
    }

    [Fact]
    public void Parse_Deeply_Nested_Unary_Op_Hits_Recursion_Limits()
    {
        var plus = new string(Symbols.Plus, 1000);
        var sql = $"SELECT {plus}";
        Assert.Throws<ParserException>(() => ParseSqlStatements(sql));
    }

    [Fact]
    public void Parse_Literal_Integer()
    {
        var sql = "SELECT 1, -10, +20";
        var select = VerifiedOnlySelect(sql);
        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new LiteralValue(new Value.Number("1")), select.Projection[0].AsExpr());


        // negative literal is parsed as a - and expr
        Assert.Equal(new UnaryOp(new LiteralValue(new Value.Number("10")), UnaryOperator.Minus),
            select.Projection[1].AsExpr());

        // positive literal is parsed as a + and expr
        Assert.Equal(new UnaryOp(new LiteralValue(new Value.Number("20")), UnaryOperator.Plus),
            select.Projection[2].AsExpr());
    }

    [Fact]
    public void ParseNegativeValue()
    {
        OneStatementParsesTo("SELECT -1", "SELECT -1");

        OneStatementParsesTo(
            "CREATE SEQUENCE name INCREMENT -10 MINVALUE -1000 MAXVALUE 15 START -100;",
            "CREATE SEQUENCE name INCREMENT -10 MINVALUE -1000 MAXVALUE 15 START -100");
    }

    [Fact]
    public void TestTruncateTableWithOnCluster()
    {
        const string sql = "TRUNCATE TABLE t ON CLUSTER cluster_name";
        var statement = VerifiedStatement<Statement.Truncate>(sql);

        Assert.Equal(new Ident("cluster_name"), statement.OnCluster);

        VerifiedStatement("TRUNCATE TABLE t");
        Assert.Throws<ParserException>(() => ParseSqlStatements("TRUNCATE TABLE t ON CLUSTER"));
    }

    [Fact]
    public void Test_Create_Policy()
    {
        var sql = """
                  CREATE POLICY my_policy ON my_table
                   AS PERMISSIVE FOR SELECT
                   TO my_role, CURRENT_USER
                   USING (c0 = 1)
                   WITH CHECK (true)
                  """;

        var create = VerifiedStatement<Statement.CreatePolicy>(sql);

        Assert.Equal("my_policy", create.Name);
        Assert.Equal("my_table", create.TableName);
        Assert.Equal([new Owner.Identity("my_role"), new Owner.CurrentUser()], create.To);
        Assert.Equal(new BinaryOp(new Identifier("c0"), BinaryOperator.Eq, new LiteralValue(new Value.Number("1"))), create.Using);

        sql = """
              CREATE POLICY my_policy ON my_table
               AS PERMISSIVE FOR SELECT
               TO my_role, CURRENT_USER
               USING (c0 IN (SELECT column FROM t0))
               WITH CHECK (true)
              """;
        VerifiedStatement(sql);
        VerifiedStatement("CREATE POLICY my_policy ON my_table");

        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE POLICY my_policy ON my_table AS"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE POLICY my_policy ON my_table FOR"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE POLICY my_policy ON my_table TO"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE POLICY my_policy ON my_table USING"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE POLICY my_policy ON my_table WITH CHECK"));
    }

    [Fact]
    public void Parse_Drop_Policy()
    {
        var drop = VerifiedStatement<Statement.DropPolicy>("DROP POLICY IF EXISTS my_policy ON my_table RESTRICT");

        Assert.True(drop.IfExists);
        Assert.Equal("my_policy", drop.Name);
        Assert.Equal("my_table", drop.TableName);
        Assert.Equal(ReferentialAction.Restrict, drop.Option);

        VerifiedStatement("DROP POLICY my_policy ON my_table CASCADE");
        VerifiedStatement("DROP POLICY my_policy ON my_table");
        Assert.Throws<ParserException>(() => ParseSqlStatements("DROP POLICY my_policy"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("DROP POLICY my_policy ON my_table WRONG"));
    }

    [Fact]
    public void Parse_Explain_With_Option_List()
    {
        var dialects = AllDialects.Where(d => d.SupportsExplainWithUtilityOptions).ToList();

        Sequence<UtilityOption> options = [
            new("ANALYZE", new LiteralValue(new Value.Boolean(false))),
            new("VERBOSE",new LiteralValue(new Value.Boolean(true)))
        ];
        TestExplain("EXPLAIN (ANALYZE false, VERBOSE true) SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None, dialects, options);

        options = [
            new("ANALYZE", new Identifier("ON")),
            new("VERBOSE", new Identifier("OFF"))
        ];
        TestExplain( "EXPLAIN (ANALYZE ON, VERBOSE OFF) SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None, dialects, options);

        options = [
            new("FORMAT1", new Identifier("TEXT")),
            new("FORMAT2", new LiteralValue(new Value.SingleQuotedString("JSON"))),
            new("FORMAT3", new Identifier(new Ident("XML", Symbols.DoubleQuote))),
            new("FORMAT4", new Identifier("YAML"))
        ];
        TestExplain("EXPLAIN (FORMAT1 TEXT, FORMAT2 'JSON', FORMAT3 \"XML\", FORMAT4 YAML) SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None, dialects, options);

        options = [
            new("NUM1", new LiteralValue(new Value.Number("10"))),
            new("NUM2", new UnaryOp(new LiteralValue(new Value.Number("10.1")), UnaryOperator.Plus)),
            new("NUM3", new UnaryOp(new LiteralValue(new Value.Number("10.2")), UnaryOperator.Minus))
        ];
        TestExplain("EXPLAIN (NUM1 10, NUM2 +10.1, NUM3 -10.2) SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None, dialects, options);
       
        options = [
            new("ANALYZE"),
            new("VERBOSE", new LiteralValue(new Value.Boolean(true))),
            new("WAL", new Identifier("OFF")),
            new("FORMAT", new Identifier("YAML")),
            new("USER_DEF_NUM", new UnaryOp(new LiteralValue(new Value.Number("100.1")), UnaryOperator.Minus))
        ];
        TestExplain("EXPLAIN (ANALYZE, VERBOSE true, WAL OFF, FORMAT YAML, USER_DEF_NUM -100.1) SELECT sqrt(id) FROM foo", false, false, AnalyzeFormat.None, dialects, options);

    }

    [Fact]
    public void Parse_Drop_Database()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP DATABASE mycatalog.mydb");

        Assert.Equal([new([new Ident("mycatalog"), new Ident("mydb")])], drop.Names);
        Assert.Equal(ObjectType.Database, drop.ObjectType);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void Parse_Drop_Database_If_Exists()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP DATABASE IF EXISTS mydb");

        Assert.Equal(ObjectType.Database, drop.ObjectType);
        Assert.True(drop.IfExists);
    }
}