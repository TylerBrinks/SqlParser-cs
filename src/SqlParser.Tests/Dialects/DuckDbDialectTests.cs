using SqlParser.Dialects;
using SqlParser.Ast;
using static SqlParser.Ast.SetExpression;

namespace SqlParser.Tests.Dialects;

public class DuckDbDialectTests : ParserTestBase
{
    public DuckDbDialectTests()
    {
        DefaultDialects = new[] { new DuckDbDialect() };
    }

    [Fact]
    public void Test_Select_Wildcard_With_Exclude()
    {
        var select = VerifiedOnlySelect("SELECT * EXCLUDE (col_a) FROM data");
        SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(["col_a"])
        });
        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT name.* EXCLUDE department_id FROM employee_table");
        expected = new SelectItem.QualifiedWildcard(new ObjectName("name"), new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Single("department_id")
        });
        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
        expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(["department_id", "employee_id"])
        });
        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Parse_Div_Infix()
    {
        VerifiedStatement("SELECT 5 / 2", [new DuckDbDialect(), new GenericDialect()]);
    }

    [Fact]
    public void Create_Macro()
    {
        var macro = VerifiedStatement("CREATE MACRO schema.add(a, b) AS a + b");
        var expected = new Statement.CreateMacro(false, false, new ObjectName(["schema", "add"]),
            new Sequence<MacroArg> { new("a"), new("b") },
            new MacroDefinition.MacroExpression(new Expression.BinaryOp(
                new Expression.Identifier("a"),
                BinaryOperator.Plus,
                new Expression.Identifier("b"))));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Create_Macro_Default_Args()
    {
        var macro = VerifiedStatement("CREATE MACRO add_default(a, b := 5) AS a + b");

        var expected = new Statement.CreateMacro(false, false, new ObjectName(["add_default"]),
            [
                new("a"),
                new("b", new Expression.LiteralValue(new Value.Number("5")))
            ],
            new MacroDefinition.MacroExpression(new Expression.BinaryOp(
                new Expression.Identifier("a"),
                BinaryOperator.Plus,
                new Expression.Identifier("b"))));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Create_Table_Macro()
    {
        var query = "SELECT col1_value AS column1, col2_value AS column2 UNION ALL SELECT 'Hello' AS col1_value, 456 AS col2_value";
        var sql = "CREATE OR REPLACE TEMPORARY MACRO dynamic_table(col1_value, col2_value) AS TABLE " + query;

        var macro = (Statement.CreateMacro)VerifiedStatement(sql);

        var subquery = VerifiedQuery(query);
        var expected = new Statement.CreateMacro(true, true, new ObjectName(["dynamic_table"]),
            [
                new("col1_value"),
                new("col2_value")
            ],
            new MacroDefinition.MacroTable(subquery));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Select_Union_By_Name()
    {
        var queries = new Dictionary<SetQuantifier, string>
        {
            { SetQuantifier.ByName, "SELECT * FROM capitals UNION BY NAME SELECT * FROM weather" },
            { SetQuantifier.AllByName,"SELECT * FROM capitals UNION ALL BY NAME SELECT * FROM weather" },
            { SetQuantifier.DistinctByName,"SELECT * FROM capitals UNION DISTINCT BY NAME SELECT * FROM weather" }
        };

        foreach (var sql in queries)
        {
            var select = VerifiedQuery(sql.Value, new[] { new DuckDbDialect() });

            var left = new SelectExpression(new Select(
            [
                new SelectItem.Wildcard(new WildcardAdditionalOptions())
            ])
            {
                From =
                [
                    new(new TableFactor.Table("capitals"))
                ]
            });
            var right = new SelectExpression(new Select(
            [
                new SelectItem.Wildcard(new WildcardAdditionalOptions())
            ])
            {
                From =
                [
                    new(new TableFactor.Table("weather"))
                ]
            });

            SetExpression expected = new SetOperation(left, SetOperator.Union, right, sql.Key);

            Assert.Equal(expected, select.Body);
        }
    }

    [Fact]
    public void Test_DuckDb_Install()
    {
        var statement = VerifiedStatement("INSTALL tpch");

        var expected = new Statement.Install("tpch");

        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Test_DuckDb_Load_Extension()
    {
        var statement = VerifiedStatement("LOAD my_extension");

        var expected = new Statement.Load("my_extension");

        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Test_DuckDb_Struct_Literal()
    {
        var select = VerifiedOnlySelect("SELECT {'a': 1, 'b': 2, 'c': 3}, [{'a': 'abc'}], {'a': 1, 'b': [t.str_col]}, {'a': 1, 'b': 'abc'}, {'abc': str_col}, {'a': {'aa': 1}}");

        Assert.Equal(6, select.Projection.Count);
        Expression expression = new Expression.Dictionary([
            new DictionaryField(new Ident("a", Symbols.SingleQuote), new Expression.LiteralValue(new Value.Number("1"))),
            new DictionaryField(new Ident("b", Symbols.SingleQuote), new Expression.LiteralValue(new Value.Number("2"))),
            new DictionaryField(new Ident("c", Symbols.SingleQuote), new Expression.LiteralValue(new Value.Number("3")))
        ]);

        Assert.Equal(expression, select.Projection[0].AsExpr());

        expression = new Expression.Array(
            new ArrayExpression([
                new Expression.Dictionary([
                    new DictionaryField(new Ident("a", Symbols.SingleQuote),
                        new Expression.LiteralValue(new Value.SingleQuotedString("abc")))
                ])
            ]));

        Assert.Equal(expression, select.Projection[1].AsExpr());

        expression = new Expression.Dictionary([
            new DictionaryField(new Ident("a", Symbols.SingleQuote), new Expression.LiteralValue(new Value.Number("1"))),
            new DictionaryField(new Ident("b", Symbols.SingleQuote), new Expression.Array(new ArrayExpression([
                new Expression.CompoundIdentifier([
                    new Ident("t"),
                    new Ident("str_col")
                ])
            ])))
        ]);

        Assert.Equal(expression, select.Projection[2].AsExpr());


        expression = new Expression.Dictionary([
            new DictionaryField(new Ident("a", Symbols.SingleQuote), new Expression.LiteralValue(new Value.Number("1"))),
            new DictionaryField(new Ident("b", Symbols.SingleQuote), new Expression.LiteralValue(new Value.SingleQuotedString("abc")))
        ]);

        Assert.Equal(expression, select.Projection[3].AsExpr());

        expression = new Expression.Dictionary([
            new DictionaryField(new Ident("abc", Symbols.SingleQuote), new Expression.Identifier("str_col"))
        ]);

        Assert.Equal(expression, select.Projection[4].AsExpr());

        expression = new Expression.Dictionary([
            new DictionaryField(new Ident("a", Symbols.SingleQuote), new Expression.Dictionary([
                new DictionaryField(new Ident("aa", Symbols.SingleQuote),
                new Expression.LiteralValue(new Value.Number("1")))
            ]))
        ]);

        Assert.Equal(expression, select.Projection[5].AsExpr());
    }

    [Fact]
    public void Test_DuckDb_Named_Argument_Function_With_Assignment_Operator()
    {
        var select = VerifiedOnlySelect("SELECT FUN(a := '1', b := '2') FROM foo");
        var function = (Expression.Function)select.Projection.First().AsExpr();

        var args = new Sequence<FunctionArg>
        {
            new FunctionArg.Named("a",
                new FunctionArgExpression.FunctionExpression(
                    new Expression.LiteralValue(new Value.SingleQuotedString("1"))),
                new FunctionArgOperator.Assignment()),
            new FunctionArg.Named("b",
                new FunctionArgExpression.FunctionExpression(
                    new Expression.LiteralValue(new Value.SingleQuotedString("2"))),
                new FunctionArgOperator.Assignment())
        };

        Assert.Equal(new Expression.Function("FUN")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList(args))
        }, function);
    }

    [Fact]
    public void Test_Create_Secret()
    {
        var create = VerifiedStatement<Statement.CreateSecret>("CREATE OR REPLACE PERSISTENT SECRET IF NOT EXISTS name IN storage ( TYPE type, key1 value1, key2 value2 )");

        var expected = new Statement.CreateSecret(true, false, true, "name", "storage", "type",
            [new SecretOption("key1", "value1"), new SecretOption("key2", "value2")]);

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Drop_Secret()
    {
        var create = VerifiedStatement<Statement.DropSecret>("DROP PERSISTENT SECRET IF EXISTS secret FROM storage");

        var expected = new Statement.DropSecret(true, false, "secret", "storage");

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Drop_Secret_Simple()
    {
        var create = VerifiedStatement<Statement.DropSecret>("DROP SECRET secret");

        var expected = new Statement.DropSecret(false, null, "secret", null);

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Attach_Database()
    {
        var create = VerifiedStatement<Statement.AttachDuckDbDatabase>("ATTACH DATABASE IF NOT EXISTS 'sqlite_file.db' AS sqlite_db (READ_ONLY false, TYPE SQLITE)");

        var expected = new Statement.AttachDuckDbDatabase(true, true, new Ident("sqlite_file.db", Symbols.SingleQuote), "sqlite_db",
            [new AttachDuckDbDatabaseOption.ReadOnly(false), new AttachDuckDbDatabaseOption.Type("SQLITE")]);

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Attach_Database_Simple()
    {
        var create = VerifiedStatement<Statement.AttachDuckDbDatabase>("ATTACH 'postgres://user.name:pass-word@some.url.com:5432/postgres'");

        var expected = new Statement.AttachDuckDbDatabase(
            false,
            false,
            new Ident("postgres://user.name:pass-word@some.url.com:5432/postgres", Symbols.SingleQuote),
            null,
            null);

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Detach_Database()
    {
        var create = VerifiedStatement<Statement.DetachDuckDbDatabase>("DETACH DATABASE IF EXISTS db_name");

        var expected = new Statement.DetachDuckDbDatabase(true, true, "db_name");

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Detach_Database_Simple()
    {
        var create = VerifiedStatement<Statement.DetachDuckDbDatabase>("DETACH db_name");

        var expected = new Statement.DetachDuckDbDatabase(false, false, "db_name");

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Test_Array_Index()
    {
        var select = VerifiedOnlySelect("SELECT ['a', 'b', 'c'][3] AS three");
        var projection = select.Projection;
        Assert.Single(projection);

        var expr = ((SelectItem.ExpressionWithAlias)projection[0]).Expression;

        var expected = new Expression.Subscript(new Expression.Array(new ArrayExpression([
                new Expression.LiteralValue(new Value.SingleQuotedString("a")),
                new Expression.LiteralValue(new Value.SingleQuotedString("b")),
                new Expression.LiteralValue(new Value.SingleQuotedString("c"))
            ])),
            new Subscript.Index(new Expression.LiteralValue(new Value.Number("3"))));

        Assert.Equal(expected, expr);
    }

    [Fact]
    public void Test_DuckDb_Union_Datatype()
    {
        const string sql = "CREATE TABLE tbl1 (one UNION(a INT), two UNION(a INT, b INT), nested UNION(a UNION(b INT)))";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        var expected = new Statement.CreateTable(new CreateTable("tbl1", [
            new ColumnDef("one", new DataType.Union([
                new UnionField("a", new DataType.Int())
            ])),
            new ColumnDef("two", new DataType.Union([
                new UnionField("a", new DataType.Int()),
                new UnionField("b", new DataType.Int())
            ])),
            new ColumnDef("nested", new DataType.Union([
                new UnionField("a", new DataType.Union([
                        new UnionField("b", new DataType.Int())
                    ]))
            ])),
        ]));

        Assert.Equal(expected, create);
    }


    [Fact]
    public void Test_Struct()
    {
        var structType = new DataType.Struct([
            new (new DataType.Varchar(), "v"),
            new (new DataType.Integer(), "i")
        ], StructBracketKind.Parentheses);

        var create = (Statement.CreateTable)VerifiedStatement("CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER))");

        Assert.Equal([new ColumnDef("s", structType)], create.Element.Columns);

        structType = new DataType.Struct([
            new (new DataType.Varchar(), "v"),
            new (new DataType.Struct([
                new(new DataType.Integer(), "a1"),
                new(new DataType.Varchar(), "a2")
            ], StructBracketKind.Parentheses), "s")
        ], StructBracketKind.Parentheses);


        create = (Statement.CreateTable)VerifiedStatement("CREATE TABLE t1 (s STRUCT(v VARCHAR, s STRUCT(a1 INTEGER, a2 VARCHAR))[])");

        Assert.Equal([new ColumnDef("s", new DataType.Array(new ArrayElementTypeDef.SquareBracket(structType)))], create.Element.Columns);

        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER)))"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT(v VARCHAR, i INTEGER>)"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT<v VARCHAR, i INTEGER>)"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT v VARCHAR, i INTEGER )"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT VARCHAR, i INTEGER )"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE t1 (s STRUCT (VARCHAR, INTEGER))"));
    }

    [Fact]
    public void Parse_Use()
    {
        List<string> validObjectNames = ["mydb", "SCHEMA", "DATABASE", "CATALOG", "WAREHOUSE", "DEFAULT"];

        List<char> quoteStyles = [Symbols.DoubleQuote, Symbols.SingleQuote];

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

        foreach (var quote in quoteStyles)
        {
            Assert.Equal(new Statement.Use(new Use.Object(new ObjectName([
                new Ident("CATALOG", quote),
                new Ident("my_schema", quote)
            ]))), VerifiedStatement($"USE {quote}CATALOG{quote}.{quote}my_schema{quote}"));
        }

        Assert.Equal(new Statement.Use(new Use.Object(new ObjectName(["mydb", "my_schema"]))), VerifiedStatement("USE mydb.my_schema"));
    }
}
