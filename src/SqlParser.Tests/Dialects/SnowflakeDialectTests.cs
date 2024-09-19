using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;
public class SnowflakeDialectTests : ParserTestBase
{
    public SnowflakeDialectTests()
    {
        DefaultDialects = new[] { new SnowflakeDialect() };
    }

    [Fact]
    public void Test_Snowflake_Create_Table()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE _my_$table (am00unt number)");

        Assert.Equal("_my_$table", create.Element.Name);
    }

    [Fact]
    public void Test_Snowflake_Create_Transient_Table()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var create =
            VerifiedStatement<Statement.CreateTable>("CREATE TRANSIENT TABLE CUSTOMER (id INT, name VARCHAR(255))");

        Assert.Equal("CUSTOMER", create.Element.Name);
        Assert.True(create.Element.Transient);
    }

    [Fact]
    public void Test_Snowflake_Single_Line_Tokenize()
    {
        var tokenizer = new Tokenizer();
        var tokens = tokenizer.Tokenize("CREATE TABLE# this is a comment \ntable_1", new SnowflakeDialect());
        var expected = new List<Token>
        {
            new Word("CREATE"),
            new Whitespace(WhitespaceKind.Space),
            new Word("TABLE"),
            new Whitespace(WhitespaceKind.InlineComment)
            {
                Prefix = "#",
                Value = " this is a comment \n"
            },
            new Word("table_1")
        };
        Assert.Equal(expected, tokens);

        tokens = tokenizer.Tokenize("CREATE TABLE// this is a comment \ntable_1", new SnowflakeDialect());
        expected =
        [
            new Word("CREATE"),
            new Whitespace(WhitespaceKind.Space),
            new Word("TABLE"),
            new Whitespace(WhitespaceKind.InlineComment)
            {
                Prefix = "//",
                Value = " this is a comment \n"
            },

            new Word("table_1")
        ];
        Assert.Equal(expected, tokens);
    }

    [Fact]
    public void Tess_Snowflake_Derived_Table_In_Parenthesis()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];
        OneStatementParsesTo(
            "SELECT * FROM ((SELECT 1) AS t)",
            "SELECT * FROM (SELECT 1) AS t");
        OneStatementParsesTo(
            "SELECT * FROM (((SELECT 1) AS t))",
            "SELECT * FROM (SELECT 1) AS t");
    }

    [Fact]
    public void Test_Single_Table_In_Parenthesis()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN (b))",
            "SELECT * FROM (a NATURAL JOIN b)");
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN ((b)))",
            "SELECT * FROM (a NATURAL JOIN b)");
    }

    [Fact]
    public void Test_Single_Table_In_Parenthesis_With_Alias()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN (b) c )",
            "SELECT * FROM (a NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN ((b)) c )",
            "SELECT * FROM (a NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN ( (b) c ) )",
            "SELECT * FROM (a NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN ( (b) as c ) )",
            "SELECT * FROM (a NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a alias1 NATURAL JOIN ( (b) c ) )",
            "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a as alias1 NATURAL JOIN ( (b) as c ) )",
            "SELECT * FROM (a AS alias1 NATURAL JOIN b AS c)");
        OneStatementParsesTo(
            "SELECT * FROM (a NATURAL JOIN b) c",
            "SELECT * FROM (a NATURAL JOIN b) AS c");

        DefaultDialects = [new SnowflakeDialect()];
        var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM (a b) c"));
        Assert.Equal("Duplicate alias b", ex.Message);
    }

    [Fact]
    public void Parse_Array()
    {
        var select = VerifiedOnlySelect("SELECT CAST(a AS ARRAY) FROM customer");
        Assert.Equal(
            new Cast(new Identifier("a"), new DataType.Array(new ArrayElementTypeDef.None()), CastKind.Cast),
            select.Projection.Single().AsExpr());
    }

    [Fact]
    public void Parse_Semi_Structured_Data_Traversal()
    {
        var select = VerifiedOnlySelect("SELECT a:b FROM t");
        var expected = new SelectItem.UnnamedExpression(new JsonAccess(
            new Identifier("a"),
            new JsonPath([new JsonPathElement.Dot("b", false)])
        ));
        Assert.Equal(expected, select.Projection[0]);


        select = VerifiedOnlySelect("SELECT a:\"my long object key name\" FROM t");
        expected = new SelectItem.UnnamedExpression(new JsonAccess(
            new Identifier("a"),
            new JsonPath([new JsonPathElement.Dot("my long object key name", true)])
        ));
        Assert.Equal(expected, select.Projection[0]);


        select = VerifiedOnlySelect("SELECT a[2 + 2] FROM t");
        expected = new SelectItem.UnnamedExpression(new JsonAccess(
            new Identifier("a"),
            new JsonPath([
                new JsonPathElement.Bracket(
                    new BinaryOp(
                        new LiteralValue(new Value.Number("2")),
                        BinaryOperator.Plus,
                        new LiteralValue(new Value.Number("2"))
                    )
                )
            ])
        ));
        Assert.Equal(expected, select.Projection[0]);

        VerifiedStatement("SELECT a:b::INT FROM t");

        select = VerifiedOnlySelect("SELECT a:select, a:from FROM t");
        Sequence<SelectItem> expectedProjection =
        [
            new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                new JsonPath([new JsonPathElement.Dot("select", false)])
            )),

            new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                new JsonPath([new JsonPathElement.Dot("from", false)])
            )),
        ];
        Assert.Equal(expectedProjection, select.Projection);


        select = VerifiedOnlySelect("SELECT a:foo.\"bar\".baz");
        expectedProjection =
        [
            new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                new JsonPath([
                    new JsonPathElement.Dot("foo", false),
                    new JsonPathElement.Dot("bar", true),
                    new JsonPathElement.Dot("baz", false),
                ])
            ))
        ];
        Assert.Equal(expectedProjection, select.Projection);


        select = VerifiedOnlySelect("SELECT a:foo[0].bar");
        expectedProjection =
        [
            new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                new JsonPath([
                    new JsonPathElement.Dot("foo", false),
                    new JsonPathElement.Bracket(new LiteralValue(new Value.Number("0"))),
                    new JsonPathElement.Dot("bar", false),
                ])
            ))
        ];
        Assert.Equal(expectedProjection, select.Projection);


        select = VerifiedOnlySelect("SELECT a[0].foo.bar");
        expectedProjection =
        [
            new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                new JsonPath([
                    new JsonPathElement.Bracket(new LiteralValue(new Value.Number("0"))),
                    new JsonPathElement.Dot("foo", false),
                    new JsonPathElement.Dot("bar", false),
                ])
            ))
        ];
        Assert.Equal(expectedProjection, select.Projection);


        var expr = VerifiedExpr("a[b:c]");
        var expectedExpr = new JsonAccess(
            new Identifier("a"),
            new JsonPath([
                new JsonPathElement.Bracket(new JsonAccess(
                    new Identifier("b"),
                    new JsonPath([new JsonPathElement.Dot("c", false)])
                ))
            ])
        );
        Assert.Equal(expectedExpr, expr);

        Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT a:42"));
    }

    [Fact]
    public void Parse_Delimited_Identifiers()
    {
        var select =
            VerifiedOnlySelect(
                "SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

        var table = new TableFactor.Table(new ObjectName(new Ident("a table", Symbols.DoubleQuote)))
        {
            Alias = new TableAlias(new Ident("alias", Symbols.DoubleQuote))
        };

        Assert.Equal(table, select.From!.Single().Relation);

        Assert.Equal(3, select.Projection.Count);
        Assert.Equal(new CompoundIdentifier(new Ident[]
        {
            new("alias", Symbols.DoubleQuote),
            new("bar baz", Symbols.DoubleQuote)
        }), select.Projection[0].AsExpr());

        Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote)))
        {
            Args = new FunctionArguments.List(FunctionArgumentList.Empty())
        }, select.Projection[1].AsExpr());

        var expr = new SelectItem.ExpressionWithAlias(new Identifier(new Ident("simple id", Symbols.DoubleQuote)),
            new Ident("column alias", Symbols.DoubleQuote));

        Assert.Equal(expr, select.Projection[2]);

        VerifiedStatement("CREATE TABLE \"foo\" (\"bar\" \"int\")");
        VerifiedStatement("ALTER TABLE foo ADD CONSTRAINT \"bar\" PRIMARY KEY (baz)");
    }

    [Fact]
    public void Parse_Array_Arg_Func()
    {
        VerifiedStatement("SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) AS a FROM T");
        VerifiedStatement("SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM tbl");
    }

    [Fact]
    public void Test_Select_Wildcard_With_Exclude()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT * EXCLUDE (col_a) FROM data");
        SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(new Ident[] { "col_a" })
        });
        Assert.Equal(expected, select.Projection[0]);


        select = VerifiedOnlySelect("SELECT name.* EXCLUDE department_id FROM employee_table");
        expected = new SelectItem.QualifiedWildcard("name",
            new WildcardAdditionalOptions
            {
                ExcludeOption = new ExcludeSelectItem.Single("department_id")

            });
        Assert.Equal(expected, select.Projection[0]);


        select = VerifiedOnlySelect("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
        expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(new Ident[] { "department_id", "employee_id" })
        });
        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Test_Select_Wildcard_With_Rename()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT * RENAME col_a AS col_b FROM data");

        SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            RenameOption = new RenameSelectItem.Single(new IdentWithAlias("col_a", "col_b"))
        });

        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect(
            "SELECT name.* RENAME (department_id AS new_dep, employee_id AS new_emp) FROM employee_table");

        expected = new SelectItem.QualifiedWildcard("name", new WildcardAdditionalOptions
        {
            RenameOption = new RenameSelectItem.Multiple(new IdentWithAlias[]
            {
                new("department_id", "new_dep"),
                new("employee_id", "new_emp")
            })
        });
        Assert.Equal(expected, select.Projection[0]);

    }

    [Fact]
    public void Test_Select_Wildcard_With_Exclude_And_Rename()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT * EXCLUDE col_z RENAME col_a AS col_b FROM data");

        SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Single("col_z"),
            RenameOption = new RenameSelectItem.Single(new IdentWithAlias("col_a", "col_b"))
        });

        Assert.Equal(expected, select.Projection[0]);

        // rename cannot precede exclude
        var ex = Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT * RENAME col_a AS col_b EXCLUDE col_z FROM data"));
        Assert.Equal("Expected end of statement, found EXCLUDE, Line: 1, Col: 32", ex.Message);
    }

    [Fact]
    public void Test_Alter_Table_Swap_With()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab1 SWAP WITH tab2");

        Assert.Equal("tab1", alter.Name);
        Assert.Equal("tab2", ((AlterTableOperation.SwapWith)alter.Operations.First()).Name);
    }

    [Fact]
    public void Test_Drop_Stage()
    {
        DefaultDialects = [new SnowflakeDialect(), new GenericDialect()];

        var drop = VerifiedStatement<Statement.Drop>("DROP STAGE s1");

        Assert.False(drop.IfExists);
        Assert.Equal("s1", drop.Names[0]);

        drop = VerifiedStatement<Statement.Drop>("DROP STAGE IF EXISTS s1");
        Assert.True(drop.IfExists);
        Assert.Equal("s1", drop.Names[0]);

        OneStatementParsesTo("DROP STAGE s1", "DROP STAGE s1");
        OneStatementParsesTo("DROP STAGE IF EXISTS s1", "DROP STAGE IF EXISTS s1");
    }

    [Fact]
    public void Test_Create_Stage()
    {
        DefaultDialects = [new SnowflakeDialect()];

        var sql = "CREATE STAGE s1.s2";
        var create = VerifiedStatement<Statement.CreateStage>(sql);
        Assert.False(create.OrReplace);
        Assert.False(create.Temporary);
        Assert.False(create.IfNotExists);
        Assert.Equal("s1.s2", create.Name);
        Assert.Equal(sql, create.ToSql());

        sql = "CREATE OR REPLACE TEMPORARY STAGE IF NOT EXISTS s1.s2 COMMENT='some-comment'";

        create = VerifiedStatement<Statement.CreateStage>(sql);
        Assert.True(create.OrReplace);
        Assert.True(create.Temporary);
        Assert.True(create.IfNotExists);
        Assert.Equal("s1.s2", create.Name);
        Assert.Equal("some-comment", create.Comment);

        Assert.Equal(sql, create.ToSql());
    }

    [Fact]
    public void Test_Create_Stage_With_Stage_Params()
    {
        DefaultDialects = [new SnowflakeDialect()];

        const string sql = """
                           CREATE OR REPLACE STAGE my_ext_stage 
                           URL='s3://load/files/' 
                           STORAGE_INTEGRATION=myint 
                           ENDPOINT='<s3_api_compatible_endpoint>' 
                           CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') 
                           ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')
                           """;

        var create = VerifiedStatement<Statement.CreateStage>(sql);

        Assert.Equal("s3://load/files/", create.StageParams.Url);
        Assert.Equal("myint", create.StageParams.StorageIntegration);
        Assert.Equal("<s3_api_compatible_endpoint>", create.StageParams.Endpoint);

        Assert.Equal(new DataLoadingOption("AWS_KEY_ID", DataLoadingOptionType.String, "1a2b3c"),
            create.StageParams.Credentials![0]);
        Assert.Equal(new DataLoadingOption("AWS_SECRET_KEY", DataLoadingOptionType.String, "4x5y6z"),
            create.StageParams.Credentials![1]);
        Assert.Equal(new DataLoadingOption("MASTER_KEY", DataLoadingOptionType.String, "key"),
            create.StageParams.Encryption![0]);
        Assert.Equal(new DataLoadingOption("TYPE", DataLoadingOptionType.String, "AWS_SSE_KMS"),
            create.StageParams.Encryption![1]);

        Assert.Equal(sql.Replace("\r", "").Replace("\n", ""), create.ToSql());
    }

    [Fact]
    public void Test_Create_Stage_With_Directory_Table_Params()
    {
        DefaultDialects = [new SnowflakeDialect()];

        const string sql = """
                           CREATE OR REPLACE STAGE my_ext_stage URL='s3://load/files/' 
                           DIRECTORY=(ENABLE=TRUE REFRESH_ON_CREATE=FALSE NOTIFICATION_INTEGRATION='some-string')
                           """;

        var create = VerifiedStatement<Statement.CreateStage>(sql);

        Assert.Equal(new DataLoadingOption("ENABLE", DataLoadingOptionType.Boolean, "TRUE"),
            create.DirectoryTableParams![0]);
        Assert.Equal(new DataLoadingOption("REFRESH_ON_CREATE", DataLoadingOptionType.Boolean, "FALSE"),
            create.DirectoryTableParams![1]);
        Assert.Equal(new DataLoadingOption("NOTIFICATION_INTEGRATION", DataLoadingOptionType.String, "some-string"),
            create.DirectoryTableParams![2]);

        Assert.Equal(sql.Replace("\r", "").Replace("\n", ""), create.ToSql());
    }

    [Fact]
    public void Test_Create_Stage_With_File_Format()
    {
        DefaultDialects = [new SnowflakeDialect()];

        const string sql = """
                           CREATE OR REPLACE STAGE my_ext_stage 
                           URL='s3://load/files/' 
                           FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')
                           """;

        var create = VerifiedStatement<Statement.CreateStage>(sql, options: new ParserOptions
        {
            Unescape = false
        });

        Assert.Equal(new DataLoadingOption("COMPRESSION", DataLoadingOptionType.Enum, "AUTO"),
            create.FileFormat![0]);
        Assert.Equal(new DataLoadingOption("BINARY_FORMAT", DataLoadingOptionType.Enum, "HEX"),
            create.FileFormat![1]);
        Assert.Equal(new DataLoadingOption("ESCAPE", DataLoadingOptionType.String, """\\"""),
            create.FileFormat![2]);

        Assert.Equal(sql.Replace("\r", null).Replace("\n", null), create.ToSql());
    }

    [Fact]
    public void Test_Create_Stage_With_Options()
    {
        DefaultDialects = [new SnowflakeDialect()];

        const string sql =
            "CREATE OR REPLACE STAGE my_ext_stage URL='s3://load/files/' COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)";

        var create = VerifiedStatement<Statement.CreateStage>(sql);

        Assert.Equal(new DataLoadingOption("ON_ERROR", DataLoadingOptionType.Enum, "CONTINUE"),
            create.CopyOptions![0]);
        Assert.Equal(new DataLoadingOption("FORCE", DataLoadingOptionType.Boolean, "TRUE"), create.CopyOptions![1]);

        Assert.Equal(sql, create.ToSql());
    }

    [Fact]
    public void Test_Copy_Into()
    {
        const string sql = "COPY INTO my_company.emp_basic FROM 'gcs://mybucket/./../a.csv'";

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        var expected = new Statement.CopyIntoSnowflake(
            new ObjectName([new Ident("my_company"), new Ident("emp_basic")]),
            new ObjectName(new Ident("gcs://mybucket/./../a.csv", Symbols.SingleQuote)));

        Assert.Equal(expected, copy);
    }

    [Fact]
    public void Test_Copy_Into_With_Stage_Params()
    {
        var sql = """
                  COPY INTO my_company.emp_basic 
                  FROM 's3://load/files/' 
                  STORAGE_INTEGRATION=myint 
                  ENDPOINT='<s3_api_compatible_endpoint>' 
                  CREDENTIALS=(AWS_KEY_ID='1a2b3c' AWS_SECRET_KEY='4x5y6z') 
                  ENCRYPTION=(MASTER_KEY='key' TYPE='AWS_SSE_KMS')
                  """;

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        var expected = new Statement.CopyIntoSnowflake(
            new ObjectName([new Ident("my_company"), new Ident("emp_basic")]),
            new ObjectName(new Ident("s3://load/files/", Symbols.SingleQuote)),
            StageParams: new StageParams
            {
                Endpoint = "<s3_api_compatible_endpoint>",
                StorageIntegration = "myint",
                Credentials =
                [
                    new("AWS_KEY_ID", DataLoadingOptionType.String, "1a2b3c"),
                    new("AWS_SECRET_KEY", DataLoadingOptionType.String, "4x5y6z"),
                ],
                Encryption =
                [
                    new("MASTER_KEY", DataLoadingOptionType.String, "key"),
                    new("TYPE", DataLoadingOptionType.String, "AWS_SSE_KMS"),
                ]
            });

        Assert.Equal(expected, copy);

        sql = """
              COPY INTO my_company.emp_basic FROM 
              (SELECT t1.$1 FROM 's3://load/files/' STORAGE_INTEGRATION=myint)
              """;

        copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        Assert.Equal(new ObjectName(new Ident("s3://load/files/", Symbols.SingleQuote)), copy.FromStage);
        Assert.Equal("myint", copy.StageParams.StorageIntegration);
    }

    [Fact]
    public void Test_Copy_Into_With_Fies_And_Pattern_And_Verification()
    {
        const string sql = """
                           COPY INTO my_company.emp_basic 
                           FROM 'gcs://mybucket/./../a.csv' AS some_alias 
                           FILES = ('file1.json', 'file2.json') 
                           PATTERN = '.*employees0[1-5].csv.gz' 
                           VALIDATION_MODE = RETURN_7_ROWS
                           """;

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        Assert.Equal(["file1.json", "file2.json"], copy.Files);
        Assert.Equal(".*employees0[1-5].csv.gz", copy.Pattern);
        Assert.Equal("RETURN_7_ROWS", copy.ValidationMode);
        Assert.Equal(new Ident("some_alias"), copy.FromStageAlias);
    }

    [Fact]
    public void Test_Copy_Into_With_Transformations()
    {
        const string sql = """
                           COPY INTO my_company.emp_basic 
                           FROM (SELECT t1.$1:st AS st, $1:index, t2.$1 FROM @schema.general_finished AS T) 
                           FILES = ('file1.json', 'file2.json') PATTERN = '.*employees0[1-5].csv.gz' 
                           VALIDATION_MODE = RETURN_7_ROW
                           """;

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        Assert.Equal(new ObjectName(["@schema", "general_finished"]), copy.FromStage);
        Assert.Equal(
        [
            new() { Alias = "t1", FileColumnNumber = 1, Element = "st", ItemAs = "st" },
            new() { FileColumnNumber = 1, Element = "index" },
            new() { Alias = "t2", FileColumnNumber = 1 },
        ], copy.FromTransformations);

    }

    [Fact]
    public void Test_Copy_Into_File_Format()
    {
        const string sql = """
                           COPY INTO my_company.emp_basic 
                           FROM 'gcs://mybucket/./../a.csv' 
                           FILES = ('file1.json', 'file2.json') 
                           PATTERN = '.*employees0[1-5].csv.gz' 
                           FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')
                           """;

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql, options: new ParserOptions
        {
            Unescape = false
        });
        const string value = """
                             \\
                             """;
        Assert.Contains(copy.FileFormat!,
            o => o is { Name: "COMPRESSION", OptionType: DataLoadingOptionType.Enum, Value: "AUTO" });
        Assert.Contains(copy.FileFormat!,
            o => o is { Name: "BINARY_FORMAT", OptionType: DataLoadingOptionType.Enum, Value: "HEX" });
        Assert.Contains(copy.FileFormat!,
            o => o is { Name: "ESCAPE", OptionType: DataLoadingOptionType.String, Value: value });
    }

    [Fact]
    public void Test_Copy_Into_Copy_Format()
    {
        const string sql = """
                           COPY INTO my_company.emp_basic 
                           FROM 'gcs://mybucket/./../a.csv' 
                           FILES = ('file1.json', 'file2.json') 
                           PATTERN = '.*employees0[1-5].csv.gz' 
                           COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)
                           """;

        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

        Assert.Contains(copy.CopyOptions!,
            o => o is { Name: "ON_ERROR", OptionType: DataLoadingOptionType.Enum, Value: "CONTINUE" });
        Assert.Contains(copy.CopyOptions!,
            o => o is { Name: "FORCE", OptionType: DataLoadingOptionType.Boolean, Value: "TRUE" });
    }

    [Fact]
    public void Test_Copy_Stage_Object_Names()
    {
        var allowedObjectNames = new List<ObjectName>
        {
            new(["my_compan", "emp_basic"]),
            new(["@namespace", "%table_name"]),
            new(["@namespace", "%table_name/path"]),
            new(["@namespace", "stage_name/path"]),
            new(new Ident("@~/path")),
        };

        foreach (var objectName in allowedObjectNames)
        {
            var sql = $"COPY INTO {objectName} FROM 'gcs://mybucket/./../a.csv'";

            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            Assert.Equal(objectName, copy.Into);
        }
    }

    [Fact]
    public void Test_BigQuery_Trim()
    {
        var sql = """
                  SELECT customer_id, TRIM(sub_items.value:item_price_id, '"', "a") AS item_price_id FROM models_staging.subscriptions
                  """;

        Assert.Equal(sql, VerifiedStatement(sql).ToSql());

        sql = "SELECT TRIM('xyz', 'a')";

        var select = VerifiedOnlySelect(sql, new[] { new SnowflakeDialect() });
        var expected = new Trim(new LiteralValue(new Value.SingleQuotedString("xyz")),
            TrimWhereField.None,
            TrimCharacters: new Sequence<Expression>
            {
                new LiteralValue(new Value.SingleQuotedString("a"))
            });

        Assert.Equal(expected, select.Projection.First().AsExpr());

        Assert.Throws<ParserException>(() =>
            ParseSqlStatements("SELECT TRIM('xyz' 'a')", new[] { new BigQueryDialect() }));
    }

    [Fact]
    public void Parse_Subquery_Function_Argument()
    {
        // Snowflake allows passing an unparenthesized subquery as the single argument to a function.
        VerifiedStatement("SELECT parse_json(SELECT '{}')");

        // Subqueries that begin with 'WITH' work too.
        VerifiedStatement("SELECT parse_json(WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q)");

        // Commas are parsed as part of the subquery, not additional arguments to
        // the function.
        VerifiedStatement("SELECT func(SELECT 1, 2)");
    }

    [Fact]
    public void Parse_Position_Not_Function_Columns()
    {
        VerifiedStatement("SELECT position FROM tbl1 WHERE position NOT IN ('first', 'last')",
            [new SnowflakeDialect(), new GenericDialect()]);
    }

    [Fact]
    public void Test_Number_Placeholder()
    {
        const string sql = "SELECT :1";
        var select = VerifiedOnlySelect(sql);
        Assert.Equal(new LiteralValue(new Value.Placeholder(":1")), select.Projection.Single().AsExpr());

        Assert.Throws<ParserException>(() => ParseSqlStatements("alter role 1 with name = 'foo'"));
    }

    [Fact]
    public void Parse_Lateral_Flatten()
    {
        VerifiedOnlySelect(
            "SELECT * FROM TABLE(FLATTEN(input => parse_json('{\"a\":1, \b\":[77,88]}'), outer => true)) AS f");
        VerifiedOnlySelect(
            "SELECT emp.employee_ID, emp.last_name, index, value AS project_name FROM employees AS emp, LATERAL FLATTEN(INPUT => emp.project_names) AS proj_names");
    }

    [Fact]
    public void Parse_Pivot_Of_Table_Factor_Derived()
    {
        VerifiedStatement(
            "SELECT * FROM (SELECT place_id, weekday, open FROM times AS p) PIVOT(max(open) FOR weekday IN (0, 1, 2, 3, 4, 5, 6)) AS p (place_id, open_sun, open_mon, open_tue, open_wed, open_thu, open_fri, open_sat)");
    }

    [Fact]
    public void Parse_Comma_Outer_Join()
    {
        var select = VerifiedOnlySelect("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)");
        Expression left = new CompoundIdentifier(["t1", "c1"]);
        Expression right = new OuterJoin(new CompoundIdentifier(["t2", "c2"]));
        Assert.Equal(select.Selection!.AsBinaryOp().Left, left);
        Assert.Equal(select.Selection.AsBinaryOp().Right, right);
        Assert.Equal(BinaryOperator.Eq, select.Selection.AsBinaryOp().Op);

        select = VerifiedOnlySelect("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = c2 (+)");
        left = new Identifier("c1");
        right = new OuterJoin(new Identifier("c2"));
        Assert.Equal(select.Selection!.AsBinaryOp().Left, left);
        Assert.Equal(select.Selection.AsBinaryOp().Right, right);
        Assert.Equal(BinaryOperator.Eq, select.Selection.AsBinaryOp().Op);

        select = VerifiedOnlySelect("SELECT t1.c1, t2.c2 FROM t1, t2 WHERE c1 = myudf(+42)");
        left = new Identifier("c1");
        right = new Function("myudf")
        {
            Args = new FunctionArguments.List(new FunctionArgumentList([
                new FunctionArg.Unnamed(
                    new FunctionArgExpression.FunctionExpression(
                        new UnaryOp(
                            new LiteralValue(
                                new Value.Number("42")), UnaryOperator.Plus)))
            ]))

        };
        Assert.Equal(select.Selection!.AsBinaryOp().Left, left);
        Assert.Equal(select.Selection.AsBinaryOp().Right, right);
        Assert.Equal(BinaryOperator.Eq, select.Selection.AsBinaryOp().Op);

        VerifiedOnlySelectWithCanonical(
            "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2(   +     )",
            "SELECT t1.c1, t2.c2 FROM t1, t2 WHERE t1.c1 = t2.c2 (+)");
    }

    [Fact]
    public void Parse_Snowflake_Declare_Cursor()
    {
        List<(string Sql, string Name, DeclareAssignment? Assignment, Sequence<Ident>? Projections)> queries =
        [
            ("DECLARE c1 CURSOR FOR SELECT id, price FROM invoices", "c1", null, ["id", "price"]),
            ("DECLARE c1 CURSOR FOR res", "c1", new DeclareAssignment.For(new Identifier("res")), null),
        ];

        foreach (var query in queries)
        {
            var declare = (Statement.Declare)VerifiedStatement(query.Sql);
            Assert.Single(declare.Statements);
            var statement = declare.Statements[0];
            Assert.Equal([query.Name], statement.Names);
            Assert.Null(statement.DataType);
            Assert.Equal(query.Assignment, statement.Assignment);

            if (statement.ForQuery != null)
            {
                var projections = statement.ForQuery.Body.AsSelect().Projection.Select(item =>
                    ((SelectItem.UnnamedExpression)item).Expression.AsIdentifier().Ident);
                var sequence = new Sequence<Ident>(projections);
                Assert.Equal(query.Projections, sequence);
            }
        }

        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE c1 CURSOR SELECT id FROM invoices"); });
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE c1 CURSOR res"); });
    }

    [Fact]
    public void Parse_Snowflake_Declare_Result_Set()
    {
        List<(string Sql, string Name, DeclareAssignment? Assignment)> queries =
        [
            ("DECLARE res RESULTSET DEFAULT 42", "res",
                new DeclareAssignment.Default(new LiteralValue(new Value.Number("42")))),
            ("DECLARE res RESULTSET := 42", "res",
                new DeclareAssignment.Assignment(new LiteralValue(new Value.Number("42")))),
            ("DECLARE res RESULTSET", "res", null),
        ];

        foreach (var query in queries)
        {
            var declare = (Statement.Declare)VerifiedStatement(query.Sql);
            Assert.Single(declare.Statements);
            var statement = declare.Statements[0];
            Assert.Equal([query.Name], statement.Names);
            Assert.Null(statement.DataType);
            Assert.Null(statement.ForQuery);
            Assert.Equal(DeclareType.ResultSet, statement.DeclareType);
            Assert.Equal(query.Assignment, statement.Assignment);
        }

        VerifiedStatement("DECLARE res RESULTSET DEFAULT (SELECT price FROM invoices)");
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE res RESULTSET DEFAULT"); });
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE res RESULTSET :="); });
    }

    [Fact]
    public void Parse_Snowflake_Declare_Exception()
    {
        List<(string Sql, string Name, DeclareAssignment? Assignment)> queries =
        [
            ("DECLARE ex EXCEPTION (42, 'ERROR')", "ex", new DeclareAssignment.DeclareExpression(
                new Expression.Tuple([
                    new LiteralValue(new Value.Number("42")),
                    new LiteralValue(new Value.SingleQuotedString("ERROR"))
                ]))),
            ("DECLARE ex EXCEPTION", "ex", null)
        ];

        foreach (var query in queries)
        {
            var declare = (Statement.Declare)VerifiedStatement(query.Sql);
            Assert.Single(declare.Statements);
            var statement = declare.Statements[0];
            Assert.Equal([query.Name], statement.Names);
            Assert.Null(statement.DataType);
            Assert.Null(statement.ForQuery);
            Assert.Equal(DeclareType.Exception, statement.DeclareType);
            Assert.Equal(query.Assignment, statement.Assignment);
        }
    }

    [Fact]
    public void Parse_Snowflake_Declare_Variable()
    {
        List<(string Sql, string Name, DataType? DataType, DeclareAssignment? Assignment)> queries =
        [
            ("DECLARE profit TEXT DEFAULT 42", "profit", new DataType.Text(),
                new DeclareAssignment.Default(new LiteralValue(new Value.Number("42")))),
            ("DECLARE profit DEFAULT 42", "profit", null,
                new DeclareAssignment.Default(new LiteralValue(new Value.Number("42")))),
            ("DECLARE profit TEXT", "profit", new DataType.Text(), null),
            ("DECLARE profit", "profit", null, null)
        ];

        foreach (var query in queries)
        {
            var declare = (Statement.Declare)VerifiedStatement(query.Sql);
            Assert.Single(declare.Statements);
            var statement = declare.Statements[0];
            Assert.Equal([query.Name], statement.Names);
            Assert.Equal(query.DataType, statement.DataType);
            Assert.Null(statement.ForQuery);
            Assert.Null(statement.DeclareType);
            Assert.Equal(query.Assignment, statement.Assignment);
        }

        OneStatementParsesTo("DECLARE profit;", "DECLARE profit");
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE profit INT 2"); });
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE profit INT DEFAULT"); });
        Assert.Throws<ParserException>(() => { ParseSqlStatements("DECLARE profit DEFAULT"); });
    }

    [Fact]
    public void Test_Snowflake_Copy_Into()
    {
        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>("COPY INTO a.b FROM @namespace.stage_name");
        var into = new ObjectName(["a", "b"]);
        var fromStage = new ObjectName(["@namespace", "stage_name"]);

        Assert.Equal(into, copy.Into);
        Assert.Equal(fromStage, copy.FromStage);
    }

    [Fact]
    public void Test_Snowflake_Copy_Into_Stage_Name_Ends_With_Parens()
    {
        var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(
            "COPY INTO SCHEMA.SOME_MONITORING_SYSTEM FROM (SELECT t.$1:st AS st FROM @schema.general_finished)");
        var into = new ObjectName(["SCHEMA", "SOME_MONITORING_SYSTEM"]);
        var fromStage = new ObjectName(["@schema", "general_finished"]);

        Assert.Equal(into, copy.Into);
        Assert.Equal(fromStage, copy.FromStage);
    }

    [Fact]
    public void Test_Sf_Trailing_Commas()
    {
        VerifiedOnlySelectWithCanonical("SELECT 1, 2, FROM t", "SELECT 1, 2 FROM t");
    }

    [Fact]
    public void Parse_Extract_Custom_Part()
    {
        var select = VerifiedOnlySelect("SELECT EXTRACT(eod FROM d)");

        var expected = new Extract(new Identifier("d"), new DateTimeField.Custom("eod"), ExtractSyntax.From);

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Extract_Comma()
    {
        var select = VerifiedOnlySelect("SELECT EXTRACT(HOUR, d)");

        var expected = new Extract(new Identifier("d"), new DateTimeField.Hour(), ExtractSyntax.Comma);

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Extract_Comma_Quoted()
    {
        var select = VerifiedOnlySelect("SELECT EXTRACT('hour', d)");

        var expected = new Extract(
            new Identifier("d"),
            new DateTimeField.Custom(new Ident("hour", Symbols.SingleQuote)),
            ExtractSyntax.Comma);

        Assert.Equal(expected, select.Projection.First().AsExpr());
    }

    [Fact]
    public void First_Value_Ignore_Nulls()
    {
        VerifiedOnlySelect("""
                           SELECT FIRST_VALUE(column2 IGNORE NULLS) 
                           OVER (PARTITION BY column1 ORDER BY column2) 
                           FROM some_table
                           """);
    }

    [Fact]
    public void Test_Pivot()
    {
        // pivot on static list of values with default
        VerifiedOnlySelect(
            """
            SELECT * 
            FROM quarterly_sales 
            PIVOT(SUM(amount) 
            FOR quarter IN (
            '2023_Q1', 
            '2023_Q2', 
            '2023_Q3', 
            '2023_Q4', 
            '2024_Q1') 
            DEFAULT ON NULL (0)
            ) 
            ORDER BY empid
            """);

        // dynamic pivot from subquery
        VerifiedOnlySelect(
            """
            SELECT * 
            FROM quarterly_sales 
            PIVOT(SUM(amount) FOR quarter IN (
            SELECT DISTINCT quarter 
            FROM ad_campaign_types_by_quarter 
            WHERE television = true 
            ORDER BY quarter)
            ) 
            ORDER BY empid
            """);

        // dynamic pivot on any value (with order by)
        VerifiedOnlySelect(
            """
            SELECT * 
            FROM quarterly_sales 
            PIVOT(SUM(amount) FOR quarter IN (ANY ORDER BY quarter)) 
            ORDER BY empid
            """);

        // dynamic pivot on any value (without order by)
        VerifiedOnlySelect(
            """
            SELECT * 
            FROM sales_data 
            PIVOT(SUM(total_sales) FOR fis_quarter IN (ANY)) 
            WHERE fis_year IN (2023) 
            ORDER BY region
            """);
    }


    [Fact]
    public void Parse_Of_Create_Or_Replace_View_With_Comment_Missing_Equal()
    {
        Assert.Single(ParseSqlStatements("CREATE OR REPLACE VIEW v COMMENT = 'hello, world' AS SELECT 1"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE OR REPLACE VIEW v COMMENT 'hello, world' AS SELECT 1"));
    }

    [Fact]
    public void Parse_Create_Or_Replace_With_Comment_For_Snowflake()
    {
        var view = VerifiedStatement<Statement.CreateView>("CREATE OR REPLACE VIEW v COMMENT = 'hello, world' AS SELECT 1");

        Assert.Equal("v", view.Name);
        Assert.Null(view.Columns);
        Assert.Equal(new CreateTableOptions.None(), view.Options);
        Assert.Equal("SELECT 1", view.Query.ToSql());
        Assert.False(view.Materialized);
        Assert.True(view.OrReplace);
        Assert.Null(view.ClusterBy);
        Assert.Equal("hello, world", view.Comment);
        Assert.False(view.WithNoSchemaBinding);
        Assert.False(view.IfNotExists);
        Assert.False(view.Temporary);
    }

    [Fact]
    public void AsOf_Joins()
    {
        var query = VerifiedOnlySelect("""
                                       SELECT * 
                                       FROM trades_unixtime AS tu 
                                       ASOF JOIN quotes_unixtime AS qu 
                                       MATCH_CONDITION (tu.trade_time >= qu.quote_time)
                                       """);

        var expected = new TableWithJoins(new TableFactor.Table("trades_unixtime")
        {
            Alias = new TableAlias("tu")
        })
        {
            Joins = [
                new Join(new TableFactor.Table("quotes_unixtime")
                    {
                        Alias = new TableAlias("qu")
                    },
                    new JoinOperator.AsOf(new BinaryOp(
                            new CompoundIdentifier(["tu", "trade_time"]),
                            BinaryOperator.GtEq,
                            new CompoundIdentifier(["qu", "quote_time"])
                        ), new JoinConstraint.None()))
            ]
        };

        Assert.Equal(expected, query.From![0]);
    }

    [Fact]
    public void Test_Snowflake_Create_Or_Replace_Table()
    {
        const string sql = "CREATE OR REPLACE TABLE my_table (a number)";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.OrReplace);
    }

    [Fact]
    public void Test_Snowflake_Create_Or_Replace_Table_Copy_Grants()
    {
        const string sql = "CREATE OR REPLACE TABLE my_table (a number) COPY GRANTS";
        var create = VerifiedStatement<Statement.CreateTable>(sql);
        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.OrReplace);
        Assert.True(create.Element.CopyGrants);
    }

    [Fact]
    public void Test_Snowflake_Create_Or_Replace_Table_Copy_Grants_At_End()
    {
        const string sql = "CREATE OR REPLACE TABLE my_table COPY GRANTS (a number) ";
        const string parsed = "CREATE OR REPLACE TABLE my_table (a number) COPY GRANTS";

        var create = OneStatementParsesTo<Statement.CreateTable>(sql, parsed);
        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.OrReplace);
        Assert.True(create.Element.CopyGrants);
    }

    [Fact]
    public void Test_Snowflake_Create_Or_Replace_Table_Copy_Grants_At_Cta()
    {
        DefaultDialects = [new SnowflakeDialect()];
        const string sql = "CREATE OR REPLACE TABLE my_table COPY GRANTS AS SELECT 1 AS a";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.OrReplace);
        Assert.True(create.Element.CopyGrants);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_Enable_Schema_Evolution()
    {
        const string sql = "CREATE TABLE my_table (a number) ENABLE_SCHEMA_EVOLUTION=TRUE";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.EnableSchemaEvolution);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_Change_Tracking()
    {
        const string sql = "CREATE TABLE my_table (a number) CHANGE_TRACKING=TRUE";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.True(create.Element.ChangeTracking);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_Data_Retention_Time_In_Days()
    {
        const string sql = "CREATE TABLE my_table (a number) DATA_RETENTION_TIME_IN_DAYS=5";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal(5, create.Element.DataRetentionTimeInDays);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_Max_Data_Extension_Time_In_Days()
    {
        const string sql = "CREATE TABLE my_table (a number) MAX_DATA_EXTENSION_TIME_IN_DAYS=5";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal(5, create.Element.MaxDataExtensionTimeInDays);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_With_Aggregation_Policy()
    {
        DefaultDialects = [new SnowflakeDialect()];
        const string sql = "CREATE TABLE my_table (a number) WITH AGGREGATION POLICY policy_name";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal("policy_name", create.Element.WithAggregationPolicy!);

        create = (Statement.CreateTable)ParseSqlStatements("CREATE TABLE my_table (a number) AGGREGATION POLICY policy_name")[0]!;

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal("policy_name", create.Element.WithAggregationPolicy!);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_With_Row_Access_Policy()
    {
        DefaultDialects = [new SnowflakeDialect()];
        const string sql = "CREATE TABLE my_table (a number, b number) WITH ROW ACCESS POLICY policy_name ON (a)";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal("WITH ROW ACCESS POLICY policy_name ON (a)", create.Element.WithRowAccessPolicy!.ToSql());

        create = (Statement.CreateTable)ParseSqlStatements("CREATE TABLE my_table (a number, b number) ROW ACCESS POLICY policy_name ON (a)")[0]!;

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal("WITH ROW ACCESS POLICY policy_name ON (a)", create.Element.WithRowAccessPolicy!.ToSql());
    }

    [Fact]
    public void Test_Snowflake_Create_Table_With_Tag()
    {
        DefaultDialects = [new SnowflakeDialect()];
        const string sql = "CREATE TABLE my_table (a number) WITH TAG (A='TAG A', B='TAG B')";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal(
        [
            new ("A", "TAG A"),
            new ("B", "TAG B")
        ], create.Element.WithTags);

        create = (Statement.CreateTable)ParseSqlStatements("CREATE TABLE my_table (a number) TAG (A='TAG A', B='TAG B')")[0]!;
        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal(
        [
            new ("A", "TAG A"),
            new ("B", "TAG B")
        ], create.Element.WithTags);
    }

    [Fact]
    public void Test_Snowflake_Create_Table_Default_Ddl_Collation()
    {
        const string sql = "CREATE TABLE my_table (a number) DEFAULT_DDL_COLLATION='de'";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        Assert.Equal("my_table", create.Element.Name);
        Assert.Equal("de", create.Element.DefaultDdlCollation);
    }

    [Fact]
    public void Test_Select_Wildcard_With_Replace_And_Rename()
    {
        var select = VerifiedOnlySelect("SELECT * REPLACE (col_z || col_z AS col_z) RENAME (col_z AS col_zz) FROM data");

        var expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ReplaceOption = new ReplaceSelectItem([
                new (new BinaryOp(
                        new Identifier("col_z"),
                        BinaryOperator.StringConcat,
                        new Identifier("col_z")
                    ), "col_z", true)
            ]),
            RenameOption = new RenameSelectItem.Multiple([
                new IdentWithAlias("col_z", "col_zz")
            ])
        });

        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Explain_Describe()
    {
        VerifiedStatement("DESCRIBE test.table");
        VerifiedStatement("DESCRIBE TABLE test.table");
    }

    [Fact]
    public void Explain_Desc()
    {
        VerifiedStatement("DESC test.table");
        VerifiedStatement("DESC TABLE test.table");
    }

    [Fact]
    public void Parse_Explain_Table()
    {
        var explain = VerifiedStatement<Statement.ExplainTable>("EXPLAIN TABLE test_identifier");

        Assert.Equal(DescribeAlias.Explain, explain.DescribeAlias);
        Assert.Null(explain.HiveFormat);
        Assert.True(explain.HasTableKeyword);
        Assert.Equal("test_identifier", explain.Name);
    }

    [Fact]
    public void Parse_Use()
    {
        List<string> validObjectNames = ["mydb", "CATALOG", "DEFAULT"];

        List<char> quoteStyles = [Symbols.SingleQuote, Symbols.DoubleQuote, Symbols.Backtick];

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
            var useStatement = VerifiedStatement<Statement.Use>($"USE {quote}CATALOG{quote}.{quote}my_schema{quote}");
            var expected = new Use.Object(new ObjectName([
                new Ident("CATALOG", quote),
                new Ident("my_schema", quote)
            ]));
            Assert.Equal(expected, useStatement.Name);
        }

        Assert.Equal(new Statement.Use(new Use.Object(new ObjectName(["mydb", "my_schema"]))), VerifiedStatement("USE mydb.my_schema"));

        foreach (var quote in quoteStyles)
        {
            Assert.Equal(new Statement.Use(new Use.Database(new ObjectName(new Ident("my_database", quote)))),
                VerifiedStatement($"USE DATABASE {quote}my_database{quote}"));

            Assert.Equal(new Statement.Use(new Use.Schema(new ObjectName(new Ident("my_schema", quote)))),
                VerifiedStatement($"USE SCHEMA {quote}my_schema{quote}"));

            Assert.Equal(new Statement.Use(new Use.Schema(new ObjectName(
                    [
                        new Ident("CATALOG", quote),
                        new Ident("my_schema", quote)
                    ]))),
                VerifiedStatement($"USE SCHEMA {quote}CATALOG{quote}.{quote}my_schema{quote}"));
        }
    }
}