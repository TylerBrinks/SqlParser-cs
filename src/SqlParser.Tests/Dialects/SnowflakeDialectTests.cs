using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects
{
    public class SnowflakeDialectTests : ParserTestBase
    {
        public SnowflakeDialectTests()
        {
            DefaultDialects = new[] { new SnowflakeDialect() };
        }

        [Fact]
        public void Test_Snowflake_Create_Table()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

            var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE _my_$table (am00unt number)");

            Assert.Equal("_my_$table", create.Name);
        }

        [Fact]
        public void Test_Snowflake_Create_Transient_Table()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

            var create = VerifiedStatement<Statement.CreateTable>("CREATE TRANSIENT TABLE CUSTOMER (id INT, name VARCHAR(255))");

            Assert.Equal("CUSTOMER", create.Name);
            Assert.True(create.Transient);
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
            expected = new List<Token>
            {
                new Word("CREATE"),
                new Whitespace(WhitespaceKind.Space),
                new Word("TABLE"),
                new Whitespace(WhitespaceKind.InlineComment)
                {
                    Prefix = "//",
                    Value = " this is a comment \n"
                },
                new Word("table_1")
            };
            Assert.Equal(expected, tokens);
        }

        [Fact]
        public void Tess_Snowflake_Derived_Table_In_Parenthesis()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };
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
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };
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
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

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

            DefaultDialects = new Dialect[] { new SnowflakeDialect() };
            var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * FROM (a b) c"));
            Assert.Equal("Duplicate alias b", ex.Message);
        }

        [Fact]
        public void Parse_Array()
        {
            var select = VerifiedOnlySelect("SELECT CAST(a AS ARRAY) FROM customer");
            Assert.Equal(new Cast(new Identifier("a"), new DataType.Array(new DataType.None())), select.Projection.Single().AsExpr());
        }

        [Fact]
        public void Parse_Json_Using_Colon()
        {
            var select = VerifiedOnlySelect("SELECT a:b FROM t");
            var expected = new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                JsonOperator.Colon,
                new LiteralValue(new Value.UnQuotedString("b"))
            ));
            Assert.Equal(expected, select.Projection[0]);


            select = VerifiedOnlySelect("SELECT a:type FROM t");
            expected = new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                JsonOperator.Colon,
                new LiteralValue(new Value.UnQuotedString("type"))
            ));
            Assert.Equal(expected, select.Projection[0]);


            select = VerifiedOnlySelect("SELECT a:location FROM t");
            expected = new SelectItem.UnnamedExpression(new JsonAccess(
                new Identifier("a"),
                JsonOperator.Colon,
                new LiteralValue(new Value.UnQuotedString("location"))
            ));
            Assert.Equal(expected, select.Projection[0]);
        }

        [Fact]
        public void Parse_Delimited_Identifiers()
        {
            var select = VerifiedOnlySelect("SELECT \"alias\".\"bar baz\", \"myfun\"(), \"simple id\" AS \"column alias\" FROM \"a table\" AS \"alias\"");

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

            Assert.Equal(new Function(new ObjectName(new Ident("myfun", Symbols.DoubleQuote))), select.Projection[1].AsExpr());

            var expr = new SelectItem.ExpressionWithAlias(new Identifier(new Ident("simple id", Symbols.DoubleQuote)),
                new Ident("column alias", Symbols.DoubleQuote));

            Assert.Equal(expr, select.Projection[2]);

            VerifiedStatement("CREATE TABLE \"foo\" (\"bar\" \"int\")");
            VerifiedStatement("ALTER TABLE foo ADD CONSTRAINT \"bar\" PRIMARY KEY (baz)");
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
        public void Parse_Array_Arg_Func()
        {
            VerifiedStatement("SELECT ARRAY_AGG(x) WITHIN GROUP (ORDER BY x) AS a FROM T");
            VerifiedStatement("SELECT ARRAY_AGG(DISTINCT x) WITHIN GROUP (ORDER BY x ASC) FROM tbl");

            var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("select array_agg(x order by x) as a from T"));
            Assert.Equal("Expected ), found order, Line: 1, Col: 20", ex.Message);

        }

        [Fact]
        public void Test_Select_Wildcard_With_Exclude()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

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
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

            var select = VerifiedOnlySelect("SELECT * RENAME col_a AS col_b FROM data");

            SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                RenameOption = new RenameSelectItem.Single(new IdentWithAlias("col_a", "col_b"))
            });

            Assert.Equal(expected, select.Projection[0]);

            select = VerifiedOnlySelect("SELECT name.* RENAME (department_id AS new_dep, employee_id AS new_emp) FROM employee_table");

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
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

            var select = VerifiedOnlySelect("SELECT * EXCLUDE col_z RENAME col_a AS col_b FROM data");

            SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
            {
                ExcludeOption = new ExcludeSelectItem.Single("col_z"),
                RenameOption = new RenameSelectItem.Single(new IdentWithAlias("col_a", "col_b"))
            });

            Assert.Equal(expected, select.Projection[0]);

            // rename cannot precede exclude
            var ex = Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT * RENAME col_a AS col_b EXCLUDE col_z FROM data"));
            Assert.Equal("Expected end of statement, found EXCLUDE, Line: 1, Col: 32", ex.Message);
        }

        [Fact]
        public void Test_Alter_Table_Swap_With()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

            var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab1 SWAP WITH tab2");

            Assert.Equal("tab1", alter.Name);
            Assert.Equal("tab2", ((AlterTableOperation.SwapWith)alter.Operations.First()).Name);
        }

        [Fact]
        public void Test_Drop_Stage()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect(), new GenericDialect() };

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
            DefaultDialects = new Dialect[] { new SnowflakeDialect() };

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
            DefaultDialects = new Dialect[] { new SnowflakeDialect() };

            var sql = """
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

            Assert.Equal(new DataLoadingOption("AWS_KEY_ID", DataLoadingOptionType.String, "1a2b3c"), create.StageParams.Credentials![0]);
            Assert.Equal(new DataLoadingOption("AWS_SECRET_KEY", DataLoadingOptionType.String, "4x5y6z"), create.StageParams.Credentials![1]);
            Assert.Equal(new DataLoadingOption("MASTER_KEY", DataLoadingOptionType.String, "key"), create.StageParams.Encryption![0]);
            Assert.Equal(new DataLoadingOption("TYPE", DataLoadingOptionType.String, "AWS_SSE_KMS"), create.StageParams.Encryption![1]);

            Assert.Equal(sql.Replace("\r", "").Replace("\n", ""), create.ToSql());
        }

        [Fact]
        public void Test_Create_Stage_With_Directory_Table_Params()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect() };

            var sql = """
                CREATE OR REPLACE STAGE my_ext_stage URL='s3://load/files/' 
                DIRECTORY=(ENABLE=TRUE REFRESH_ON_CREATE=FALSE NOTIFICATION_INTEGRATION='some-string')
                """;

            var create = VerifiedStatement<Statement.CreateStage>(sql);

            Assert.Equal(new DataLoadingOption("ENABLE", DataLoadingOptionType.Boolean, "TRUE"), create.DirectoryTableParams![0]);
            Assert.Equal(new DataLoadingOption("REFRESH_ON_CREATE", DataLoadingOptionType.Boolean, "FALSE"), create.DirectoryTableParams![1]);
            Assert.Equal(new DataLoadingOption("NOTIFICATION_INTEGRATION", DataLoadingOptionType.String, "some-string"), create.DirectoryTableParams![2]);

            Assert.Equal(sql.Replace("\r", "").Replace("\n", ""), create.ToSql());
        }


        [Fact]
        public void Test_Create_Stage_With_File_Format()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect() };

            const string sql = "CREATE OR REPLACE STAGE my_ext_stage URL='s3://load/files/' FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\\')";

            var create = VerifiedStatement<Statement.CreateStage>(sql);

            Assert.Equal(new DataLoadingOption("COMPRESSION", DataLoadingOptionType.Enum, "AUTO"), create.FileFormat![0]);
            Assert.Equal(new DataLoadingOption("BINARY_FORMAT", DataLoadingOptionType.Enum, "HEX"), create.FileFormat![1]);
            Assert.Equal(new DataLoadingOption("ESCAPE", DataLoadingOptionType.String, "\\"), create.FileFormat![2]);

            Assert.Equal(sql, create.ToSql());
        }

        [Fact]
        public void Test_Create_Stage_With_Options()
        {
            DefaultDialects = new Dialect[] { new SnowflakeDialect() };

            const string sql = "CREATE OR REPLACE STAGE my_ext_stage URL='s3://load/files/' COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)";

            var create = VerifiedStatement<Statement.CreateStage>(sql);

            Assert.Equal(new DataLoadingOption("ON_ERROR", DataLoadingOptionType.Enum, "CONTINUE"), create.CopyOptions![0]);
            Assert.Equal(new DataLoadingOption("FORCE", DataLoadingOptionType.Boolean, "TRUE"), create.CopyOptions![1]);

            Assert.Equal(sql, create.ToSql());
        }

        [Fact]
        public void Test_Coppy_Into()
        {
            const string sql = "COPY INTO my_company.emp_basic FROM 'gcs://mybucket/./../a.csv'";

            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            var expected = new Statement.CopyIntoSnowflake(
                new ObjectName(new[] { new Ident("my_company"), new Ident("emp_basic") }),
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
                new ObjectName(new[] { new Ident("my_company"), new Ident("emp_basic") }),
                new ObjectName(new Ident("s3://load/files/", Symbols.SingleQuote)),
                StageParams: new StageParams
                {
                    Endpoint = "<s3_api_compatible_endpoint>",
                    StorageIntegration = "myint",
                    Credentials = new Sequence<DataLoadingOption>
                    {
                        new ("AWS_KEY_ID", DataLoadingOptionType.String, "1a2b3c"),
                        new ("AWS_SECRET_KEY", DataLoadingOptionType.String, "4x5y6z"),
                    },
                    Encryption = new Sequence<DataLoadingOption>
                    {
                        new ("MASTER_KEY", DataLoadingOptionType.String, "key"),
                        new ("TYPE", DataLoadingOptionType.String, "AWS_SSE_KMS"),
                    }
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
            var sql = """
                      COPY INTO my_company.emp_basic 
                      FROM 'gcs://mybucket/./../a.csv' AS some_alias 
                      FILES = ('file1.json', 'file2.json') 
                      PATTERN = '.*employees0[1-5].csv.gz' 
                      VALIDATION_MODE = RETURN_7_ROWS
                      """;

            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            Assert.Equal(new Sequence<string> { "file1.json", "file2.json" }, copy.Files);
            Assert.Equal(".*employees0[1-5].csv.gz", copy.Pattern);
            Assert.Equal("RETURN_7_ROWS", copy.ValidationMode);
            Assert.Equal(new Ident("some_alias"), copy.FromStageAlias);
        }

        [Fact]
        public void Test_Copy_Into_With_Transformations()
        {
            var sql = """
                    COPY INTO my_company.emp_basic 
                    FROM (SELECT t1.$1:st AS st, $1:index, t2.$1 FROM @schema.general_finished AS T) 
                    FILES = ('file1.json', 'file2.json') PATTERN = '.*employees0[1-5].csv.gz' 
                    VALIDATION_MODE = RETURN_7_ROW
                    """;

            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            Assert.Equal(new ObjectName(new Ident[] { "@schema", "general_finished" }), copy.FromStage);
            Assert.Equal(new Sequence<StageLoadSelectItem>
            {
                new () { Alias = "t1", FileColumnNumber = 1, Element = "st", ItemAs = "st" },
                new () { FileColumnNumber = 1, Element = "index" },
                new () { Alias = "t2", FileColumnNumber = 1 },
            }, copy.FromTransformations);
            
        }

        [Fact]
        public void Test_Copy_Into_File_Format()
        {
            var sql = """
                COPY INTO my_company.emp_basic 
                FROM 'gcs://mybucket/./../a.csv' 
                FILES = ('file1.json', 'file2.json') 
                PATTERN = '.*employees0[1-5].csv.gz' 
                FILE_FORMAT=(COMPRESSION=AUTO BINARY_FORMAT=HEX ESCAPE='\')
                """;
            
            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            Assert.Contains(copy!.FileFormat!, o => o is {Name: "COMPRESSION", OptionType: DataLoadingOptionType.Enum, Value: "AUTO"});
            Assert.Contains(copy!.FileFormat!, o => o is {Name: "BINARY_FORMAT", OptionType: DataLoadingOptionType.Enum, Value: "HEX"});
            Assert.Contains(copy!.FileFormat!, o => o is {Name: "ESCAPE", OptionType: DataLoadingOptionType.String, Value: "\\"});
        }

        [Fact]
        public void Test_Copy_Into_Copy_Format()
        {
            var sql = """
                      COPY INTO my_company.emp_basic 
                      FROM 'gcs://mybucket/./../a.csv' 
                      FILES = ('file1.json', 'file2.json') 
                      PATTERN = '.*employees0[1-5].csv.gz' 
                      COPY_OPTIONS=(ON_ERROR=CONTINUE FORCE=TRUE)
                      """;

            var copy = VerifiedStatement<Statement.CopyIntoSnowflake>(sql);

            Assert.Contains(copy!.CopyOptions!, o => o is { Name: "ON_ERROR", OptionType: DataLoadingOptionType.Enum, Value: "CONTINUE" });
            Assert.Contains(copy!.CopyOptions!, o => o is { Name: "FORCE", OptionType: DataLoadingOptionType.Boolean, Value: "TRUE" });
        }

        [Fact]
        public void Test_Copy_Stage_Object_Names()
        {
            var allowedObjectNames = new List<ObjectName>
            {
                new (new Ident[]{ "my_compan", "emp_basic"}),
                new (new Ident[]{ "@namespace", "%table_name"}),
                new (new Ident[]{ "@namespace", "%table_name/path"}),
                new (new Ident[]{ "@namespace", "stage_name/path"}),
                new (new Ident("@~/path")),
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

            Assert.Throws<ParserException>(() => ParseSqlStatements("SELECT TRIM('xyz' 'a')", new[] { new BigQueryDialect() }));
        }

        [Fact]
        public void Parse_Subquery_Function_Argument()
        {
            // Snowflake allows passing an unparenthesized subquery as the single argument to a function.
            OneStatementParsesTo("SELECT parse_json(SELECT '{}')", "SELECT parse_json((SELECT '{}'))");

            // Subqueries that begin with WITH work too.
            OneStatementParsesTo(
                "SELECT parse_json(WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q)",
                "SELECT parse_json((WITH q AS (SELECT '{}' AS foo) SELECT foo FROM q))"
            );

            // Commas are parsed as part of the subquery, not additional arguments to
            // the function.
            OneStatementParsesTo("SELECT func(SELECT 1, 2)", "SELECT func((SELECT 1, 2))");
        }

        [Fact]
        public void Parse_Position_Not_Function_Columns()
        {
            VerifiedStatement("SELECT position FROM tbl1 WHERE position NOT IN ('first', 'last')");
        }
    }
}
