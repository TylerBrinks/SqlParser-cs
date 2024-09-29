using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests.Dialects;
public class MySqlDialectTests : ParserTestBase
{
    public MySqlDialectTests()
    {
        DefaultDialects = new[] { new MySqlDialect() };
    }

    [Fact]
    public void Parse_Identifier()
    {
        VerifiedStatement("SELECT $a$, àà");
    }

    [Fact]
    public void Parse_Literal_String()
    {
        var select = VerifiedOnlySelect("SELECT 'single', \"double\"");
        Assert.Equal(2, select.Projection.Count);
        Assert.Equal(new LiteralValue(new Value.SingleQuotedString("single")), select.Projection.First().AsExpr());
        Assert.Equal(new LiteralValue(new Value.DoubleQuotedString("double")), select.Projection.Last().AsExpr());
    }

    [Fact]
    public void Parse_Show_Columns()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        var tableName = new ObjectName("mytable");
        Assert.Equal(new Statement.ShowColumns(false, false, tableName),
            VerifiedStatement("SHOW COLUMNS FROM mytable"));

        tableName = new ObjectName(["mydb", "mytable"]);
        Assert.Equal(new Statement.ShowColumns(false, false, tableName),
            VerifiedStatement("SHOW COLUMNS FROM mydb.mytable"));

        tableName = new ObjectName("mytable");
        Assert.Equal(new Statement.ShowColumns(true, false, tableName),
            VerifiedStatement("SHOW EXTENDED COLUMNS FROM mytable"));
        Assert.Equal(new Statement.ShowColumns(false, true, tableName),
            VerifiedStatement("SHOW FULL COLUMNS FROM mytable"));

        var filter = new ShowStatementFilter.Like("pattern");
        Assert.Equal(new Statement.ShowColumns(false, false, tableName, filter),
            VerifiedStatement("SHOW COLUMNS FROM mytable LIKE 'pattern'"));

        var where = new ShowStatementFilter.Where(VerifiedExpr("1 = 2"));
        Assert.Equal(new Statement.ShowColumns(false, false, tableName, where),
            VerifiedStatement("SHOW COLUMNS FROM mytable WHERE 1 = 2"));

        OneStatementParsesTo("SHOW FIELDS FROM mytable", "SHOW COLUMNS FROM mytable");
        OneStatementParsesTo("SHOW COLUMNS IN mytable", "SHOW COLUMNS FROM mytable");
        OneStatementParsesTo("SHOW FIELDS IN mytable", "SHOW COLUMNS FROM mytable");
        OneStatementParsesTo("SHOW COLUMNS FROM mytable FROM mydb", "SHOW COLUMNS FROM mydb.mytable");
    }

    [Fact]
    public void Parse_Show_Tables()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        var show = VerifiedStatement<Statement.ShowTables>("SHOW TABLES");
        Assert.Equal(new Statement.ShowTables(false, false), show);

        show = VerifiedStatement<Statement.ShowTables>("SHOW TABLES FROM mydb");
        Assert.Equal(new Statement.ShowTables(false, false, "mydb"), show);

        show = VerifiedStatement<Statement.ShowTables>("SHOW EXTENDED TABLES");
        Assert.Equal(new Statement.ShowTables(true, false), show);

        show = VerifiedStatement<Statement.ShowTables>("SHOW FULL TABLES");
        Assert.Equal(new Statement.ShowTables(false, true), show);

        show = VerifiedStatement<Statement.ShowTables>("SHOW TABLES LIKE 'pattern'");
        Assert.Equal(new Statement.ShowTables(false, false, null, new ShowStatementFilter.Like("pattern")), show);

        OneStatementParsesTo("SHOW TABLES IN mydb", "SHOW TABLES FROM mydb");
    }

    [Fact]
    public void Parse_Show_Extended_Full()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        ParseSqlStatements("SHOW EXTENDED FULL TABLES");
        ParseSqlStatements("SHOW EXTENDED FULL COLUMNS FROM mytable");
        Assert.Throws<ParserException>(() => ParseSqlStatements("SHOW EXTENDED FULL CREATE TABLE mytable"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("SHOW EXTENDED FULL COLLATION"));
        Assert.Throws<ParserException>(() => ParseSqlStatements("SHOW EXTENDED FULL VARIABLES"));
    }

    [Fact]
    public void Parse_Show_Create()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        var name = new ObjectName("myident");

        foreach (var type in new[]
                 {
                     ShowCreateObject.Table,
                     ShowCreateObject.Trigger,
                     ShowCreateObject.Event,
                     ShowCreateObject.Function,
                     ShowCreateObject.Procedure,
                     ShowCreateObject.View
                 })
        {
            var statement = VerifiedStatement($"SHOW CREATE {type} myident");
            Assert.Equal(new Statement.ShowCreate(type, name), statement);
        }
    }

    [Fact]
    public void Parse_Show_Collation()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        Assert.Equal(new Statement.ShowCollation(), VerifiedStatement("SHOW COLLATION"));
        Assert.Equal(new Statement.ShowCollation(new ShowStatementFilter.Like("pattern")),
            VerifiedStatement("SHOW COLLATION LIKE 'pattern'"));
        Assert.Equal(new Statement.ShowCollation(new ShowStatementFilter.Where(VerifiedExpr("1 = 2"))),
            VerifiedStatement("SHOW COLLATION WHERE 1 = 2"));
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
    public void Parse_Set_Variables()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };
        VerifiedStatement("SET sql_mode = CONCAT(@@sql_mode, ',STRICT_TRANS_TABLES')");

        var expected = new Statement.SetVariable(true, false, new OneOrManyWithParens<ObjectName>.One("autocommit"), new[]
        {
            new LiteralValue(Number("1"))
        });

        Assert.Equal(expected, VerifiedStatement("SET LOCAL autocommit = 1"));
    }

    [Fact]
    public void Parse_Create_Table_Auto_Increment()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (bar INT PRIMARY KEY AUTO_INCREMENT)");

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal(new ColumnDef[]
        {
            new("bar", new DataType.Int(),
                Options: new ColumnOptionDef[]
                {
                    new(new ColumnOption.Unique(true)),
                    new(new ColumnOption.DialectSpecific(new[] {new Word("AUTO_INCREMENT")}))

                })
        }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Set_Enum()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (bar SET('a', 'b'), baz ENUM('a', 'b'))");

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal(new ColumnDef[]
        {
            new("bar", new DataType.Set(new[] {"a", "b"})),
            new("baz", new DataType.Enum(new[] {"a", "b"}))
        }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Engine_Default_Charset()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>(
                "CREATE TABLE foo (id INT(11)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3");

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal(new TableEngine("InnoDB"), create.Element.Engine);
        Assert.Equal("utf8mb3", create.Element.DefaultCharset);
        Assert.Equal(new ColumnDef[]
        {
            new("id", new DataType.Int(11))

        }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Collate()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (id INT(11)) COLLATE=utf8mb4_0900_ai_ci");

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal("utf8mb4_0900_ai_ci", create.Element.Collation);
        Assert.Equal(new ColumnDef[] { new("id", new DataType.Int(11)) }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Comment_Character_Set()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>(
                "CREATE TABLE foo (s TEXT CHARACTER SET utf8mb4 COMMENT 'comment')");

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal(new ColumnDef[]
        {
            new("s", new DataType.Text(),
                Options: new ColumnOptionDef[]
                {
                    new(new ColumnOption.CharacterSet("utf8mb4")),
                    new(new ColumnOption.Comment("comment"))
                })
        }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Quote_Identifiers()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE `PRIMARY` (`BEGIN` INT PRIMARY KEY)");

        Assert.Equal("`PRIMARY`", create.Element.Name);
        Assert.Equal(new ColumnDef[]
        {
            new(new Ident("BEGIN", Symbols.Backtick), new DataType.Int(), Options: new ColumnOptionDef[]
            {
                new(new ColumnOption.Unique(true))
            })
        }, create.Element.Columns);
    }

    [Fact]
    public void Parse_Escaped_Quote_Identifiers_With_Escape()
    {
        var query = VerifiedStatement<Statement.Select>("SELECT `quoted `` identifier`", unescape: true);
        var body = new SetExpression.SelectExpression(new Select(new[]
        {
            new SelectItem.UnnamedExpression(new Identifier(new Ident("quoted ` identifier", Symbols.Backtick)))
        }));
        var expected = new Statement.Select(new Query(body));

        Assert.Equal(expected, query);
    }

    [Fact]
    public void Parse_Escaped_Quote_Identifiers_No_Escape()
    {
        var query = VerifiedStatement<Statement.Select>("SELECT `quoted `` identifier`");
        var body = new SetExpression.SelectExpression(new Select(new[]
        {
            new SelectItem.UnnamedExpression(new Identifier(new Ident("quoted `` identifier", Symbols.Backtick)))
        }));
        var expected = new Statement.Select(new Query(body));

        Assert.Equal(expected, query);
    }

    [Fact]
    public void Parse_Escaped_Backticks_With_No_Escape()
    {
        const string sql = "SELECT ```quoted identifier```";

        var statement = VerifiedStatement(sql, new[] { new MySqlDialect() });

        var body = new SetExpression.SelectExpression(new Select([
            new SelectItem.UnnamedExpression(new Identifier(new Ident("``quoted identifier``", '`')))
        ]));
        var expected = new Query(body);

        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Unterminated_Escape()
    {
        var ex = Assert.Throws<TokenizeException>(() => OneStatementParsesTo("SELECT 'I\'m not fine\'", ""));
        Assert.Equal("Unterminated string literal. Expected ' after Line: 1, Col: 21", ex.Message);
        ex = Assert.Throws<TokenizeException>(() => OneStatementParsesTo("SELECT 'I\\\\'m not fine'", ""));
        Assert.Equal("Unterminated string literal. Expected ' after Line: 1, Col: 23", ex.Message);
    }

    [Fact]
    public void Parse_Escaped_String_With_Escape()
    {
        AssertMySqlQuotedString("SELECT 'I\\'m fine'", "I'm fine");
        AssertMySqlQuotedString("SELECT 'I''m fine'", "I'm fine");
        AssertMySqlQuotedString("SELECT 'I\"m fine'", "I\"m fine");

        void AssertMySqlQuotedString(string sql, string quoted)
        {
            var statement = OneStatementParsesTo(sql, "", unescape: true);
            var query = (Statement.Select)statement;
            var body = (SetExpression.SelectExpression)query.Query.Body;

            Assert.Equal(new LiteralValue(new Value.SingleQuotedString(quoted)),
                body.Select.Projection.Single().AsExpr());
        }
    }

    [Fact]
    public void Parse_Create_Table_With_Minimum_Display_Width()
    {
        const string sql =
            "CREATE TABLE foo (bar_tinyint TINYINT(3), bar_smallint SMALLINT(5), bar_mediumint MEDIUMINT(6), bar_int INT(11), bar_bigint BIGINT(20))";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        var expected = new ColumnDef[]
        {
            new("bar_tinyint", new DataType.TinyInt(3)),
            new("bar_smallint", new DataType.SmallInt(5)),
            new("bar_mediumint", new DataType.MediumInt(6)),
            new("bar_int", new DataType.Int(11)),
            new("bar_bigint", new DataType.BigInt(20)),
        };

        Assert.Equal("foo", create.Element.Name.Values[0]);
        Assert.Equal(expected, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Unsigned()
    {
        const string sql =
            "CREATE TABLE foo (bar_tinyint TINYINT(3) UNSIGNED, bar_smallint SMALLINT(5) UNSIGNED, bar_mediumint MEDIUMINT(13) UNSIGNED, bar_int INT(11) UNSIGNED, bar_bigint BIGINT(20) UNSIGNED)";
        var create = VerifiedStatement<Statement.CreateTable>(sql);

        var expected = new ColumnDef[]
        {
            new("bar_tinyint", new DataType.UnsignedTinyInt(3)),
            new("bar_smallint", new DataType.UnsignedSmallInt(5)),
            new("bar_mediumint", new DataType.UnsignedMediumInt(13)),
            new("bar_int", new DataType.UnsignedInt(11)),
            new("bar_bigint", new DataType.UnsignedBigInt(20)),
        };

        Assert.Equal("foo", create.Element.Name.Values[0]);
        Assert.Equal(expected, create.Element.Columns);
    }

    [Fact]
    public void Parse_Simple_Insert()
    {
        const string sql =
            "INSERT INTO tasks (title, priority) VALUES ('Test Some Inserts', 1), ('Test Entry 2', 2), ('Test Entry 3', 3)";
        var insert = VerifiedStatement<Statement.Insert>(sql);
        var body = new SetExpression.ValuesExpression(new Values(new Sequence<Expression>[]
        {
            [
                new LiteralValue(new Value.SingleQuotedString("Test Some Inserts")),
                new LiteralValue(Number("1"))
            ],
            [
                new LiteralValue(new Value.SingleQuotedString("Test Entry 2")),
                new LiteralValue(Number("2"))
            ],
            [
                new LiteralValue(new Value.SingleQuotedString("Test Entry 3")),
                new LiteralValue(Number("3"))
            ]
        }));
        var expected = new Query(body);

        Assert.Equal("tasks", insert.InsertOperation.Name);
        Assert.Equal(new Ident[] { "title", "priority" }, insert.InsertOperation.Columns!);
        Assert.Equal(expected, insert.InsertOperation.Source!.Query);
    }

    [Fact]
    public void Parse_Empty_Row_Insert()
    {
        ParseSqlStatements("INSERT INTO tb () VALUES (), ()");
        ParseSqlStatements("INSERT INTO tb VALUES (), ()");

        var insert = OneStatementParsesTo<Statement.Insert>(
            "INSERT INTO tb () VALUES (), ()",
            "INSERT INTO tb VALUES (), ()");

        Assert.Equal("tb", insert.InsertOperation.Name);
        Assert.Equal(new Statement.Select(
                new Query(new SetExpression.ValuesExpression(new Values(new Sequence<Expression>[]
                {
                    new(),
                    new()
                }))))
            , insert.InsertOperation.Source);
    }

    [Fact]
    public void Parse_Insert_With_On_Duplicate_Update()
    {
        const string sql =
            "INSERT INTO permission_groups (name, description, perm_create, perm_read, perm_update, perm_delete) VALUES ('accounting_manager', 'Some description about the group', true, true, true, true) ON DUPLICATE KEY UPDATE description = VALUES(description), perm_create = VALUES(perm_create), perm_read = VALUES(perm_read), perm_update = VALUES(perm_update), perm_delete = VALUES(perm_delete)";

        var insert = VerifiedStatement<Statement.Insert>(sql);

        Assert.Equal("permission_groups", insert.InsertOperation.Name);
        Assert.Equal(new Ident[] { "name", "description", "perm_create", "perm_read", "perm_update", "perm_delete" },
            insert.InsertOperation.Columns!);

        var rows = new Sequence<Expression>[]
        {
            [
                new LiteralValue(new Value.SingleQuotedString("accounting_manager")),
                new LiteralValue(new Value.SingleQuotedString("Some description about the group")),
                new LiteralValue(new Value.Boolean(true)),
                new LiteralValue(new Value.Boolean(true)),
                new LiteralValue(new Value.Boolean(true)),
                new LiteralValue(new Value.Boolean(true))
            ]
        };

        Assert.Equal(new Query(new SetExpression.ValuesExpression(new Values(rows))), (Query)insert.InsertOperation.Source!);

        var update = new OnInsert.DuplicateKeyUpdate(new Statement.Assignment[]
        {
            new(new AssignmentTarget.ColumnName("description") , new Function("VALUES")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("description")))
                ]))
            }),

            new(new AssignmentTarget.ColumnName("perm_create"), new Function("VALUES")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("perm_create")))
                ]))
            }),

            new(new AssignmentTarget.ColumnName("perm_read"), new Function("VALUES")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("perm_read")))
                ]))
            }),

            new(new AssignmentTarget.ColumnName("perm_update"), new Function("VALUES")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("perm_update")))
                ]))
            }),

            new(new AssignmentTarget.ColumnName("perm_delete"), new Function("VALUES")
            {
                Args = new FunctionArguments.List(new FunctionArgumentList([
                    new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Identifier("perm_delete")))
                ]))
            })
        });

        Assert.Equal(update, insert.InsertOperation.On);
    }

    [Fact]
    public void Parse_Update_With_Joins()
    {
        const string sql =
            "UPDATE orders AS o JOIN customers AS c ON o.customer_id = c.id SET o.completed = true WHERE c.firstname = 'Peter'";

        var update = VerifiedStatement<Statement.Update>(sql);

        var table = new TableWithJoins(new TableFactor.Table("orders")
        {
            Alias = new TableAlias("o")
        })
        {
            Joins = new Join[]
            {
                new(new TableFactor.Table("customers")
                {
                    Alias = new TableAlias("c")
                })
                {
                    JoinOperator = new JoinOperator.Inner(new JoinConstraint.On(new BinaryOp(
                        new CompoundIdentifier(new Ident[] {"o", "customer_id"}),
                        BinaryOperator.Eq,
                        new CompoundIdentifier(new Ident[] {"c", "id"})
                    )))
                }
            }
        };

        var assignments = new Statement.Assignment[]
        {
            new(new AssignmentTarget.ColumnName(new ObjectName(["o", "completed"])), new LiteralValue(new Value.Boolean(true)))
        };

        var op = new BinaryOp(
            new CompoundIdentifier(new Ident[] { "c", "firstname" }),
            BinaryOperator.Eq,
            new LiteralValue(new Value.SingleQuotedString("Peter"))
        );

        Assert.Equal(table, update.Table);
        Assert.Equal(assignments, update.Assignments);
        Assert.Equal(op, update.Selection);
    }

    [Fact]
    public void Parse_Alter_Table_Drop_Primary_Key()
    {
        DefaultDialects = [new MySqlDialect(), new GenericDialect()];

        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab DROP PRIMARY KEY");

        Assert.Equal("tab", alter.Name);
    }

    [Fact]
    public void Parse_Alter_Table_Change_Column()
    {
        var alter = VerifiedStatement<Statement.AlterTable>(
            "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL");

        var operation = new AlterTableOperation.ChangeColumn("description", "desc", new DataType.Text(),
            new[]
            {
                new ColumnOption.NotNull()
            });

        Assert.Equal("orders", alter.Name);
        Assert.Equal(operation, alter.Operations.First());

        alter = VerifiedStatement<Statement.AlterTable>(
            "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL");
        Assert.Equal("orders", alter.Name);
        Assert.Equal(operation, alter.Operations.First());

        var expectedOperation = new AlterTableOperation.ChangeColumn(
            "description", "desc", new DataType.Text(), [new ColumnOption.NotNull()],
            new MySqlColumnPosition.First());

        var alterTable = VerifiedStatement<Statement.AlterTable>("ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL FIRST");
        Assert.Equal(expectedOperation, alterTable.Operations.First());


        alterTable = VerifiedStatement<Statement.AlterTable>("ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL AFTER foo");
        expectedOperation = new AlterTableOperation.ChangeColumn(
            "description", "desc", new DataType.Text(), [new ColumnOption.NotNull()],
            new MySqlColumnPosition.After("foo"));
        Assert.Equal(expectedOperation, alterTable.Operations.First());
    }

    [Fact]
    public void Parse_Alter_Table_Change_Column_With_Column_Position()
    {
        AlterTableOperation expectedOperation = new AlterTableOperation.ChangeColumn("description", "desc",
            new DataType.Text(),
            [new ColumnOption.NotNull()], new MySqlColumnPosition.First());
        var sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL FIRST";

        var operation = VerifiedStatement<Statement.AlterTable>(sql1).Operations.First();
        Assert.Equal(expectedOperation, operation);

        expectedOperation = new AlterTableOperation.ChangeColumn("description", "desc", new DataType.Text(),
            [new ColumnOption.NotNull()], new MySqlColumnPosition.First());

        var sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL FIRST";
        operation = OneStatementParsesTo<Statement.AlterTable>(sql2, sql1).Operations.First();
        Assert.Equal(expectedOperation, operation);

        expectedOperation = new AlterTableOperation.ChangeColumn("description", "desc", new DataType.Text(),
            [new ColumnOption.NotNull()], new MySqlColumnPosition.After("total_count"));

        sql1 = "ALTER TABLE orders CHANGE COLUMN description desc TEXT NOT NULL AFTER total_count";
        operation = VerifiedStatement<Statement.AlterTable>(sql1).Operations.First();
        Assert.Equal(expectedOperation, operation);

        sql2 = "ALTER TABLE orders CHANGE description desc TEXT NOT NULL AFTER total_count";
        operation = OneStatementParsesTo<Statement.AlterTable>(sql2, sql1).Operations.First();
        Assert.Equal(expectedOperation, operation);
    }


    [Fact]
    public void Parse_Substring_In_Select()
    {
        var query = OneStatementParsesTo<Statement.Select>(
            "SELECT DISTINCT SUBSTRING(description, 0, 1) FROM test",
            "SELECT DISTINCT SUBSTRING(description FROM 0 FOR 1) FROM test");
        var body = new SetExpression.SelectExpression(new Select(new[]
        {
            new SelectItem.UnnamedExpression(new Substring(
                new Identifier("description"),
                new LiteralValue(Number("0")),
                new LiteralValue(Number("1"))
            ))
        })
        {
            Distinct = new DistinctFilter.Distinct(),
            From = new TableWithJoins[]
            {
                new(new TableFactor.Table("test"))
            }
        });
        var expected = new Statement.Select(new Query(body));

        Assert.Equal(expected, query);
    }

    [Fact]
    public void Parse_Show_Variables()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        VerifiedStatement("SHOW VARIABLES");
        VerifiedStatement("SHOW VARIABLES LIKE 'admin%'");
        VerifiedStatement("SHOW VARIABLES WHERE value = '3306'");
        VerifiedStatement("SHOW GLOBAL VARIABLES");
        VerifiedStatement("SHOW GLOBAL VARIABLES LIKE 'admin%'");
        VerifiedStatement("SHOW GLOBAL VARIABLES WHERE value = '3306'");
        VerifiedStatement("SHOW SESSION VARIABLES");
        VerifiedStatement("SHOW SESSION VARIABLES LIKE 'admin%'");
        VerifiedStatement("SHOW GLOBAL VARIABLES WHERE value = '3306'");
    }

    [Fact]
    public void Parse_Kill()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        var kill = VerifiedStatement<Statement.Kill>("KILL CONNECTION 5");
        Assert.Equal(new Statement.Kill(KillType.Connection, 5), kill);

        kill = VerifiedStatement<Statement.Kill>("KILL QUERY 5");
        Assert.Equal(new Statement.Kill(KillType.Query, 5), kill);

        kill = VerifiedStatement<Statement.Kill>("KILL 5");
        Assert.Equal(new Statement.Kill(KillType.None, 5), kill);
    }

    [Fact]
    public void Public_Table_Column_Option_On_Update()
    {
        var create =
            VerifiedStatement<Statement.CreateTable>(
                "CREATE TABLE foo (`modification_time` DATETIME ON UPDATE CURRENT_TIMESTAMP())");
        Assert.Equal("foo", create.Element.Name);

        Assert.Equal([
            new(new Ident("modification_time", Symbols.Backtick), new DataType.Datetime(),
                Options: new ColumnOptionDef[]
                {
                    new(Option: new ColumnOption.OnUpdate(new Function("CURRENT_TIMESTAMP")
                    {
                        Args = new FunctionArguments.List(FunctionArgumentList.Empty())
                    }))

                })
        ], create.Element.Columns);
    }

    [Fact]
    public void Parse_Set_Names()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        var set = VerifiedStatement<Statement.SetNames>("SET NAMES utf8mb4");
        Assert.Equal("utf8mb4", set.CharsetName);

        set = VerifiedStatement<Statement.SetNames>("SET NAMES utf8mb4 COLLATE bogus");
        Assert.Equal("utf8mb4", set.CharsetName);
        Assert.Equal("bogus", set.CollationName);

        set = VerifiedStatement<Statement.SetNames>("set names utf8mb4 collate bogus");
        Assert.Equal("utf8mb4", set.CharsetName);

        var def = VerifiedStatement<Statement.SetNamesDefault>("SET NAMES DEFAULT");
        Assert.Equal(new Statement.SetNamesDefault(), def);
    }

    [Fact]
    public void Parse_Limit_MySql_Syntax()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        OneStatementParsesTo(
            "SELECT id, fname, lname FROM customer LIMIT 5, 10",
            "SELECT id, fname, lname FROM customer LIMIT 10 OFFSET 5");
    }

    [Fact]
    public void Parse_Create_Table_With_Index_Definition()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, INDEX (id))",
            "CREATE TABLE tb (id INT, INDEX (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, index USING BTREE (id))",
            "CREATE TABLE tb (id INT, INDEX USING BTREE (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, KEY USING HASH (id))",
            "CREATE TABLE tb (id INT, KEY USING HASH (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, key index (id))",
            "CREATE TABLE tb (id INT, KEY index (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, INDEX 'index' (id))",
            "CREATE TABLE tb (id INT, INDEX 'index' (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, INDEX index USING BTREE (id))",
            "CREATE TABLE tb (id INT, INDEX index USING BTREE (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, INDEX index USING HASH (id))",
            "CREATE TABLE tb (id INT, INDEX index USING HASH (id))");

        OneStatementParsesTo(
            "CREATE TABLE tb (id INT, INDEX (c1, c2, c3, c4,c5))",
            "CREATE TABLE tb (id INT, INDEX (c1, c2, c3, c4, c5))");
    }

    [Fact]
    public void Parse_Create_Table_With_Fulltext_Definition()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT INDEX (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT KEY (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT potato (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT INDEX potato (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, FULLTEXT KEY potato (id))");
        VerifiedStatement("CREATE TABLE tb (c1 INT, c2 INT, FULLTEXT KEY potato (c1, c2))");
    }

    [Fact]
    public void Parse_Create_Table_With_Special_Definition()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL INDEX (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL KEY (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL potato (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL INDEX potato (id))");
        VerifiedStatement("CREATE TABLE tb (id INT, SPATIAL KEY potato (id))");
        VerifiedStatement("CREATE TABLE tb (c1 INT, c2 INT, SPATIAL KEY potato (c1, c2))");
    }

    [Fact]
    public void Parse_Fulltext_Expression()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string')");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN NATURAL LANGUAGE MODE)");
        VerifiedStatement(
            "SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' IN BOOLEAN MODE)");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST ('string' WITH QUERY EXPANSION)");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1, c2, c3) AGAINST ('string')");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST (123)");
        VerifiedStatement("SELECT * FROM tb WHERE MATCH (c1) AGAINST (NULL)");
        VerifiedStatement(
            "SELECT COUNT(IF(MATCH (title, body) AGAINST ('database' IN NATURAL LANGUAGE MODE), 1, NULL)) AS count FROM articles");
    }

    [Fact]
    public void Parse_Create_Table_With_Fulltext_Definition_Should_Not_Accept_Constraint_Name()
    {
        DefaultDialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        Assert.Throws<ParserException>(() =>
            VerifiedStatement("CREATE TABLE tb (c1 INT, CONSTRAINT cons FULLTEXT (c1))"));
    }

    [Fact]
    public void Parse_Values()
    {
        VerifiedStatement("VALUES ROW(1, true, 'a')");
        VerifiedStatement(
            "SELECT a, c FROM (VALUES ROW(1, true, 'a'), ROW(2, false, 'b'), ROW(3, false, 'c')) AS t (a, b, c)");
    }

    [Fact]
    public void Parse_Hex_String_Introducer()
    {
        var query = VerifiedStatement<Statement.Select>("SELECT _latin1 X'4D7953514C'");

        var projection = new SelectItem[]
        {
            new SelectItem.UnnamedExpression(new IntroducedString("_latin1",
                new Value.HexStringLiteral("4D7953514C")))
        };

        Assert.Equal(projection, ((SetExpression.SelectExpression)query.Query.Body).Select.Projection);
    }

    [Fact]
    // ReSharper disable once IdentifierTypo
    public void Parse_String_Introducers()
    {
        VerifiedStatement("SELECT _binary 'abc'");
        OneStatementParsesTo("SELECT _utf8'abc'", "SELECT _utf8 'abc'");
        OneStatementParsesTo("SELECT _utf8mb4'abc'", "SELECT _utf8mb4 'abc'");
        VerifiedStatement("SELECT _binary 'abc', _utf8mb4 'abc'");
    }

    [Fact]
    public void Parse_Select_With_Numeric_Prefix_Column_Name()
    {
        const string sql = "SELECT 123col_$@123abc FROM \"table\"";
        var select = VerifiedOnlySelect(sql);
        Assert.Equal(new Identifier(new Ident("123col_$@123abc")), select.Projection.First().AsExpr());
    }

    [Fact]
    public void Parse_Div_Infix()
    {
        const string sql = "SELECT 5 DIV 2";
        VerifiedOnlySelect(sql);
    }

    [Fact]
    public void Parse_Drop_Table()
    {
        var drop = VerifiedStatement<Statement.Drop>("DROP TEMPORARY TABLE foo");

        Assert.True(drop.Temporary);
    }

    [Fact]
    public void Parse_Create_Table_Unique_Key()
    {
        const string sql =
            "CREATE TABLE foo (id INT PRIMARY KEY AUTO_INCREMENT, bar INT NOT NULL, UNIQUE KEY bar_key (bar))";
        const string canonical =
            "CREATE TABLE foo (id INT PRIMARY KEY AUTO_INCREMENT, bar INT NOT NULL, CONSTRAINT bar_key UNIQUE (bar))";

        var create = (Statement.CreateTable)OneStatementParsesTo(sql, canonical, new[] { new MySqlDialect() });

        var constraints = new Sequence<TableConstraint>
        {
            new TableConstraint.Unique(["bar"]){ Name = "bar_key" }
        };

        Assert.Equal("foo", create.Element.Name);
        Assert.Equal(constraints, create.Element.Constraints);

        var columns = new Sequence<ColumnDef>
        {
            new("id", new DataType.Int(), Options: new Sequence<ColumnOptionDef>
            {
                new(new ColumnOption.Unique(true)),
                new(new ColumnOption.DialectSpecific([new Word("AUTO_INCREMENT")]))
            }),
            new("bar", new DataType.Int(), Options: new Sequence<ColumnOptionDef>
            {
                new(new ColumnOption.NotNull())
            })
        };
        Assert.Equal(columns, create.Element.Columns);
    }

    [Fact]
    public void Parse_Create_Table_Comment()
    {
        const string canonical = "CREATE TABLE foo (bar INT) COMMENT 'baz'";
        const string withEqual = "CREATE TABLE foo (bar INT) COMMENT = 'baz'";

        foreach (var sql in new[] { canonical, withEqual })
        {
            var create = (Statement.CreateTable)OneStatementParsesTo(sql, canonical);
            Assert.Equal("foo", create.Element.Name);
            Assert.Equal("baz", create.Element.Comment!.Comment);
        }
    }

    [Fact]
    public void Parse_Alter_Role()
    {
        var sql = "ALTER ROLE old_name WITH NAME = new_name";
        var dialect = new[] { new MsSqlDialect() };
        var alter = ParseSqlStatements(sql, dialect);
        var expected = new Statement.AlterRole("old_name", new AlterRoleOperation.RenameRole("new_name"));
        Assert.Equal(expected, alter.First());

        sql = "ALTER ROLE role_name ADD MEMBER new_member";
        var statement = VerifiedStatement(sql, dialect);
        expected = new Statement.AlterRole("role_name", new AlterRoleOperation.AddMember("new_member"));
        Assert.Equal(expected, statement);

        sql = "ALTER ROLE role_name DROP MEMBER old_member";
        statement = VerifiedStatement(sql, dialect);
        expected = new Statement.AlterRole("role_name", new AlterRoleOperation.DropMember("old_member"));
        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Create_Table_Auto_Increment_Offset()
    {
        const string canonical = "CREATE TABLE foo (bar INT NOT NULL AUTO_INCREMENT) ENGINE=InnoDB AUTO_INCREMENT 123";
        const string withEqual = "CREATE TABLE foo(bar INT NOT NULL AUTO_INCREMENT) ENGINE = InnoDB AUTO_INCREMENT = 123";

        foreach (var sql in new[] { canonical, withEqual })
        {
            var create = (Statement.CreateTable)OneStatementParsesTo(sql, canonical);

            Assert.Equal(123, create.Element.AutoIncrementOffset!.Value);
        }
    }

    [Fact]
    public void Parse_Attach_Database()
    {
        const string sql = "ATTACH DATABASE 'test.db' AS test";
        var statement = VerifiedStatement(sql);

        Assert.Equal(sql, statement.ToSql());

        var expected = new Statement.AttachDatabase("test", new LiteralValue(new Value.SingleQuotedString("test.db")), true);
        Assert.Equal(expected, statement);
    }

    [Fact]
    public void Parse_Delete_With_Order_By()
    {
        const string sql = "DELETE FROM customers ORDER BY id DESC";
        var delete = VerifiedStatement(sql);

        var from = new FromTable.WithFromKeyword([new(new TableFactor.Table("customers"))]);
        var expected = new Statement.Delete(new DeleteOperation(null, from,
            OrderBy: new Sequence<OrderByExpression>
            {
                new (new Identifier("id"), Asc:false)
            }));

        Assert.Equal(expected, delete);
    }

    [Fact]
    public void Parse_Delete_With_Limit()
    {
        const string sql = "DELETE FROM customers LIMIT 100";
        var delete = VerifiedStatement(sql);
        var from = new FromTable.WithFromKeyword([new(new TableFactor.Table("customers"))]);
        var expected = new Statement.Delete(new DeleteOperation(null, from,
            Limit: new LiteralValue(new Value.Number("100"))
        ));
        Assert.Equal(expected, delete);
    }

    [Fact]
    public void Parse_Rlike_And_Regexp()
    {
        var queries = new[]{
                "SELECT 1 WHERE 'a' RLIKE '^a$'",
                "SELECT 1 WHERE 'a' REGEXP '^a$'",
                "SELECT 1 WHERE 'a' NOT RLIKE '^a$'",
                "SELECT 1 WHERE 'a' NOT REGEXP '^a$'",
            };

        var dialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };
        foreach (var sql in queries)
        {
            VerifiedOnlySelect(sql, dialects);
        }
    }

    [Fact]
    public void Parse_Ignore_Insert()
    {
        const string sql = "INSERT IGNORE INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";

        var dialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };

        var insert = VerifiedStatement(sql, dialects);

        var query = new Query(new SetExpression.ValuesExpression(new Values([
            [
                new LiteralValue(new Value.SingleQuotedString("Test Some Inserts")),
                new LiteralValue(new Value.Number("1"))
            ]
        ])));

        var expected = new Statement.Insert(new InsertOperation("tasks", new Statement.Select(query))
        {
            Ignore = true,
            Into = true,
            Columns = new Sequence<Ident>
            {
                "title",
                "priority"
            }
        });

        Assert.Equal(expected, insert);
    }

    [Fact]
    public void Parse_Convert_Using()
    {
        // CONVERT(expr USING transcoding_name)
        VerifiedOnlySelect("SELECT CONVERT('x' USING latin1)");
        VerifiedOnlySelect("SELECT CONVERT(my_column USING utf8mb4) FROM my_table");

        // CONVERT(expr, type)
        VerifiedOnlySelect("SELECT CONVERT('abc', CHAR(60))");
        VerifiedOnlySelect("SELECT CONVERT(123.456, DECIMAL(5,2))");
        // with a type + a charset
        VerifiedOnlySelect("SELECT CONVERT('test', CHAR CHARACTER SET utf8mb4)");
    }

    [Fact]
    public void Parse_Create_Table_Gencol()
    {
        var dialects = new Dialect[] { new MySqlDialect(), new GenericDialect() };
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2))", dialects);
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)", dialects);
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)", dialects);

        VerifiedStatement("CREATE TABLE t1 (a INT, b INT AS (a * 2))", dialects);
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT AS (a * 2) VIRTUAL)", dialects);
        VerifiedStatement("CREATE TABLE t1 (a INT, b INT AS (a * 2) STORED)", dialects);
    }

    [Fact]
    public void Parse_Json_Table()
    {
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[[1, 2], [3, 4]]', '$[*]' COLUMNS(a INT PATH '$[0]', b INT PATH '$[1]')) AS t");
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[\"x\", \"y\"]', '$[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t");

        // with a bound parameter
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE(?, '$[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t");

        // quote escaping
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('{\"''\": [1,2,3]}', '$.\"''\"[*]' COLUMNS(a VARCHAR(20) PATH '$')) AS t");

        // double quotes
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE(\"[]\", \"$[*]\" COLUMNS(a VARCHAR(20) PATH \"$\")) AS t");

        // exists
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[{}, {\"x\":1}]', '$[*]' COLUMNS(x INT EXISTS PATH '$.x')) AS t");

        // error handling
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON ERROR)) AS t");
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON EMPTY)) AS t");
        VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' ERROR ON EMPTY DEFAULT '0' ON ERROR)) AS t");

        var joinTable = VerifiedOnlySelect("SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(x INT PATH '$' DEFAULT '0' ON EMPTY NULL ON ERROR)) AS t");

        var expected = new TableFactor.JsonTable(
            new LiteralValue(new Value.SingleQuotedString("[1,2]")),
            new Value.SingleQuotedString("$[*]"),
            [
                new("x", new DataType.Int(), new Value.SingleQuotedString("$"),
                    false, new JsonTableColumnErrorHandling.Default(new Value.SingleQuotedString("0")),
                    new JsonTableColumnErrorHandling.Null())
            ])
        {
            Alias = new TableAlias("t")
        };

        Assert.Equal(expected, joinTable.From![0].Relation);
    }

    [Fact]
    public void Parse_Create_Table_With_Column_Collate()
    {
        var sql = "CREATE TABLE tb (id TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci)";
        var canonical = "CREATE TABLE tb (id TEXT COLLATE utf8mb4_0900_ai_ci CHARACTER SET utf8mb4)";

        var create = OneStatementParsesTo(sql, canonical);

        var expected = new Statement.CreateTable(new CreateTable("tb", [
            new("id", new DataType.Text(), "utf8mb4_0900_ai_ci", new Sequence<ColumnOptionDef>
            {
                new(new ColumnOption.CharacterSet("utf8mb4"))
            })
        ]));

        Assert.Equal(expected, create);
    }

    [Fact]
    public void Parse_Lock_Tables()
    {
        OneStatementParsesTo("LOCK TABLES trans t READ, customer WRITE", "LOCK TABLES trans AS t READ, customer WRITE");

        //VerifiedStatement("LOCK TABLES trans AS t READ, customer WRITE");
        //VerifiedStatement("LOCK TABLES trans AS t READ LOCAL, customer WRITE");
        //VerifiedStatement("LOCK TABLES trans AS t READ, customer LOW_PRIORITY WRITE");
        //VerifiedStatement("UNLOCK TABLES");
    }

    [Fact]
    public void Parse_Priority_Insert()
    {
        var sql = "INSERT HIGH_PRIORITY INTO tasks (title, priority) VALUES ('Test Some Inserts', 1)";

        var insert = VerifiedStatement(sql);

        var select = new Statement.Select(new Query(new SetExpression.ValuesExpression(
            new Values([
                [
                    new LiteralValue(new Value.SingleQuotedString("Test Some Inserts")),
                    new LiteralValue(new Value.Number("1"))
                ]
            ]))));

        var expected = new Statement.Insert(new InsertOperation("tasks", select)
        {
            Into = true,
            Columns = new Sequence<Ident> { "title", "priority" },
            Priority = MySqlInsertPriority.HighPriority
        });

        Assert.Equal(expected, insert);
    }

    [Fact]
    public void Parse_Flush()
    {
        var flush = (Statement.Flush)VerifiedStatement("FLUSH OPTIMIZER_COSTS");
        var expected = new Statement.Flush(FlushType.OptimizerCosts, null, null, false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH BINARY LOGS");
        expected = new Statement.Flush(FlushType.BinaryLogs, null, null, false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH ENGINE LOGS");
        expected = new Statement.Flush(FlushType.EngineLogs, null, null, false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH ERROR LOGS");
        expected = new Statement.Flush(FlushType.ErrorLogs, null, null, false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH RELAY LOGS FOR CHANNEL test");
        expected = new Statement.Flush(FlushType.RelayLogs, null, "test", false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH LOCAL SLOW LOGS");
        expected = new Statement.Flush(FlushType.SlowLogs, new FlushLocation.Local(), null, false, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH TABLES `mek`.`table1`, table2");
        expected = new Statement.Flush(FlushType.Tables, null, null, false, false,
            new Sequence<ObjectName>
            {
                new (new Ident[]
                {
                    new ("mek", Symbols.Backtick),
                    new ("table1", Symbols.Backtick),
                }),
                new ("table2")
            });
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH TABLES WITH READ LOCK");
        expected = new Statement.Flush(FlushType.Tables, null, null, true, false, null);
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH TABLES `mek`.`table1`, table2 WITH READ LOCK");
        expected = new Statement.Flush(FlushType.Tables, null, null, true, false,
            new Sequence<ObjectName>
            {
                new (new Ident[]
                {
                    new ("mek", Symbols.Backtick),
                    new ("table1", Symbols.Backtick),
                }),
                new ("table2")
            });
        Assert.Equal(expected, flush);

        flush = (Statement.Flush)VerifiedStatement("FLUSH TABLES `mek`.`table1`, table2 FOR EXPORT");
        expected = new Statement.Flush(FlushType.Tables, null, null, false, true,
            new Sequence<ObjectName>
            {
                new (new Ident[]
                {
                    new ("mek", Symbols.Backtick),
                    new ("table1", Symbols.Backtick),
                }),
                new ("table2")
            });
        Assert.Equal(expected, flush);
    }

    [Fact]
    public void Parse_Show_Status()
    {
        var show = (Statement.ShowStatus)VerifiedStatement("SHOW SESSION STATUS LIKE 'ssl_cipher'");
        var expected = new Statement.ShowStatus(new ShowStatementFilter.Like("ssl_cipher"), true, false);
        Assert.Equal(expected, show);

        show = (Statement.ShowStatus)VerifiedStatement("SHOW GLOBAL STATUS LIKE 'ssl_cipher'");
        expected = new Statement.ShowStatus(new ShowStatementFilter.Like("ssl_cipher"), false, true);
        Assert.Equal(expected, show);

        show = (Statement.ShowStatus)VerifiedStatement("SHOW STATUS WHERE value = 2");
        expected = new Statement.ShowStatus(new ShowStatementFilter.Where(VerifiedExpr("value = 2")), false, false);
        Assert.Equal(expected, show);
    }

    [Fact]
    public void Parse_Insert_As()
    {
        var statement = VerifiedStatement("INSERT INTO `table` (`date`) VALUES ('2024-01-01') AS `alias`");

        var values = new Values([[new LiteralValue(new Value.SingleQuotedString("2024-01-01"))]]);
        var source = new Statement.Select(new Query(new SetExpression.ValuesExpression(values)));
        var expected = new Statement.Insert(new InsertOperation(
            new ObjectName(new Ident("table", Symbols.Backtick)), source)
        {
            Into = true,
            Columns = new Sequence<Ident>
            {
                new ("date", Symbols.Backtick)
            },
            InsertAlias = new InsertAliases(new ObjectName(new Ident("alias", Symbols.Backtick)), new Sequence<Ident>())
        });

        Assert.Equal(expected, statement);

        Assert.Throws<ParserException>(() => ParseSqlStatements("INSERT INTO `table` (`date`) VALUES ('2024-01-01') AS `alias` ()"));

        statement = VerifiedStatement("INSERT INTO `table` (`id`, `date`) VALUES (1, '2024-01-01') AS `alias` (`mek_id`, `mek_date`)");

        values = new Values([[
            new LiteralValue(new Value.Number("1")),
            new LiteralValue(new Value.SingleQuotedString("2024-01-01")),
        ]]);
        source = new Statement.Select(new Query(new SetExpression.ValuesExpression(values)));
        expected = new Statement.Insert(
            new InsertOperation(new ObjectName(new Ident("table", Symbols.Backtick)), source)
            {
                Into = true,
                Columns = new Sequence<Ident>
            {
                new ("id", Symbols.Backtick),
                new ("date", Symbols.Backtick)
            },
                InsertAlias = new InsertAliases(new ObjectName(new Ident("alias", Symbols.Backtick)),
                new Sequence<Ident>
                {
                    new ("mek_id", Symbols.Backtick),
                    new ("mek_date", Symbols.Backtick)
                })
            });

        Assert.Equal(expected, statement);
    }


    [Fact]
    public void Parse_Alter_Table_Add_Column()
    {
        var alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD COLUMN b INT FIRST");

        var expected = new Statement.AlterTable("tab", false, false, [
            new AlterTableOperation.AddColumn(
                true, false, new ColumnDef("b", new DataType.Int()), new MySqlColumnPosition.First())
        ], null);
        Assert.Equal(expected, alter);

        alter = VerifiedStatement<Statement.AlterTable>("ALTER TABLE tab ADD COLUMN b INT AFTER foo");
        expected = new Statement.AlterTable("tab", false, false, [
            new AlterTableOperation.AddColumn(
                true, false, new ColumnDef("b", new DataType.Int()), new MySqlColumnPosition.After("foo"))
        ], null);
        Assert.Equal(expected, alter);
    }


    [Fact]
    public void Parse_Alter_Table_Modify_Column()
    {
        var sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL";
        var expectedName = new ObjectName("orders");
        var expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(), [new ColumnOption.NotNull()], null);
        var operation = AlterTableOpWithName(VerifiedStatement(sql1), expectedName);
        Assert.Equal(expectedOperation, operation);

        operation = AlterTableOpWithName(OneStatementParsesTo("ALTER TABLE orders MODIFY description TEXT NOT NULL", sql1), expectedName);
        Assert.Equal(expectedOperation, operation);

        operation = AlterTableOpWithName(VerifiedStatement("ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL FIRST"), expectedName);
        expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(), [new ColumnOption.NotNull()],
            new MySqlColumnPosition.First());
        Assert.Equal(expectedOperation, operation);

        expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(),
            [new ColumnOption.NotNull()],
            new MySqlColumnPosition.After("foo"));

        operation = AlterTableOpWithName(VerifiedStatement("ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL AFTER foo"), expectedName);
        Assert.Equal(expectedOperation, operation);
    }

    [Fact]
    public void Parse_Alter_Table_Modify_Column_With_Column_Position()
    {
        var sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL FIRST";
        var expectedName = new ObjectName("orders");
        var expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(), [new ColumnOption.NotNull()],
            new MySqlColumnPosition.First());
        var operation = AlterTableOpWithName(VerifiedStatement(sql1), expectedName);
        Assert.Equal(expectedOperation, operation);

        operation = AlterTableOpWithName(OneStatementParsesTo("ALTER TABLE orders MODIFY description TEXT NOT NULL FIRST", sql1), expectedName);
        expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(), [new ColumnOption.NotNull()],
            new MySqlColumnPosition.First());
        Assert.Equal(expectedOperation, operation);

        sql1 = "ALTER TABLE orders MODIFY COLUMN description TEXT NOT NULL AFTER total_count";
        expectedOperation = new AlterTableOperation.ModifyColumn("description", new DataType.Text(),
            [new ColumnOption.NotNull()],
            new MySqlColumnPosition.After("total_count"));
        operation = AlterTableOpWithName(VerifiedStatement(sql1), expectedName);
        Assert.Equal(expectedOperation, operation);

        operation = AlterTableOpWithName(OneStatementParsesTo("ALTER TABLE orders MODIFY description TEXT NOT NULL AFTER total_count", sql1), expectedName);
        Assert.Equal(expectedOperation, operation);
    }

    [Fact]
    public void Test_Group_Concat()
    {
        VerifiedExpr("GROUP_CONCAT(DISTINCT test_score)");
        VerifiedExpr("GROUP_CONCAT(test_score ORDER BY test_score)");
        VerifiedExpr("GROUP_CONCAT(test_score SEPARATOR ' ')");
        VerifiedExpr("GROUP_CONCAT(DISTINCT test_score ORDER BY test_score DESC SEPARATOR ' ')");
    }

    [Fact]
    public void Parse_Create_Table_Both_Options_And_As_Query()
    {
        var create = VerifiedStatement<Statement.CreateTable>("CREATE TABLE foo (id INT(11)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb4_0900_ai_ci AS SELECT 1").Element;

        Assert.Equal("foo", create.Name);
        Assert.Equal("utf8mb4_0900_ai_ci", create.Collation);
        Assert.Equal(new Sequence<SelectItem.UnnamedExpression>
        {
            new (new LiteralValue(new Value.Number("1")))
        }, create.Query!.Body.AsSelect().Projection);

        Assert.Throws<ParserException>(() => ParseSqlStatements("CREATE TABLE foo (id INT(11)) ENGINE=InnoDB AS SELECT 1 DEFAULT CHARSET=utf8mb3"));
    }
}