using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using DataType = SqlParser.Ast.DataType;

// ReSharper disable StringLiteralTypo
// ReSharper disable CommentTypo
// ReSharper disable AccessToModifiedClosure

namespace SqlParser.Tests;

public class ParserTests : ParserTestBase
{
    [Fact]
    public void Parser_Adjusts_Culture_Number_Separator()
    {
        var parser = new Parser();
        _ = parser.ParseSql("select a from tbl where b = 123.45");
        // Successful test will not throw an exception
    }

    [Fact]
    public void Test_Previous_Index()
    {
        AllDialects.RunParserMethod("SELECT version", parser =>
        {
            var word = new Word("SELECT");
            var p = parser.PeekToken();

            Assert.Equal(word, p);

            Assert.Equal(parser.PeekToken(), new Word("SELECT"));
            Assert.Equal(parser.NextToken(), new Word("SELECT"));
            parser.PrevToken();
            Assert.Equal(parser.NextToken(), new Word("SELECT"));
            Assert.Equal(parser.NextToken(), new Word("version"));
            parser.PrevToken();
            Assert.Equal(parser.PeekToken(), new Word("version"));
            Assert.Equal(parser.NextToken(), new Word("version"));
            Assert.Equal(parser.PeekToken(), new EOF());
            parser.PrevToken();
            Assert.Equal(parser.NextToken(), new Word("version"));
            Assert.Equal(parser.NextToken(), new EOF());
            Assert.Equal(parser.NextToken(), new EOF());
            parser.PrevToken();
        });
    }

    [Fact]
    public void Test_Parse_Limit()
    {
        var sql = "SELECT * FROM user LIMIT 1";
        AllDialects.RunParserMethod(sql, parser =>
        {
            var query = parser.ParseQuery();

            Assert.Equal(sql, query.ToSql());
        });

        sql = "SELECT * FROM user LIMIT $1 OFFSET $2";
        var dialects = new Dialect[]
        {
            new PostgreSqlDialect(),
            new ClickHouseDialect(),
            new GenericDialect(),
            new MsSqlDialect(),
            new SnowflakeDialect()
        };
        dialects.RunParserMethod(sql, parser =>
        {
            var query = parser.ParseQuery();

            Assert.Equal(sql, query.ToSql());
        });

        dialects = [new MsSqlDialect()];

        sql = "SELECT * FROM user LIMIT ? OFFSET ?";
        dialects.RunParserMethod(sql, parser =>
        {
            var query = parser.ParseQuery();

            Assert.Equal(sql, query.ToSql());
        });
    }

    [Fact]
    public void Test_Parse_Data_Type()
    {
        DefaultDialects = [new GenericDialect(), new AnsiDialect()];

        TestDataType("CHARACTER", new DataType.Character());
        TestDataType("CHARACTER(20)", new DataType.Character(new CharacterLength.IntegerLength(20)));
        TestDataType("CHARACTER(20 CHARACTERS)", new DataType.Character(new CharacterLength.IntegerLength(20, CharLengthUnit.Characters)));
        TestDataType("CHARACTER(20 OCTETS)", new DataType.Character(new CharacterLength.IntegerLength(20, CharLengthUnit.Octets)));
        TestDataType("CHAR", new DataType.Char());
        TestDataType("CHAR(20)", new DataType.Char(new CharacterLength.IntegerLength(20)));
        TestDataType("CHAR(20 CHARACTERS)", new DataType.Char(new CharacterLength.IntegerLength(20, CharLengthUnit.Characters)));
        TestDataType("CHAR(20 OCTETS)", new DataType.Char(new CharacterLength.IntegerLength(20, CharLengthUnit.Octets)));

        TestDataType("CHARACTER VARYING(20)", new DataType.CharacterVarying(new CharacterLength.IntegerLength(20)));
        TestDataType("CHARACTER VARYING(20 CHARACTERS)", new DataType.CharacterVarying(new CharacterLength.IntegerLength(20, CharLengthUnit.Characters)));
        TestDataType("CHARACTER VARYING(20 OCTETS)", new DataType.CharacterVarying(new CharacterLength.IntegerLength(20, CharLengthUnit.Octets)));
        TestDataType("CHAR VARYING(20)", new DataType.CharVarying(new CharacterLength.IntegerLength(20)));
        TestDataType("CHAR VARYING(20 CHARACTERS)", new DataType.CharVarying(new CharacterLength.IntegerLength(20, CharLengthUnit.Characters)));
        TestDataType("CHAR VARYING(20 OCTETS)", new DataType.CharVarying(new CharacterLength.IntegerLength(20, CharLengthUnit.Octets)));
    }

    [Fact]
    public void Test_Ansi_Character_Large_Object_Types()
    {
        DefaultDialects = [new GenericDialect(), new AnsiDialect()];

        TestDataType("CHARACTER LARGE OBJECT", new DataType.CharacterLargeObject());
        TestDataType("CHARACTER LARGE OBJECT(20)", new DataType.CharacterLargeObject(20));
        TestDataType("CHAR LARGE OBJECT", new DataType.CharLargeObject());
        TestDataType("CHAR LARGE OBJECT(20)", new DataType.CharLargeObject(20));
        TestDataType("CLOB", new DataType.Clob());
        TestDataType("CLOB(20)", new DataType.Clob(20));
    }

    [Fact]
    public void Test_Parse_Custom_Types()
    {
        DefaultDialects = [new GenericDialect(), new AnsiDialect()];

        TestDataType("GEOMETRY", new DataType.Custom("GEOMETRY"));
        TestDataType("GEOMETRY(POINT)", new DataType.Custom("GEOMETRY", new Sequence<string> { "POINT" }));
        TestDataType("GEOMETRY(POINT, 4326)", new DataType.Custom("GEOMETRY", new Sequence<string> { "POINT", "4326" }));
    }

    [Fact]
    public void Test_Ansi_Exact_Numeric_Types()
    {
        DefaultDialects = [new GenericDialect(), new AnsiDialect()];

        TestDataType("NUMERIC", new DataType.Numeric(new ExactNumberInfo.None()));
        TestDataType("NUMERIC(2)", new DataType.Numeric(new ExactNumberInfo.Precision(2)));
        TestDataType("NUMERIC(2,10)", new DataType.Numeric(new ExactNumberInfo.PrecisionAndScale(2, 10)));
        TestDataType("DECIMAL", new DataType.Decimal(new ExactNumberInfo.None()));
        TestDataType("DECIMAL(2)", new DataType.Decimal(new ExactNumberInfo.Precision(2)));
        TestDataType("DECIMAL(2,10)", new DataType.Decimal(new ExactNumberInfo.PrecisionAndScale(2, 10)));
        TestDataType("DEC", new DataType.Dec(new ExactNumberInfo.None()));
        TestDataType("DEC(2)", new DataType.Dec(new ExactNumberInfo.Precision(2)));
        TestDataType("DEC(2,10)", new DataType.Dec(new ExactNumberInfo.PrecisionAndScale(2, 10)));
    }

    [Fact]
    public void Test_Ansi_Date_Types()
    {
        DefaultDialects = [new GenericDialect(), new AnsiDialect()];

        TestDataType("DATE", new DataType.Date());
        TestDataType("TIME", new DataType.Time(TimezoneInfo.None));
        TestDataType("TIME(6)", new DataType.Time(TimezoneInfo.None, 6));
        TestDataType("TIME WITH TIME ZONE", new DataType.Time(TimezoneInfo.WithTimeZone));
        TestDataType("TIME(6) WITH TIME ZONE", new DataType.Time(TimezoneInfo.WithTimeZone, 6));
        TestDataType("TIME WITHOUT TIME ZONE", new DataType.Time(TimezoneInfo.WithoutTimeZone));
        TestDataType("TIME(6) WITHOUT TIME ZONE", new DataType.Time(TimezoneInfo.WithoutTimeZone, 6));
        TestDataType("TIMESTAMP", new DataType.Timestamp(TimezoneInfo.None));
        TestDataType("TIMESTAMP(22)", new DataType.Timestamp(TimezoneInfo.None, 22));
        TestDataType("TIMESTAMP(22) WITH TIME ZONE", new DataType.Timestamp(TimezoneInfo.WithTimeZone, 22));
        TestDataType("TIMESTAMP(22) WITHOUT TIME ZONE", new DataType.Timestamp(TimezoneInfo.WithoutTimeZone, 22));
    }

    [Fact]
    public void Test_Parse_Schema_Name()
    {
        TestSchemaName("dummy_name", new SchemaName.Simple("dummy_name"));
        TestSchemaName("AUTHORIZATION dummy_authorization", new SchemaName.UnnamedAuthorization("dummy_authorization"));
        TestSchemaName("dummy_name AUTHORIZATION dummy_authorization", new SchemaName.NamedAuthorization("dummy_name", "dummy_authorization"));

        void TestSchemaName(string sql, SchemaName schemaName)
        {
            AllDialects.RunParserMethod(sql, parser =>
            {
                var parsedName = parser.ParseSchemaName();
                Assert.Equal(schemaName, parsedName);
                Assert.Equal(schemaName.ToSql(), parsedName.ToSql());
            });
        }
    }

    [Fact]
    public void MySql_Parse_Index_Table_Constraint()
    {
        DefaultDialects = [new GenericDialect(), new MySqlDialect()];

        TestTableConstraint("INDEX (c1)", new TableConstraint.Index(new Ident[] { "c1" }));
        TestTableConstraint("KEY (c1)", new TableConstraint.Index(new Ident[] { "c1" }) { DisplayAsKey = true });
        TestTableConstraint("INDEX 'index' (c1, c2)", new TableConstraint.Index(new Ident[] { "c1", "c2" })
        {
            Name = new Ident("index", Symbols.SingleQuote)
        });
        TestTableConstraint("INDEX USING BTREE (c1)", new TableConstraint.Index(new Ident[] { "c1" })
        {
            IndexType = IndexType.BTree
        });
        TestTableConstraint("INDEX USING HASH (c1)", new TableConstraint.Index(new Ident[] { "c1" })
        {
            IndexType = IndexType.Hash
        });
        TestTableConstraint("INDEX idx_name USING BTREE (c1)", new TableConstraint.Index(new Ident[] { "c1" })
        {
            IndexType = IndexType.BTree,
            Name = "idx_name"
        });
        TestTableConstraint("INDEX idx_name USING HASH (c1)", new TableConstraint.Index(new Ident[] { "c1" })
        {
            IndexType = IndexType.Hash,
            Name = "idx_name"
        });
        return;

        void TestTableConstraint(string sql, TableConstraint constraint)
        {
            DefaultDialects!.RunParserMethod(sql, parser =>
            {
                var parsedConstraint = parser.ParseOptionalTableConstraint(false, false);
                Assert.Equal(constraint, parsedConstraint);
                Assert.Equal(sql, parsedConstraint!.ToSql());
            });
        }
    }

    [Fact]
    public void Test_Update_Has_Keyword()
    {
        const string sql = """
                           UPDATE test SET name=$1,
                           value=$2,
                           where=$3,
                           create=$4,
                           is_default=$5,
                           classification=$6,
                           sort=$7
                           WHERE id=$8
                           """;

        var ast = new Parser().ParseSql(sql, new PostgreSqlDialect());
        Assert.Equal("UPDATE test SET name = $1, value = $2, where = $3, create = $4, is_default = $5, classification = $6, sort = $7 WHERE id = $8",
            ast.ToSql());
    }

    [Fact]
    public void Test_Tokenizer_Error_Loc()
    {
        var ex = Assert.Throws<TokenizeException>(() => new Parser().ParseSql("foo '", new GenericDialect()));
        Assert.Equal("Unterminated string literal. Expected ' after Line: 1, Col: 5", ex.Message);
    }

    [Fact]
    public void Test_Parser_Error_Loc()
    {
        var ex = Assert.Throws<ParserException>(() => new Parser().ParseSql("SELECT this is a syntax error", new GenericDialect()));
        Assert.Equal("Expected [NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS, found a, Line: 1, Col: 16", ex.Message);
    }

    [Fact]
    public void Test_Nested_Explain_Error()
    {
        var ex = Assert.Throws<ParserException>(() => new Parser().ParseSql("EXPLAIN EXPLAIN SELECT 1", new GenericDialect()));
        Assert.Equal("Explain must be root of the plan.", ex.Message);
    }

    [Fact]
    public void Parse_Alter_Table()
    {
        var sql = "ALTER TABLE tab ADD COLUMN foo TEXT;";
            
        var addColumn = (AlterTableOperation.AddColumn)AlterTableOp(OneStatementParsesTo(sql, "ALTER TABLE tab ADD COLUMN foo TEXT"));

        Assert.True(addColumn.ColumnKeyword);
        Assert.False(addColumn.IfNotExists);
        Assert.Equal("foo", addColumn.ColumnDef.Name);
        Assert.Equal("TEXT", addColumn.ColumnDef.DataType.ToSql());

        sql = "ALTER TABLE tab RENAME TO new_tab";
        var renameTable = (AlterTableOperation.RenameTable) AlterTableOp(VerifiedStatement(sql));
        Assert.Equal("new_tab", renameTable.Name);
            
        sql = "ALTER TABLE tab RENAME COLUMN foo TO new_foo";
        var renameColumn = (AlterTableOperation.RenameColumn)AlterTableOp(VerifiedStatement(sql));
        Assert.Equal("foo", renameColumn.OldColumnName);
        Assert.Equal("new_foo", renameColumn.NewColumnName);
    }

    [Fact]
    public void Parse_Alter_Table_Add_Column()
    {
        var sql = "ALTER TABLE tab ADD foo TEXT";
        var addColumn = (AlterTableOperation.AddColumn) AlterTableOp(VerifiedStatement(sql));
        Assert.False(addColumn.ColumnKeyword);

        sql = "ALTER TABLE tab ADD COLUMN foo TEXT";
        addColumn = (AlterTableOperation.AddColumn)AlterTableOp(VerifiedStatement(sql));
        Assert.True(addColumn.ColumnKeyword);
    }

    [Fact]
    public void Parse_Double_Equality_Operator()
    {
        OneStatementParsesTo("SELECT a==b FROM t", "SELECT a = b FROM t", new Dialect[]{new SQLiteDialect(), new GenericDialect()});
    }

    [Fact]
    public void Parse_Optional_Cast_Format()
    {
        var parser = new Parser();
        _ = parser.ParseSql("select a from tbl where b = 123.45");
    }

    [Fact]
    public void Parse_Double_Colon_Cast_At_Timezone()
    {
        var select = VerifiedOnlySelect("SELECT '2001-01-01T00:00:00.000Z'::TIMESTAMP AT TIME ZONE 'Europe/Brussels' FROM t");

        var expression = new Expression.LiteralValue(new Value.SingleQuotedString("2001-01-01T00:00:00.000Z"));
        var cast = new Expression.Cast(expression, new DataType.Timestamp(TimezoneInfo.None), CastKind.DoubleColon);
        var expected = new Expression.AtTimeZone(cast,
            new Expression.LiteralValue(new Value.SingleQuotedString("Europe/Brussels")));
            
        Assert.Equal(expected, select.Projection[0].AsExpr());
    }

    [Fact]
    public void Parse_Alter_Table_Add_Unique_Using_Index_Constraint()
    {
        const string expectedConstraintName = "constraint_name";
        const string expectedIndexName = "index_name";
        const string sql = $"ALTER TABLE tab ADD CONSTRAINT {expectedConstraintName} UNIQUE USING INDEX {expectedIndexName}";
        var postgresDialect = new PostgreSqlDialect();
        var parser = new Parser();
        _ = parser.ParseSql(sql, postgresDialect);
        var addConstraint = (AlterTableOperation.AddConstraint)AlterTableOp(VerifiedStatement(sql, new Dialect[] { postgresDialect }));
        var addedConstraint = (TableConstraint.PostgresAlterTableIndex) addConstraint.TableConstraint;
        Assert.Equal(expectedConstraintName, addedConstraint.Name);
        Assert.Equal(expectedIndexName, addedConstraint.IndexName);
        Assert.False(addedConstraint.IsPrimaryKey);
    }

    [Fact]
    public void Parse_Alter_Table_Add_Primary_Key_Using_Index_Constraint()
    {
        const string expectedConstraintName = "constraint_name";
        const string expectedIndexName = "index_name";
        const string sql = $"ALTER TABLE tab ADD CONSTRAINT {expectedConstraintName} PRIMARY KEY USING INDEX {expectedIndexName}";
        var postgresDialect = new PostgreSqlDialect();
        var parser = new Parser();
        _ = parser.ParseSql(sql, postgresDialect);
        var addConstraint = (AlterTableOperation.AddConstraint)AlterTableOp(VerifiedStatement(sql, new Dialect[] { postgresDialect }));
        var addedConstraint = (TableConstraint.PostgresAlterTableIndex) addConstraint.TableConstraint;
        Assert.Equal(expectedConstraintName, addedConstraint.Name);
        Assert.Equal(expectedIndexName, addedConstraint.IndexName);
        Assert.True(addedConstraint.IsPrimaryKey);
    }

    [Fact]
    public void Parse_Alter_Table_Add_Constraint_Using_With_Characteristics()
    {
        const string sql = "ALTER TABLE tab ADD CONSTRAINT constraint_name UNIQUE USING INDEX index_name DEFERRABLE";
        var postgresDialect = new PostgreSqlDialect();
        var parser = new Parser();
        _ = parser.ParseSql(sql, postgresDialect);
        var addConstraint = (AlterTableOperation.AddConstraint)AlterTableOp(VerifiedStatement(sql, new Dialect[] { postgresDialect }));
        var addedConstraint = (TableConstraint.PostgresAlterTableIndex) addConstraint.TableConstraint;
        Assert.NotNull(addedConstraint.Characteristics);
        Assert.True(addedConstraint.Characteristics.Deferrable);
    }
}