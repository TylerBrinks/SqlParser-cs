using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.Expression;
using static SqlParser.Ast.Statement;

// ReSharper disable StringLiteralTypo

namespace SqlParser.Tests;

/// <summary>
/// Tests for new features from Rust v0.53-v0.60
/// </summary>
public class NewFeaturesTests : ParserTestBase
{
    #region Statement Tests

    [Fact]
    public void Parse_Print_Statement()
    {
        var sql = "PRINT 'Hello World'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Print>(statement);

        var print = (Print)statement;
        Assert.IsType<LiteralValue>(print.PrintStatement.Message);
    }

    [Fact]
    public void Parse_Print_Statement_With_Variable()
    {
        DefaultDialects = [new GenericDialect(), new MsSqlDialect()];
        var sql = "PRINT @message";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Print>(statement);
    }

    [Fact]
    public void Parse_Vacuum_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Simple VACUUM
        var sql = "VACUUM";
        var statement = OneStatementParsesTo(sql, "VACUUM");
        Assert.IsType<Vacuum>(statement);

        // VACUUM with table
        sql = "VACUUM my_table";
        statement = VerifiedStatement(sql);
        Assert.IsType<Vacuum>(statement);
        var vacuum = (Vacuum)statement;
        Assert.NotNull(vacuum.VacuumStatement.Table);

        // VACUUM ANALYZE
        sql = "VACUUM ANALYZE";
        statement = OneStatementParsesTo(sql, "VACUUM ANALYZE");
        Assert.IsType<Vacuum>(statement);
    }

    [Fact]
    public void Parse_Raise_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Simple RAISE
        var sql = "RAISE";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Raise>(statement);

        // RAISE with level
        sql = "RAISE EXCEPTION 'Error message'";
        statement = VerifiedStatement(sql);
        Assert.IsType<Raise>(statement);
        var raise = (Raise)statement;
        Assert.Equal(RaiseStatementLevel.Exception, raise.RaiseStatement.Level);

        // RAISE NOTICE
        sql = "RAISE NOTICE 'Info message'";
        statement = VerifiedStatement(sql);
        raise = (Raise)statement;
        Assert.Equal(RaiseStatementLevel.Notice, raise.RaiseStatement.Level);
    }

    [Fact]
    public void Parse_RaiseError_Statement()
    {
        DefaultDialects = [new GenericDialect(), new MsSqlDialect()];

        var sql = "RAISERROR('Error message', 16, 1)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<RaiseError>(statement);

        // With arguments
        sql = "RAISERROR('Error %s', 16, 1, 'arg1')";
        statement = VerifiedStatement(sql);
        Assert.IsType<RaiseError>(statement);
        var raiseError = (RaiseError)statement;
        Assert.NotNull(raiseError.RaiseErrorStatement.Arguments);

        // With options
        sql = "RAISERROR('Error', 16, 1) WITH LOG";
        statement = VerifiedStatement(sql);
        raiseError = (RaiseError)statement;
        Assert.NotNull(raiseError.RaiseErrorStatement.Options);
        Assert.Contains(RaiseErrorOption.Log, raiseError.RaiseErrorStatement.Options);
    }

    [Fact]
    public void Parse_Reset_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // RESET ALL
        var sql = "RESET ALL";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Reset>(statement);
        var reset = (Reset)statement;
        Assert.IsType<ResetStatement.All>(reset.ResetStatement);

        // RESET specific setting
        sql = "RESET search_path";
        statement = VerifiedStatement(sql);
        reset = (Reset)statement;
        Assert.IsType<ResetStatement.Name>(reset.ResetStatement);
    }

    [Fact]
    public void Parse_Deny_Statement()
    {
        DefaultDialects = [new GenericDialect(), new MsSqlDialect()];

        var sql = "DENY SELECT ON my_table TO my_user";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Deny>(statement);

        var deny = (Deny)statement;
        Assert.Equal("my_user", deny.DenyStatement.Grantee.Value);
    }

    [Fact]
    public void Parse_If_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Simple IF
        var sql = "IF 1 = 1 THEN SELECT 1 END IF";
        var statement = VerifiedStatement(sql);
        Assert.IsType<If>(statement);
        var ifStmt = (If)statement;
        Assert.NotNull(ifStmt.IfStatement.ThenBlock);

        // IF with ELSE
        sql = "IF 1 = 0 THEN SELECT 1 ELSE SELECT 2 END IF";
        statement = VerifiedStatement(sql);
        ifStmt = (If)statement;
        Assert.NotNull(ifStmt.IfStatement.ElseBlock);

        // IF with ELSEIF
        sql = "IF 1 = 0 THEN SELECT 1 ELSEIF 1 = 1 THEN SELECT 2 ELSE SELECT 3 END IF";
        statement = VerifiedStatement(sql);
        ifStmt = (If)statement;
        Assert.NotNull(ifStmt.IfStatement.ElseIfs);
        Assert.Single(ifStmt.IfStatement.ElseIfs);
    }

    [Fact]
    public void Parse_While_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "WHILE counter > 0 DO SELECT 1 END WHILE";
        var statement = VerifiedStatement(sql);
        Assert.IsType<While>(statement);
        var whileStmt = (While)statement;
        Assert.NotNull(whileStmt.WhileStatement.Body);
    }

    [Fact]
    public void Parse_Case_Statement()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Simple CASE
        var sql = "CASE WHEN 1 = 1 THEN SELECT 1 END CASE";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Case>(statement);

        // CASE with operand
        sql = "CASE x WHEN 1 THEN SELECT 'one' WHEN 2 THEN SELECT 'two' ELSE SELECT 'other' END CASE";
        statement = VerifiedStatement(sql);
        var caseStmt = (Case)statement;
        Assert.NotNull(caseStmt.CaseStatement.Operand);
        Assert.Equal(2, caseStmt.CaseStatement.Branches.Count);
        Assert.NotNull(caseStmt.CaseStatement.ElseBlock);
    }

    [Fact]
    public void Parse_Create_Domain()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE DOMAIN email AS VARCHAR(255)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateDomain>(statement);
        var domain = (CreateDomain)statement;
        Assert.Equal("email", domain.Name.ToString());

        // With DEFAULT
        sql = "CREATE DOMAIN positive_int AS INT DEFAULT 0";
        statement = VerifiedStatement(sql);
        domain = (CreateDomain)statement;
        Assert.NotNull(domain.Default);

        // With COLLATE
        sql = "CREATE DOMAIN text_domain AS TEXT COLLATE \"en_US\"";
        statement = VerifiedStatement(sql);
        domain = (CreateDomain)statement;
        Assert.NotNull(domain.CollationName);
    }

    [Fact]
    public void Parse_Drop_Domain()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP DOMAIN email";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropDomain>(statement);

        // With IF EXISTS
        sql = "DROP DOMAIN IF EXISTS email";
        statement = VerifiedStatement(sql);
        var drop = (DropDomain)statement;
        Assert.True(drop.IfExists);

        // With CASCADE
        sql = "DROP DOMAIN email CASCADE";
        statement = VerifiedStatement(sql);
        drop = (DropDomain)statement;
        Assert.Equal(DropBehavior.Cascade, drop.DropBehavior);
    }

    [Fact]
    public void Parse_Drop_User()
    {
        DefaultDialects = [new GenericDialect()];

        var sql = "DROP USER test_user";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropUser>(statement);

        // With IF EXISTS
        sql = "DROP USER IF EXISTS test_user";
        statement = VerifiedStatement(sql);
        var drop = (DropUser)statement;
        Assert.True(drop.IfExists);

        // Multiple users
        sql = "DROP USER user1, user2, user3";
        statement = VerifiedStatement(sql);
        drop = (DropUser)statement;
        Assert.Equal(3, drop.Names.Count);
    }

    [Fact]
    public void Parse_Create_Connector()
    {
        DefaultDialects = [new GenericDialect(), new BigQueryDialect()];

        var sql = "CREATE CONNECTOR my_connector TYPE jdbc";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateConnector>(statement);
        var conn = (CreateConnector)statement;
        Assert.Equal("my_connector", conn.ConnectorStatement.Name.Value);

        // With IF NOT EXISTS
        sql = "CREATE CONNECTOR IF NOT EXISTS my_connector TYPE jdbc";
        statement = VerifiedStatement(sql);
        conn = (CreateConnector)statement;
        Assert.True(conn.ConnectorStatement.IfNotExists);
    }

    [Fact]
    public void Parse_Alter_Connector()
    {
        DefaultDialects = [new GenericDialect(), new BigQueryDialect()];

        var sql = "ALTER CONNECTOR my_connector SET OPTIONS (host = 'localhost')";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterConnector>(statement);
        var alter = (AlterConnector)statement;
        Assert.IsType<AlterConnectorOperation.SetOptions>(alter.ConnectorStatement.Operation);
    }

    [Fact]
    public void Parse_Drop_Connector()
    {
        DefaultDialects = [new GenericDialect(), new BigQueryDialect()];

        var sql = "DROP CONNECTOR my_connector";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropConnector>(statement);

        // With IF EXISTS
        sql = "DROP CONNECTOR IF EXISTS my_connector";
        statement = VerifiedStatement(sql);
        var drop = (DropConnector)statement;
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void Parse_Create_Server()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateServer>(statement);
        var server = (CreateServer)statement;
        Assert.Equal("myserver", server.ServerStatement.Name.ToString());
        Assert.Equal("postgres_fdw", server.ServerStatement.ForeignDataWrapper.ToString());

        // With OPTIONS
        sql = "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'testdb')";
        statement = VerifiedStatement(sql);
        server = (CreateServer)statement;
        Assert.NotNull(server.ServerStatement.Options);
    }

    [Fact]
    public void Parse_Alter_Schema()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // RENAME TO
        var sql = "ALTER SCHEMA old_schema RENAME TO new_schema";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterSchema>(statement);
        var alter = (AlterSchema)statement;
        Assert.IsType<AlterSchemaOperation.RenameTo>(alter.Operation);

        // OWNER TO
        sql = "ALTER SCHEMA my_schema OWNER TO new_owner";
        statement = VerifiedStatement(sql);
        alter = (AlterSchema)statement;
        Assert.IsType<AlterSchemaOperation.OwnerTo>(alter.Operation);
    }

    [Fact]
    public void Parse_Execute_Immediate()
    {
        DefaultDialects = [new GenericDialect()];

        var sql = "EXECUTE IMMEDIATE 'SELECT 1'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<ExecuteImmediate>(statement);

        // With INTO
        sql = "EXECUTE IMMEDIATE 'SELECT name FROM users WHERE id = 1' INTO result";
        statement = VerifiedStatement(sql);
        var exec = (ExecuteImmediate)statement;
        Assert.NotNull(exec.ExecuteStatement.Into);

        // With USING
        sql = "EXECUTE IMMEDIATE 'SELECT * FROM users WHERE id = ?' USING 1";
        statement = VerifiedStatement(sql);
        exec = (ExecuteImmediate)statement;
        Assert.NotNull(exec.ExecuteStatement.Using);
    }

    #endregion

    #region Expression Tests

    [Fact]
    public void Parse_Is_Normalized_Expression()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Simple IS NORMALIZED
        var select = VerifiedOnlySelect("SELECT 'test' IS NORMALIZED");
        var expr = select.Projection[0].AsExpr();
        Assert.IsType<IsNormalized>(expr);

        // IS NOT NORMALIZED
        select = VerifiedOnlySelect("SELECT 'test' IS NOT NORMALIZED");
        expr = select.Projection[0].AsExpr();
        var isNorm = (IsNormalized)expr;
        Assert.True(isNorm.Negated);

        // With normalization form
        select = VerifiedOnlySelect("SELECT 'test' IS NFC NORMALIZED");
        expr = select.Projection[0].AsExpr();
        isNorm = (IsNormalized)expr;
        Assert.Equal(NormalForm.NFC, isNorm.Form);

        // All forms
        VerifiedOnlySelect("SELECT 'test' IS NFD NORMALIZED");
        VerifiedOnlySelect("SELECT 'test' IS NFKC NORMALIZED");
        VerifiedOnlySelect("SELECT 'test' IS NFKD NORMALIZED");
    }

    [Fact]
    public void Parse_Member_Of_Expression()
    {
        DefaultDialects = [new GenericDialect(), new MySqlDialect()];

        var select = VerifiedOnlySelect("SELECT 1 MEMBER OF(json_array)");
        var expr = select.Projection[0].AsExpr();
        Assert.IsType<MemberOf>(expr);

        var memberOf = (MemberOf)expr;
        Assert.NotNull(memberOf.Member);
        Assert.NotNull(memberOf.Array);
    }

    [Fact]
    public void Parse_Connect_By_Root_Expression()
    {
        DefaultDialects = [new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT CONNECT_BY_ROOT name FROM employees");
        var expr = select.Projection[0].AsExpr();
        Assert.IsType<ConnectByRoot>(expr);
    }

    [Fact]
    public void Parse_NotNull_Expression()
    {
        DefaultDialects = [new GenericDialect(), new SQLiteDialect()];

        var select = VerifiedOnlySelect("SELECT x NOTNULL FROM t");
        var expr = select.Projection[0].AsExpr();
        Assert.IsType<NotNull>(expr);
    }

    #endregion

    #region Table Factor Tests

    [Fact]
    public void Parse_TableSample_Percent()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var select = VerifiedOnlySelect("SELECT * FROM my_table TABLESAMPLE (10 PERCENT)");
        var from = select.From!.First();
        Assert.IsType<TableFactor.TableSample>(from.Relation);

        var sample = (TableFactor.TableSample)from.Relation!;
        Assert.IsType<TableSampleKind.Percent>(sample.TableSampleKind);
    }

    [Fact]
    public void Parse_TableSample_Rows()
    {
        DefaultDialects = [new GenericDialect()];

        var select = VerifiedOnlySelect("SELECT * FROM my_table TABLESAMPLE (100 ROWS)");
        var from = select.From!.First();
        var sample = (TableFactor.TableSample)from.Relation!;
        Assert.IsType<TableSampleKind.Rows>(sample.TableSampleKind);
    }

    [Fact]
    public void Parse_TableSample_With_Method()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // BERNOULLI
        var select = VerifiedOnlySelect("SELECT * FROM my_table TABLESAMPLE BERNOULLI (10 PERCENT)");
        var from = select.From!.First();
        var sample = (TableFactor.TableSample)from.Relation!;
        var percent = (TableSampleKind.Percent)sample.TableSampleKind;
        Assert.Equal(TableSampleMethod.Bernoulli, percent.Method);

        // SYSTEM
        select = VerifiedOnlySelect("SELECT * FROM my_table TABLESAMPLE SYSTEM (10 PERCENT)");
        from = select.From!.First();
        sample = (TableFactor.TableSample)from.Relation!;
        percent = (TableSampleKind.Percent)sample.TableSampleKind;
        Assert.Equal(TableSampleMethod.System, percent.Method);
    }

    [Fact]
    public void Parse_XmlTable()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SELECT * FROM XMLTABLE('/root/item' PASSING xml_data COLUMNS id INT PATH '@id', name VARCHAR(100) PATH 'name')";
        var select = VerifiedOnlySelect(sql);
        var from = select.From!.First();
        Assert.IsType<TableFactor.XmlTable>(from.Relation);
    }

    #endregion

    #region Join Tests

    [Fact]
    public void Parse_Straight_Join()
    {
        DefaultDialects = [new GenericDialect(), new MySqlDialect()];

        var sql = "SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.id";
        var select = VerifiedOnlySelect(sql);
        var join = select.From!.First().Joins!.First();
        Assert.IsType<JoinOperator.StraightJoin>(join.JoinOperator);
    }

    #endregion

    #region Additional Expression Tests

    [Fact]
    public void Parse_Overlaps_Expression()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SELECT (start1, end1) OVERLAPS (start2, end2)";
        var select = VerifiedOnlySelect(sql);
        var expr = select.Projection[0].AsExpr();
        Assert.IsType<Overlaps>(expr);

        var overlaps = (Overlaps)expr;
        Assert.Equal(2, overlaps.Left.Count);
        Assert.Equal(2, overlaps.Right.Count);
    }

    #endregion

    #region Additional Table Factor Tests

    [Fact]
    public void Parse_SemanticView()
    {
        DefaultDialects = [new GenericDialect()];

        var sql = "SELECT * FROM SEMANTIC_VIEW(my_view)";
        var select = VerifiedOnlySelect(sql);
        var from = select.From!.First();
        Assert.IsType<TableFactor.SemanticView>(from.Relation);

        var semanticView = (TableFactor.SemanticView)from.Relation!;
        Assert.Equal("my_view", semanticView.Name.ToString());
    }

    [Fact]
    public void Parse_SemanticView_With_Args()
    {
        DefaultDialects = [new GenericDialect()];

        var sql = "SELECT * FROM SEMANTIC_VIEW(my_view, arg1, arg2)";
        var select = VerifiedOnlySelect(sql);
        var from = select.From!.First();
        var semanticView = (TableFactor.SemanticView)from.Relation!;
        Assert.Equal(2, semanticView.Args.Count);
    }

    #endregion

    #region v0.60.0 Feature Tests

    [Fact]
    public void Parse_Invisible_Column_Option()
    {
        DefaultDialects = [new GenericDialect(), new MySqlDialect()];

        // Create table with INVISIBLE column
        var sql = "CREATE TABLE t (id INT, secret VARCHAR(100) INVISIBLE)";
        var statement = VerifiedStatement(sql);
        var create = (CreateTable)statement;
        var columns = create.Element.Columns;

        Assert.Equal(2, columns.Count);
        var secretCol = columns[1];
        Assert.Contains(secretCol.Options, opt => opt.Option is ColumnOption.Invisible);
    }

    [Fact]
    public void Parse_Foreign_Key_Match_Type()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // MATCH FULL
        var sql = "CREATE TABLE t (id INT REFERENCES parent(id) MATCH FULL)";
        var statement = VerifiedStatement(sql);
        var create = (CreateTable)statement;
        var col = create.Element.Columns[0];
        var fk = col.Options.First(o => o.Option is ColumnOption.ForeignKey).Option as ColumnOption.ForeignKey;
        Assert.NotNull(fk);
        Assert.Equal(MatchType.Full, fk!.Match);

        // MATCH PARTIAL
        sql = "CREATE TABLE t (id INT REFERENCES parent(id) MATCH PARTIAL)";
        statement = VerifiedStatement(sql);
        create = (CreateTable)statement;
        col = create.Element.Columns[0];
        fk = col.Options.First(o => o.Option is ColumnOption.ForeignKey).Option as ColumnOption.ForeignKey;
        Assert.Equal(MatchType.Partial, fk!.Match);

        // MATCH SIMPLE
        sql = "CREATE TABLE t (id INT REFERENCES parent(id) MATCH SIMPLE)";
        statement = VerifiedStatement(sql);
        create = (CreateTable)statement;
        col = create.Element.Columns[0];
        fk = col.Options.First(o => o.Option is ColumnOption.ForeignKey).Option as ColumnOption.ForeignKey;
        Assert.Equal(MatchType.Simple, fk!.Match);
    }

    [Fact]
    public void Parse_Index_Types()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // GIN index
        var sql = "CREATE INDEX idx ON t USING GIN(col)";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.GIN, createIndex.Element.IndexType);

        // GiST index
        sql = "CREATE INDEX idx ON t USING GIST(col)";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.GiST, createIndex.Element.IndexType);

        // SP-GiST index
        sql = "CREATE INDEX idx ON t USING SPGIST(col)";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.SPGiST, createIndex.Element.IndexType);

        // BRIN index
        sql = "CREATE INDEX idx ON t USING BRIN(col)";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.BRIN, createIndex.Element.IndexType);

        // Bloom index
        sql = "CREATE INDEX idx ON t USING BLOOM(col)";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.Bloom, createIndex.Element.IndexType);

        // Custom index type
        sql = "CREATE INDEX idx ON t USING custom_type(col)";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.Equal(IndexType.Custom, createIndex.Element.IndexType);
        Assert.NotNull(createIndex.Element.CustomIndexTypeName);
    }

    [Fact]
    public void Parse_Drop_Operator_Statements()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // DROP OPERATOR (standalone)
        var sql = "DROP OPERATOR @(NONE, INT)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropOperator>(statement);
        var dropOp = (DropOperator)statement;
        Assert.Single(dropOp.Operators);
        Assert.Null(dropOp.Operators[0].LeftType);
        Assert.IsType<DataType.Int>(dropOp.Operators[0].RightType);

        // DROP OPERATOR with both types
        sql = "DROP OPERATOR +(INT, INT)";
        statement = VerifiedStatement(sql);
        dropOp = (DropOperator)statement;
        Assert.NotNull(dropOp.Operators[0].LeftType);
        Assert.IsType<DataType.Int>(dropOp.Operators[0].LeftType);
        Assert.IsType<DataType.Int>(dropOp.Operators[0].RightType);

        // DROP OPERATOR IF EXISTS CASCADE
        sql = "DROP OPERATOR IF EXISTS @(NONE, INT) CASCADE";
        statement = VerifiedStatement(sql);
        dropOp = (DropOperator)statement;
        Assert.True(dropOp.IfExists);
        Assert.Equal(DropBehavior.Cascade, dropOp.DropBehavior);

        // DROP OPERATOR CLASS
        sql = "DROP OPERATOR CLASS my_opclass USING btree";
        statement = VerifiedStatement(sql);
        Assert.IsType<DropOperatorClass>(statement);
        var dropOpClass = (DropOperatorClass)statement;
        Assert.Equal("my_opclass", dropOpClass.Name.ToString());
        Assert.Equal("btree", dropOpClass.IndexMethod.Value);

        // DROP OPERATOR CLASS IF EXISTS CASCADE
        sql = "DROP OPERATOR CLASS IF EXISTS my_opclass USING btree CASCADE";
        statement = VerifiedStatement(sql);
        dropOpClass = (DropOperatorClass)statement;
        Assert.True(dropOpClass.IfExists);
        Assert.Equal(DropBehavior.Cascade, dropOpClass.DropBehavior);

        // DROP OPERATOR FAMILY
        sql = "DROP OPERATOR FAMILY my_opfamily USING hash";
        statement = VerifiedStatement(sql);
        Assert.IsType<DropOperatorFamily>(statement);
        var dropOpFamily = (DropOperatorFamily)statement;
        Assert.Equal("my_opfamily", dropOpFamily.Name.ToString());
        Assert.Equal("hash", dropOpFamily.IndexMethod.Value);

        // DROP OPERATOR FAMILY IF EXISTS RESTRICT
        sql = "DROP OPERATOR FAMILY IF EXISTS my_opfamily USING hash RESTRICT";
        statement = VerifiedStatement(sql);
        dropOpFamily = (DropOperatorFamily)statement;
        Assert.True(dropOpFamily.IfExists);
        Assert.Equal(DropBehavior.Restrict, dropOpFamily.DropBehavior);
    }

    [Fact]
    public void Parse_Alter_User()
    {
        DefaultDialects = [new GenericDialect(), new SnowflakeDialect()];

        // ALTER USER SET
        var sql = "ALTER USER test_user SET PASSWORD = 'new_pass'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterUser>(statement);
        var alterUser = (AlterUser)statement;
        Assert.Equal("test_user", alterUser.Name.ToString());
        Assert.IsType<AlterUserOperation.Set>(alterUser.Operation);

        // ALTER USER RENAME
        sql = "ALTER USER old_user RENAME TO new_user";
        statement = VerifiedStatement(sql);
        alterUser = (AlterUser)statement;
        Assert.IsType<AlterUserOperation.RenameTo>(alterUser.Operation);
    }

    [Fact]
    public void Test_Dynamic_Table_Ast()
    {
        // Test that dynamic table AST types work correctly
        var targetLag = new DynamicTableLag.IntervalLag("1 minute");
        var writer = new SqlTextWriter();
        targetLag.ToSql(writer);
        Assert.Equal("TARGET_LAG = '1 minute'", writer.ToString());

        var downstream = new DynamicTableLag.Downstream();
        writer = new SqlTextWriter();
        downstream.ToSql(writer);
        Assert.Equal("TARGET_LAG = DOWNSTREAM", writer.ToString());
    }

    [Fact]
    public void Test_Oracle_Dialect()
    {
        // Test that OracleDialect can be instantiated and used
        var dialect = new OracleDialect();

        Assert.True(dialect.SupportsConnectBy);
        Assert.True(dialect.IsIdentifierStart('a'));
        Assert.True(dialect.IsIdentifierPart('1'));
        Assert.True(dialect.IsIdentifierPart('_'));
        Assert.True(dialect.IsIdentifierPart('$'));
        Assert.False(dialect.IsIdentifierStart('1'));

        // Parse a simple query
        var parser = new Parser();
        var sql = "SELECT * FROM dual";
        var statements = parser.ParseSql(sql, dialect);
        Assert.Single(statements);
    }

    [Fact]
    public void Test_Index_Column_With_Operator_Class()
    {
        // This tests the IndexColumn struct can be used
        var indexCol = new IndexColumn(new Identifier("my_col"))
        {
            OperatorClass = new Ident("text_pattern_ops"),
            Asc = true,
            NullsFirst = false
        };

        var writer = new SqlTextWriter();
        indexCol.ToSql(writer);
        var result = writer.ToString();

        Assert.Contains("my_col", result);
        Assert.Contains("text_pattern_ops", result);
        Assert.Contains("ASC", result);
        Assert.Contains("NULLS LAST", result);
    }

    [Fact]
    public void Test_Drop_Operator_Info_Ast()
    {
        // Test DropOperatorInfo AST serialization
        var info = new DropOperatorInfo(
            new ObjectName("my_operator"),
            new DataType.Int(),
            new DataType.Text()
        );

        var writer = new SqlTextWriter();
        info.ToSql(writer);
        var result = writer.ToString();

        Assert.Contains("my_operator", result);
        Assert.Contains("INT", result);
        Assert.Contains("TEXT", result);
    }

    [Fact]
    public void Test_Procedure_Param_With_Default()
    {
        // Test that ProcedureParam with default value works
        var param = new ProcedureParam(new Ident("param1"), new DataType.Int())
        {
            Default = new Expression.LiteralValue(new Value.Number("42"))
        };

        var writer = new SqlTextWriter();
        param.ToSql(writer);
        var result = writer.ToString();

        Assert.Contains("param1", result);
        Assert.Contains("INT", result);
        Assert.Contains("DEFAULT", result);
        Assert.Contains("42", result);
    }

    [Fact]
    public void Test_Bitwise_Not_Operator()
    {
        // Test cross-dialect BitwiseNot operator AST
        var expr = new Expression.UnaryOp(
            new Identifier("a"),
            UnaryOperator.BitwiseNot
        );

        var writer = new SqlTextWriter();
        expr.ToSql(writer);
        var result = writer.ToString();

        Assert.Equal("~a", result);
    }

    [Fact]
    public void Test_Alter_User_Reset()
    {
        DefaultDialects = [new GenericDialect(), new SnowflakeDialect()];

        // ALTER USER RESET
        var sql = "ALTER USER test_user RESET PASSWORD";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterUser>(statement);
        var alterUser = (AlterUser)statement;
        Assert.IsType<AlterUserOperation.Reset>(alterUser.Operation);
    }

    [Fact]
    public void Test_Match_Type_ToSql()
    {
        // Test MATCH type serialization in foreign key
        var fk = new ColumnOption.ForeignKey(
            new ObjectName("parent"),
            new Sequence<Ident> { new Ident("id") },
            ReferentialAction.Cascade,
            ReferentialAction.None)
        {
            Match = MatchType.Full
        };

        var writer = new SqlTextWriter();
        fk.ToSql(writer);
        var result = writer.ToString();

        Assert.Contains("REFERENCES parent", result);
        Assert.Contains("MATCH Full", result);
        Assert.Contains("ON DELETE CASCADE", result);
    }

    [Fact]
    public void Test_Table_Constraint_Foreign_Key_Match()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        // Table constraint with MATCH FULL
        var sql = "CREATE TABLE t (id INT, FOREIGN KEY (id) REFERENCES parent(id) MATCH FULL ON DELETE CASCADE)";
        var statement = VerifiedStatement(sql);
        var create = (CreateTable)statement;
        var constraint = create.Element.Constraints!.First() as TableConstraint.ForeignKey;
        Assert.NotNull(constraint);
        Assert.Equal(MatchType.Full, constraint!.Match);
    }

    #endregion

    #region PostgreSQL Table Partitioning Tests

    [Fact]
    public void Parse_Create_Table_Partition_Of_Default()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_default PARTITION OF measurement DEFAULT";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partition_Of_List()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_y2020 PARTITION OF measurement FOR VALUES IN ('2020')";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partition_Of_Range()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_y2020 PARTITION OF measurement FOR VALUES FROM ('2020-01-01') TO ('2021-01-01')";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partition_Of_Hash()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_p0 PARTITION OF measurement FOR VALUES WITH (MODULUS 4, REMAINDER 0)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partition_Of_Range_Minvalue_Maxvalue()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_old PARTITION OF measurement FOR VALUES FROM (MINVALUE) TO ('2020-01-01')";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);

        sql = "CREATE TABLE measurement_future PARTITION OF measurement FOR VALUES FROM ('2030-01-01') TO (MAXVALUE)";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partition_Of_Multicolumn_Range()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement_y2020m01 PARTITION OF measurement FOR VALUES FROM ('2020-01-01', 1) TO ('2020-02-01', 1)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    [Fact]
    public void Parse_Create_Table_Partitioned_By()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TABLE measurement (id INT, created_at DATE) PARTITION BY RANGE (created_at)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);

        sql = "CREATE TABLE measurement (id INT, region TEXT) PARTITION BY LIST (region)";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);

        sql = "CREATE TABLE measurement (id INT) PARTITION BY HASH (id)";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateTable>(statement);
    }

    #endregion

    #region PostgreSQL Operator DDL Tests

    // Note: CREATE OPERATOR, CREATE OPERATOR CLASS, CREATE OPERATOR FAMILY,
    // ALTER OPERATOR, ALTER OPERATOR CLASS, ALTER OPERATOR FAMILY
    // are not yet implemented in the parser. Only DROP variants are supported.
    // Tests for DROP OPERATOR/CLASS/FAMILY are in Parse_Drop_Operator_Statements() above.

    #endregion

    #region PostgreSQL Advanced Trigger Tests

    [Fact]
    public void Parse_Create_Trigger_Instead_Of()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TRIGGER my_trigger INSTEAD OF DELETE ON my_view FOR EACH ROW EXECUTE FUNCTION my_func()";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTrigger>(statement);
        var trigger = (CreateTrigger)statement;
        Assert.Equal(TriggerPeriod.InsteadOf, trigger.Period);
    }

    [Fact]
    public void Parse_Create_Trigger_With_Condition()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TRIGGER my_trigger AFTER UPDATE ON my_table FOR EACH ROW WHEN (OLD.value <> NEW.value) EXECUTE FUNCTION my_func()";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTrigger>(statement);
    }

    [Fact]
    public void Parse_Create_Trigger_Multiple_Events()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TRIGGER my_trigger AFTER INSERT OR UPDATE OR DELETE ON my_table FOR EACH ROW EXECUTE FUNCTION my_func()";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTrigger>(statement);
    }

    [Fact]
    public void Parse_Create_Trigger_With_Referencing()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TRIGGER my_trigger AFTER UPDATE ON my_table REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE FUNCTION my_func()";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTrigger>(statement);
    }

    [Fact]
    public void Parse_Create_Trigger_Deferrable()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE CONSTRAINT TRIGGER my_trigger AFTER INSERT ON my_table DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION my_func()";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateTrigger>(statement);
    }

    [Fact]
    public void Parse_Drop_Trigger()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP TRIGGER my_trigger ON my_table";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropTrigger>(statement);

        sql = "DROP TRIGGER IF EXISTS my_trigger ON my_table CASCADE";
        statement = VerifiedStatement(sql);
        var dropTrigger = (DropTrigger)statement;
        Assert.True(dropTrigger.IfExists);
    }

    #endregion

    #region PostgreSQL Function Advanced Options Tests

    [Fact]
    public void Parse_Create_Function_With_Security()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL SECURITY DEFINER AS 'SELECT 1'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);

        sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL SECURITY INVOKER AS 'SELECT 1'";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);
    }

    [Fact]
    public void Parse_Create_Function_With_Set_Params()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL SET search_path TO my_schema AS 'SELECT 1'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);
    }

    [Fact]
    public void Parse_Create_Function_Parallel()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL PARALLEL SAFE AS 'SELECT 1'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);

        sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL PARALLEL UNSAFE AS 'SELECT 1'";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);

        sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE SQL PARALLEL RESTRICTED AS 'SELECT 1'";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);
    }

    [Fact]
    public void Parse_Create_Function_C_Language()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE FUNCTION my_func() RETURNS void LANGUAGE C AS 'my_module', 'my_symbol'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateFunction>(statement);
    }

    #endregion

    #region PostgreSQL Advanced Index Tests

    [Fact]
    public void Parse_Create_Index_Nulls_Distinct()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE INDEX idx ON t (col) NULLS DISTINCT";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.True(createIndex.Element.NullsDistinct);

        sql = "CREATE INDEX idx ON t (col) NULLS NOT DISTINCT";
        statement = VerifiedStatement(sql);
        createIndex = (CreateIndex)statement;
        Assert.False(createIndex.Element.NullsDistinct);
    }

    [Fact]
    public void Parse_Create_Index_With_Include()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE INDEX idx ON t (col1) INCLUDE (col2, col3)";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.NotNull(createIndex.Element.Include);
        Assert.Equal(2, createIndex.Element.Include!.Count);
    }

    [Fact]
    public void Parse_Create_Index_With_Where()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE INDEX idx ON t (col) WHERE col > 0";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.NotNull(createIndex.Element.Predicate);
    }

    [Fact]
    public void Parse_Create_Index_Concurrently()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE INDEX CONCURRENTLY idx ON t (col)";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.True(createIndex.Element.Concurrently);
    }

    [Fact]
    public void Parse_Create_Index_With_Options()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE INDEX idx ON t USING GIN(col) WITH (fastupdate = off)";
        var statement = VerifiedStatement(sql);
        var createIndex = (CreateIndex)statement;
        Assert.NotNull(createIndex.Element.With);
    }

    #endregion

    #region PostgreSQL TRUNCATE Tests

    [Fact]
    public void Parse_Truncate_Basic()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "TRUNCATE TABLE my_table";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Truncate>(statement);
    }

    [Fact]
    public void Parse_Truncate_Multiple_Tables()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "TRUNCATE TABLE table1, table2, table3";
        var statement = VerifiedStatement(sql);
        var truncate = (Truncate)statement;
        Assert.Equal(3, truncate.TableNames.Count);
    }

    [Fact]
    public void Parse_Truncate_With_Options()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "TRUNCATE TABLE my_table RESTART IDENTITY";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Truncate>(statement);

        sql = "TRUNCATE TABLE my_table CONTINUE IDENTITY";
        statement = VerifiedStatement(sql);
        Assert.IsType<Truncate>(statement);
    }

    [Fact]
    public void Parse_Truncate_Cascade()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "TRUNCATE TABLE my_table CASCADE";
        var statement = VerifiedStatement(sql);
        var truncate = (Truncate)statement;
        Assert.True(truncate.Cascade);

        sql = "TRUNCATE TABLE my_table RESTRICT";
        statement = VerifiedStatement(sql);
        truncate = (Truncate)statement;
        Assert.False(truncate.Cascade);
    }

    #endregion

    #region PostgreSQL ALTER TABLE Advanced Tests

    [Fact]
    public void Parse_Alter_Table_Replica_Identity()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER TABLE my_table REPLICA IDENTITY FULL";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);

        sql = "ALTER TABLE my_table REPLICA IDENTITY DEFAULT";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);

        sql = "ALTER TABLE my_table REPLICA IDENTITY NOTHING";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);

        sql = "ALTER TABLE my_table REPLICA IDENTITY USING INDEX my_index";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);
    }

    [Fact]
    public void Parse_Alter_Table_Validate_Constraint()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER TABLE my_table VALIDATE CONSTRAINT my_constraint";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);
    }

    [Fact]
    public void Parse_Alter_Table_Set_Schema()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER TABLE my_table SET SCHEMA new_schema";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);
    }

    [Fact]
    public void Parse_Alter_Table_Owner_To()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER TABLE my_table OWNER TO new_owner";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterTable>(statement);
    }

    #endregion

    #region Other Missing Tests

    [Fact]
    public void Parse_Drop_Extension()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP EXTENSION my_extension";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropExtension>(statement);

        sql = "DROP EXTENSION IF EXISTS my_extension CASCADE";
        statement = VerifiedStatement(sql);
        var dropExt = (DropExtension)statement;
        Assert.True(dropExt.IfExists);
        Assert.True(dropExt.Cascade);
    }

    [Fact]
    public void Parse_Alter_Type()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER TYPE my_type ADD VALUE 'new_value'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterType>(statement);

        sql = "ALTER TYPE my_type RENAME TO new_type";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterType>(statement);

        sql = "ALTER TYPE my_type OWNER TO new_owner";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterType>(statement);
    }

    [Fact]
    public void Parse_Copy_From()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "COPY my_table FROM '/path/to/file.csv' WITH (FORMAT CSV, HEADER true)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Copy>(statement);
    }

    [Fact]
    public void Parse_Copy_To()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "COPY my_table TO '/path/to/file.csv' WITH (FORMAT CSV, HEADER true)";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Copy>(statement);
    }

    [Fact]
    public void Parse_Listen_Notify()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "LISTEN my_channel";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Listen>(statement);

        sql = "NOTIFY my_channel";
        statement = VerifiedStatement(sql);
        Assert.IsType<Notify>(statement);

        sql = "NOTIFY my_channel, 'payload'";
        statement = VerifiedStatement(sql);
        Assert.IsType<Notify>(statement);
    }

    [Fact]
    public void Parse_Discard()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DISCARD ALL";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Discard>(statement);

        sql = "DISCARD PLANS";
        statement = VerifiedStatement(sql);
        Assert.IsType<Discard>(statement);

        sql = "DISCARD SEQUENCES";
        statement = VerifiedStatement(sql);
        Assert.IsType<Discard>(statement);

        sql = "DISCARD TEMP";
        statement = VerifiedStatement(sql);
        Assert.IsType<Discard>(statement);
    }

    [Fact]
    public void Parse_Deallocate()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DEALLOCATE my_statement";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Deallocate>(statement);

        sql = "DEALLOCATE ALL";
        statement = VerifiedStatement(sql);
        Assert.IsType<Deallocate>(statement);

        sql = "DEALLOCATE PREPARE my_statement";
        statement = VerifiedStatement(sql);
        Assert.IsType<Deallocate>(statement);
    }

    [Fact]
    public void Parse_Prepare_Execute()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "PREPARE my_stmt (int, text) AS SELECT * FROM t WHERE id = $1 AND name = $2";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Prepare>(statement);

        sql = "EXECUTE my_stmt(1, 'test')";
        statement = VerifiedStatement(sql);
        Assert.IsType<Execute>(statement);
    }

    [Fact]
    public void Parse_Comment_On()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "COMMENT ON TABLE my_table IS 'This is a table comment'";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Comment>(statement);

        sql = "COMMENT ON COLUMN my_table.my_column IS 'This is a column comment'";
        statement = VerifiedStatement(sql);
        Assert.IsType<Comment>(statement);
    }

    [Fact]
    public void Parse_Set_Transaction()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        var statement = VerifiedStatement(sql);
        Assert.IsType<SetTransaction>(statement);

        sql = "SET TRANSACTION READ ONLY";
        statement = VerifiedStatement(sql);
        Assert.IsType<SetTransaction>(statement);
    }

    [Fact]
    public void Parse_Create_Sequence()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE SEQUENCE my_seq";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateSequence>(statement);

        sql = "CREATE SEQUENCE my_seq START WITH 100 INCREMENT BY 10 MINVALUE 1 MAXVALUE 1000 CYCLE";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateSequence>(statement);
    }

    [Fact]
    public void Parse_Alter_Sequence()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "ALTER SEQUENCE my_seq RESTART WITH 100";
        var statement = VerifiedStatement(sql);
        Assert.IsType<AlterSequence>(statement);

        sql = "ALTER SEQUENCE my_seq INCREMENT BY 5";
        statement = VerifiedStatement(sql);
        Assert.IsType<AlterSequence>(statement);
    }

    [Fact]
    public void Parse_Drop_Sequence()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP SEQUENCE my_seq";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);

        sql = "DROP SEQUENCE IF EXISTS my_seq CASCADE";
        statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);
    }

    [Fact]
    public void Parse_Create_Type()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE TYPE my_enum AS ENUM ('value1', 'value2', 'value3')";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateType>(statement);
    }

    [Fact]
    public void Parse_Create_Schema()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE SCHEMA my_schema";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateSchema>(statement);

        sql = "CREATE SCHEMA IF NOT EXISTS my_schema AUTHORIZATION my_user";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateSchema>(statement);
    }

    [Fact]
    public void Parse_Drop_Schema()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP SCHEMA my_schema";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);

        sql = "DROP SCHEMA IF EXISTS my_schema CASCADE";
        statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);
    }

    [Fact]
    public void Parse_Create_Role()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE ROLE my_role";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreateRole>(statement);

        sql = "CREATE ROLE my_role WITH LOGIN PASSWORD 'secret' SUPERUSER";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreateRole>(statement);
    }

    [Fact]
    public void Parse_Drop_Role()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP ROLE my_role";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);

        sql = "DROP ROLE IF EXISTS my_role";
        statement = VerifiedStatement(sql);
        Assert.IsType<Drop>(statement);
    }

    [Fact]
    public void Parse_Grant_Revoke()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "GRANT SELECT, INSERT ON my_table TO my_role";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Grant>(statement);

        sql = "REVOKE SELECT ON my_table FROM my_role";
        statement = VerifiedStatement(sql);
        Assert.IsType<Revoke>(statement);

        sql = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO my_role";
        statement = VerifiedStatement(sql);
        Assert.IsType<Grant>(statement);
    }

    [Fact]
    public void Parse_Create_Policy()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "CREATE POLICY my_policy ON my_table FOR SELECT USING (user_id = current_user_id())";
        var statement = VerifiedStatement(sql);
        Assert.IsType<CreatePolicy>(statement);

        sql = "CREATE POLICY my_policy ON my_table FOR ALL TO my_role USING (true) WITH CHECK (true)";
        statement = VerifiedStatement(sql);
        Assert.IsType<CreatePolicy>(statement);
    }

    [Fact]
    public void Parse_Drop_Policy()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "DROP POLICY my_policy ON my_table";
        var statement = VerifiedStatement(sql);
        Assert.IsType<DropPolicy>(statement);

        sql = "DROP POLICY IF EXISTS my_policy ON my_table";
        statement = VerifiedStatement(sql);
        var dropPolicy = (DropPolicy)statement;
        Assert.True(dropPolicy.IfExists);
    }

    [Fact]
    public void Parse_Savepoint_Release_Rollback()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SAVEPOINT my_savepoint";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Savepoint>(statement);

        sql = "RELEASE SAVEPOINT my_savepoint";
        statement = VerifiedStatement(sql);
        Assert.IsType<ReleaseSavepoint>(statement);

        sql = "ROLLBACK TO SAVEPOINT my_savepoint";
        statement = VerifiedStatement(sql);
        Assert.IsType<Rollback>(statement);
    }

    [Fact]
    public void Parse_Lock_Table()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "LOCK TABLE my_table IN ACCESS EXCLUSIVE MODE";
        var statement = VerifiedStatement(sql);
        Assert.IsType<LockTables>(statement);
    }

    [Fact]
    public void Parse_Explain()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "EXPLAIN SELECT * FROM my_table";
        var statement = VerifiedStatement(sql);
        Assert.IsType<Explain>(statement);

        sql = "EXPLAIN ANALYZE SELECT * FROM my_table";
        statement = VerifiedStatement(sql);
        Assert.IsType<Explain>(statement);

        sql = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM my_table";
        statement = VerifiedStatement(sql);
        Assert.IsType<Explain>(statement);
    }

    [Fact]
    public void Parse_Show()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SHOW search_path";
        var statement = VerifiedStatement(sql);
        Assert.IsType<ShowVariable>(statement);

        sql = "SHOW ALL";
        statement = VerifiedStatement(sql);
        Assert.IsType<ShowVariable>(statement);
    }

    [Fact]
    public void Parse_Set_Variable()
    {
        DefaultDialects = [new GenericDialect(), new PostgreSqlDialect()];

        var sql = "SET search_path TO my_schema, public";
        var statement = VerifiedStatement(sql);
        Assert.IsType<SetVariable>(statement);

        sql = "SET LOCAL timezone = 'UTC'";
        statement = VerifiedStatement(sql);
        Assert.IsType<SetVariable>(statement);
    }

    #endregion
}
