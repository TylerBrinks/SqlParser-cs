using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.WindowFrameBound;

namespace SqlParser.Tests;

public class ToSqlTests : ParserTestBase
{
    [Fact]
    public void Kill_ToSql()
    {
        DefaultDialects = new[] { new ClickHouseDialect() };
        Assert.Equal("KILL MUTATION 5", VerifiedStatement("kill mutation 5").ToSql());
    }

    [Fact]
    public void Explain_ToSql()
    {
        Assert.Equal(
            "DESCRIBE SELECT sqrt(id) FROM foo", VerifiedStatement(
                "describe select sqrt(id) FROM foo").ToSql());
        Assert.Equal(
            "EXPLAIN SELECT sqrt(id) FROM foo", VerifiedStatement(
                "explain select sqrt(id) FROM foo").ToSql());
    }

    [Fact]
    public void Explain_Table_ToSql()
    {
        Assert.Equal("EXPLAIN test_identifier", VerifiedStatement("explain test_identifier").ToSql());
        Assert.Equal("DESCRIBE test_identifier", VerifiedStatement("describe test_identifier").ToSql());
    }

    [Fact]
    public void Insert_ToSq()
    {
        Assert.Equal("INSERT customer VALUES (1, 2, 3)", VerifiedStatement("insert customer values (1, 2, 3)").ToSql());
        Assert.Equal("INSERT INTO customer VALUES (1, 2, 3)", VerifiedStatement("insert into customer values (1, 2, 3)").ToSql());
    }

    [Fact]
    public void Update_ToSql()
    {
        Assert.Equal(
            "UPDATE t SET a = 1, b = 2, c = 3 WHERE d", VerifiedStatement(
                "update t set a = 1, b = 2, c = 3 where d").ToSql());
        Assert.Equal(
            "UPDATE users AS u SET u.username = 'new_user' WHERE u.username = 'old_user'", VerifiedStatement(
                "update users AS u set u.username = 'new_user' where u.username = 'old_user'").ToSql());
    }

    [Fact]
    public void Actions_ToSql()
    {
        Assert.Equal(
            "GRANT SELECT, INSERT, UPDATE (shape, size), USAGE, DELETE, TRUNCATE, REFERENCES, TRIGGER, CONNECT, CREATE, EXECUTE, TEMPORARY ON abc, def TO xyz, m WITH GRANT OPTION GRANTED BY lmno", VerifiedStatement(
                "grant select, insert, update (shape, size), usage, delete, truncate, references, trigger, connect, create, execute, temporary on abc, def to xyz, m with grant option granted by lmno").ToSql());
    }

    [Fact]
    public void Alter_ToSql()
    {
        Assert.Equal(
            "ALTER TABLE tab ALTER COLUMN is_active SET NOT NULL", VerifiedStatement(
                "alter table tab alter column is_active set not null").ToSql());
    }

    [Fact]
    public void ArrayAgg_ToSql()
    {
        Assert.Equal(
            "SELECT ARRAY[]", VerifiedStatement(
                "select array[]", new Dialect[] { new PostgreSqlDialect() }).ToSql());
    }

    [Fact]
    public void Modulo_Token()
    {
        VerifiedOnlySelect("SELECT 10 % 5");
    }

    [Fact]
    public void LessThanOrEqual_Token()
    {
        VerifiedOnlySelect("SELECT 10 <= 15");
    }

    [Fact]
    public void DoubleQuotedString_Wraps_Values()
    {
        var val = new DoubleQuotedString("test");
        Assert.Equal("\"test\"", val.ToString());
        Assert.Equal("test", val.Value);
    }

    [Fact]
    public void Following_Null_Message()
    {
        var following = new Following(null);

        Assert.Equal("UNBOUNDED FOLLOWING", following.ToSql());
    }

    [Fact]
    public void DollarQuotedString_ToString()
    {
        var str = new Value.DollarQuotedString(new DollarQuotedStringValue("test"));
        Assert.Equal("$$test$$", str.ToSql());

        str = new Value.DollarQuotedString(new DollarQuotedStringValue("test", "tag"));
        Assert.Equal("$tag$test$tag$", str.ToSql());
    }

    [Fact]
    public void Word_Throws_Exception()
    {
        Assert.Throws<ArgumentException>(() => Word.GetEndQuote('^'));
    }

    [Fact]
    public void Whitespace_Outputs_Strings()
    {
        var whitespace = new Whitespace(WhitespaceKind.InlineComment, "value"){Prefix = "prefix"};
        Assert.Equal("prefixvalue", whitespace.ToString());

        whitespace = new Whitespace(WhitespaceKind.NewLine, "value") { Prefix = "prefix" };
        Assert.Equal(Symbols.NewLine.ToString(), whitespace.ToString());

        whitespace = new Whitespace(WhitespaceKind.Space, "value") { Prefix = "prefix" };
        Assert.Equal(Symbols.Space.ToString(), whitespace.ToString());

        whitespace = new Whitespace(WhitespaceKind.Tab, "value") { Prefix = "prefix" };
        Assert.Equal(Symbols.Tab.ToString(), whitespace.ToString());

        whitespace = new Whitespace(WhitespaceKind.MultilineComment, "value") { Prefix = "prefix" };
        Assert.Equal("/*value*/", whitespace.ToString());

        whitespace = new Whitespace((WhitespaceKind)999, "value") { Prefix = "prefix" };
        Assert.Throws<ArgumentOutOfRangeException>(() => whitespace.ToString());
    }

    [Fact]
    public void Sequence_Overrides_ToString()
    {
        var sequence = new Sequence<string>
        {
            "one", "two", "three"
        };

        Assert.Equal("[one, two, three]", sequence.ToString());
    }

    [Fact]
    public void Delimited_Writes_Null_Lists()
    {
        List<Statement>? list = null;
        Assert.Equal(string.Empty, list.ToSqlDelimited());
    }

    [Fact]
    public void EnumWriter_Converts_Values_To_Strings()
    {
        Assert.Equal("DROP PARTITIONS", EnumWriter.Write(AddDropSync.Drop));
        Assert.Equal("SYNC PARTITIONS", EnumWriter.Write(AddDropSync.Sync));

        Assert.Null(EnumWriter.Write(FileFormat.None));

        Assert.Equal("WITH TIME ZONE", EnumWriter.Write(TimezoneInfo.WithTimeZone));
    }

    [Fact]
    public void Password_ToSql()
    {
        var valid = new Password.ValidPassword(new Expression.LiteralValue(new Value.SingleQuotedString("test")));
        Assert.Equal(" PASSWORD 'test'", valid.ToSql());

        var invalid = new Password.NullPassword();
        Assert.Equal(" PASSWORD NULL", invalid.ToSql());
    }
}