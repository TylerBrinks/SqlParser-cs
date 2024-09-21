using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser.Tests;

public class ParserTestBase
{
    protected IEnumerable<Dialect>? DefaultDialects { get; set; }

    public static readonly IEnumerable<Dialect> AllDialects =
    [
        new GenericDialect(),
        new PostgreSqlDialect(),
        new MsSqlDialect(),
        new AnsiDialect(),
        new SnowflakeDialect(),
        new HiveDialect(),
        new RedshiftDialect(),
        new MySqlDialect(),
        new BigQueryDialect(),
        new SQLiteDialect(),
        new DuckDbDialect(),
        new DatabricksDialect()
    ];

    private IEnumerable<Dialect> Dialects => DefaultDialects ?? AllDialects;

    public Query VerifiedQuery(string sql, bool preserveFormatting = false)
    {
        return VerifiedQuery(sql, AllDialects, preserveFormatting);
    }

    public Query VerifiedQuery(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        return VerifiedStatement(sql, dialects, preserveFormatting, unescape, options).AsQuery()!;
    }

    public Statement VerifiedStatement(string sql, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        return VerifiedStatement(sql, Dialects, preserveFormatting, unescape, options);
    }

    public Statement VerifiedStatement(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        return OneStatementParsesTo(sql, sql, dialects, preserveFormatting, unescape, options);
    }

    public T VerifiedStatement<T>(string sql, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null) where T : class
    {
        return VerifiedStatement<T>(sql, Dialects, preserveFormatting, unescape, options);
    }

    public T VerifiedStatement<T>(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null) where T : class
    {
        return (VerifiedStatement(sql, dialects, unescape, unescape, options) as T)!;
    }

    public Select VerifiedOnlySelect(string sql, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        return VerifiedOnlySelect(sql, Dialects, preserveFormatting, unescape, options);
    }

    public Select VerifiedOnlySelect(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        var expr = (SetExpression.SelectExpression)VerifiedQuery(sql, dialects, preserveFormatting, unescape, options).Body;
        return expr.Select;
    }

    public Select VerifiedOnlySelectWithCanonical(string sql, string canonical)
    {
        var query = OneStatementParsesTo(sql, canonical).AsQuery();
        return query!.Body.AsSelect();
    }

    public Select VerifiedOnlySelectWithCanonical(string sql, string canonical, IEnumerable<Dialect> dialects)
    {
        var query = OneStatementParsesTo(sql, canonical, dialects).AsQuery();
        return query!.Body.AsSelect();
    }

    public Expression VerifiedExpr(string sql)
    {
        return VerifiedExpr(sql, Dialects);
    }

    public Expression VerifiedExpr(string sql, IEnumerable<Dialect> dialects)
    {
        return OneOfIdenticalResults(dialect => new Parser().TryWithSql(sql, dialect).ParseExpr(), dialects);
    }

    public Query VerifiedQueryWithCanonical(string sql, string canonical)
    {
        var select = OneStatementParsesTo(sql, canonical);
        return select.AsQuery()!;
    }

    public Query VerifiedQueryWithCanonical(string sql, string canonical, IEnumerable<Dialect> dialects)
    {
        var select = OneStatementParsesTo(sql, canonical, dialects);
        return select.AsQuery()!;
    }

    public T OneStatementParsesTo<T>(string sql, string canonical, bool preserveFormatting = false) where T : Statement
    {
        return OneStatementParsesTo<T>(sql, canonical, Dialects, preserveFormatting);
    }

    public T OneStatementParsesTo<T>(string sql, string canonical, IEnumerable<Dialect> dialects, bool preserveFormatting = false) where T : Statement
    {
        return (T)OneStatementParsesTo(sql, canonical, dialects, preserveFormatting);
    }

    public Statement OneStatementParsesTo(string sql, string canonical, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        return OneStatementParsesTo(sql, canonical, Dialects, preserveFormatting, unescape, options);
    }

    public Statement OneStatementParsesTo(string sql, string canonical, IEnumerable<Dialect> dialects, bool preserveFormatting = false, bool unescape = false, ParserOptions? options = null)
    {
        var enumerable = dialects.ToList();
        var statements = ParseSqlStatements(sql, enumerable, unescape, options);
        Assert.Single(statements);

        if (!string.IsNullOrEmpty(canonical) && sql != canonical)
        {
            var expected = ParseSqlStatements(canonical, enumerable);
            Assert.Equal(expected, statements);
        }

        var onlyStatement = statements.First();
        if (!string.IsNullOrEmpty(canonical))
        {
            var expected = canonical;

            if (!preserveFormatting)
            {
                expected = expected.Replace("\r", "").Replace("\n", "");
            }

            var actual = statements.First()!.ToSql();

            Assert.Equal(expected, actual, StringComparer.InvariantCultureIgnoreCase);
        }

        return onlyStatement!;
    }

    public Sequence<Statement?> ParseSqlStatements(string sql, bool unescape = false, ParserOptions? options = null)
    {
        return ParseSqlStatements(sql, Dialects, unescape, options);
    }

    public Expression ExpressionParsesTo(string sql, string canonical, IEnumerable<Dialect> dialects)
    {
        var expr = VerifiedExpr(sql, dialects);

        if (!string.IsNullOrEmpty(canonical))
        {
            var actual = expr.ToSql();

            Assert.Equal(canonical, actual, StringComparer.InvariantCultureIgnoreCase);
        }

        return expr;
    }

    // Ensures that `sql` parses as a single statement and returns it.
    // If non-empty `canonical` SQL representation is provided,
    // additionally asserts that parsing `sql` results in the same parse
    // tree as parsing `canonical`, and that serializing it back to string
    // results in the `canonical` representation.
    public Sequence<Statement?> ParseSqlStatements(string sql, IEnumerable<Dialect> dialects, bool unescape = false, ParserOptions? options = null)
    {
        options ??= new ParserOptions { Unescape = unescape };
        return OneOfIdenticalResults(dialect =>
        {
            options.TrailingCommas |= dialect.SupportsTrailingCommas;
            return new Parser().ParseSql(sql, dialect, options);
        }, dialects)!;
    }

    public T OneOfIdenticalResults<T>(Func<Dialect, T> action, IEnumerable<Dialect> dialects) where T : class
    {
        if (!dialects.Any())
        {
            Assert.Fail("No test dialect provided.");
        }

        var map = dialects.Select(action);

        var agg = map.Aggregate(default(T), (_, next) => next);

        return agg!;
    }


    public void TestDataType(string sql, DataType dataType)
    {
        Dialects.RunParserMethod(sql, parser =>
        {
            var parsedType = parser.ParseDataType();
            Assert.Equal(dataType, parsedType);
        });
    }

    public static Value.Number Number(string value)
    {
        return new Value.Number(value);
    }

    public static AlterTableOperation AlterTableOpWithName(Statement statement, string expectedName)
    {
        var alter = (Statement.AlterTable)statement;

        Assert.Equal(expectedName, alter.Name);
        Assert.False(alter.IfExists);
        Assert.False(alter.Only);
        return alter.Operations.First();
    }

    public static AlterTableOperation AlterTableOp(Statement statement)
    {
        return AlterTableOpWithName(statement, "tab");
    }
}