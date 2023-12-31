using SqlParser.Ast;
using SqlParser.Dialects;

namespace SqlParser.Tests;

public class ParserTestBase 
{
    protected IEnumerable<Dialect>? DefaultDialects { get; set; }

    public static readonly IEnumerable<Dialect> AllDialects = new Dialect[]
    {
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
        new DuckDbDialect()
    };

    private IEnumerable<Dialect> Dialects => DefaultDialects ?? AllDialects;

    public Query VerifiedQuery(string sql, bool preserveFormatting = false)
    {
        return VerifiedQuery(sql, AllDialects, preserveFormatting);
    }

    public Query VerifiedQuery(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false)
    {
        return VerifiedStatement(sql, dialects, preserveFormatting).AsQuery()!;
    }

    public Statement VerifiedStatement(string sql, bool preserveFormatting = false)
    {
        return VerifiedStatement(sql, Dialects, preserveFormatting);
    }

    public Statement VerifiedStatement(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false)
    {
        return OneStatementParsesTo(sql, sql, dialects, preserveFormatting);
    }

    public T VerifiedStatement<T>(string sql, bool preserveFormatting = false) where T : class
    {
        return VerifiedStatement<T>(sql, Dialects, preserveFormatting);
    }

    public T VerifiedStatement<T>(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false) where T : class
    {
        return (VerifiedStatement(sql, dialects) as T)!;
    }

    public Select VerifiedOnlySelect(string sql, bool preserveFormatting = false)
    {
        return VerifiedOnlySelect(sql, Dialects, preserveFormatting);
    }

    public Select VerifiedOnlySelect(string sql, IEnumerable<Dialect> dialects, bool preserveFormatting = false)
    {
        var expr = (SetExpression.SelectExpression)VerifiedQuery(sql, dialects, preserveFormatting).Body;
        return expr.Select;
    }

    public Expression VerifiedExpr(string sql)
    {
        return VerifiedExpr(sql, Dialects);
    }

    public Expression VerifiedExpr(string sql, IEnumerable<Dialect> dialects)
    {
        return OneOfIdenticalResults(dialect => new Parser().TryWithSql(sql, dialect).ParseExpr(), dialects);
    }

    public T OneStatementParsesTo<T>(string sql, string canonical, bool preserveFormatting = false) where T : Statement
    {
        return OneStatementParsesTo<T>(sql, canonical, Dialects, preserveFormatting);
    }

    public T OneStatementParsesTo<T>(string sql, string canonical, IEnumerable<Dialect> dialects, bool preserveFormatting = false) where T : Statement
    {
        return (T)OneStatementParsesTo(sql, canonical, dialects, preserveFormatting);
    }

    public Statement OneStatementParsesTo(string sql, string canonical, bool preserveFormatting = false)
    {
        return OneStatementParsesTo(sql, canonical, Dialects, preserveFormatting);
    }

    public Statement OneStatementParsesTo(string sql, string canonical, IEnumerable<Dialect> dialects, bool preserveFormatting = false)
    {
        var enumerable = dialects.ToList();
        var statements = ParseSqlStatements(sql, enumerable);
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

    public Sequence<Statement?> ParseSqlStatements(string sql)
    {
        return ParseSqlStatements(sql, Dialects);
    }

    // Ensures that `sql` parses as a single statement and returns it.
    // If non-empty `canonical` SQL representation is provided,
    // additionally asserts that parsing `sql` results in the same parse
    // tree as parsing `canonical`, and that serializing it back to string
    // results in the `canonical` representation.
    public Sequence<Statement?> ParseSqlStatements(string sql, IEnumerable<Dialect> dialects)
    {
        return OneOfIdenticalResults(dialect => new Parser().ParseSql(sql, dialect), dialects)!;
    }

    public T OneOfIdenticalResults<T>(Func<Dialect, T> action, IEnumerable<Dialect> dialects) where T : class
    {
        if (!dialects.Any())
        {
            Assert.Fail("No test dialect provided.");
        }

        var map = dialects.Select(action);

        var agg = map.Aggregate(default(T), (result, next) =>
        {
            if (result == default(T))
            {
                return next;
            }

            return next;
        });

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

    public Value.Number Number(string value)
    {
        return new Value.Number(value);
    }
}