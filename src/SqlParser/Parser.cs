using System.Text.RegularExpressions;
using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.ExactNumberInfo;
using static SqlParser.Ast.Statement;
using static SqlParser.Ast.MinMaxValue;
using static SqlParser.Ast.GrantObjects;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;
using Select = SqlParser.Ast.Select;
using System.Globalization;
using static SqlParser.Ast.AlterTableOperation;

namespace SqlParser;

// This record type fills in the outcome from the Rust project's macro that
// intercepts control flow depending on parsing result.  The same flow is
// used in the parser, and the outcome of the lambda matches this record.  
public record MaybeParsed<T>(bool Parsed, T Result);

public class Parser
{
    // https://www.postgresql.org/docs/7.0/operators.htm#AEN2026ExpectRightParen
    public const short OrPrecedence = 5;
    public const short AndPrecedence = 10;
    public const short UnaryNotPrecedence = 15;
    public const short IsPrecedence = 17;
    public const short LikePrecedence = 19;
    public const short BetweenPrecedence = 20;
    public const short PipePrecedence = 21;
    public const short CaretPrecedence = 22;
    public const short AmpersandPrecedence = 23;
    public const short XOrPrecedence = 24;
    public const short MulDivModOpPrecedence = 40;
    public const short PlusMinusPrecedence = 30;
    //public const short MultiplyPrecedence = 40;
    public const short ArrowPrecedence = 50;

    private int _index;
    private Sequence<Token> _tokens = null!;
    private DepthGuard _depthGuard = null!;
    private Dialect _dialect = null!;
    private ParserOptions _options = null!;

    /// <summary>
    /// Parses a given SQL string into an Abstract Syntax Tree with a generic SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to parse</param>
    /// <param name="options">Parsing options</param>
    /// <returns></returns>
    /// <exception cref="TokenizeException">Thrown when an unexpected token in encountered while parsing the input string</exception>
    /// <exception cref="ParserException">Thrown when the sequence of tokens does not match the dialect's expected grammar</exception>
    public Sequence<Statement> ParseSql(ReadOnlySpan<char> sql, ParserOptions? options = null)
    {
        return ParseSql(sql, new GenericDialect(), options);
    }
    /// <summary>
    /// Parses a given SQL string into an Abstract Syntax Tree using a given SQL dialect
    /// </summary>
    /// <param name="sql">SQL string to parse</param>
    /// <param name="dialect">SQL dialect instance</param>
    /// <param name="options">Parsing options</param>
    /// <returns></returns>
    public Sequence<Statement> ParseSql(ReadOnlySpan<char> sql, Dialect dialect, ParserOptions? options = null)
    {
        _options = options ?? new ParserOptions();
        _depthGuard = new DepthGuard(_options.RecursionLimit);
        _dialect = dialect;

        var tokenizer = new Tokenizer(_options.Unescape);
        _tokens = tokenizer.Tokenize(sql, dialect).ToSequence();
        _index = 0;

        var statements = ParseStatements();

        return statements;
    }
    /// <summary>
    /// Builds a parser with a SQL fragment that is tokenized but not yet parsed.  This
    /// allows the parser to be used for with subsets of SQL calling any of the parser's
    /// underlying parsing methods.
    /// </summary>
    /// <param name="sql">SQL fragment to tokenize</param>
    /// <param name="dialect">SQL dialect instance</param>
    /// <param name="options">Parsing options</param>
    /// <returns></returns>
    public Parser TryWithSql(ReadOnlySpan<char> sql, Dialect dialect, ParserOptions? options = null)
    {
        _options = options ?? new ParserOptions();
        _depthGuard = new DepthGuard(50);
        _dialect = dialect;
        var tokenizer = new Tokenizer(options?.Unescape ?? true);
        _tokens = tokenizer.Tokenize(sql, dialect).ToSequence();
        _index = 0;
        return this;
    }
    /// <summary>
    /// Initializes a field if the input condition is met; default initialization if not.
    /// </summary>
    /// <typeparam name="T">Type to initialize</typeparam>
    /// <param name="condition">Condition to satisfy initialization</param>
    /// <param name="initialization">Function to initialize the output value</param>
    /// <returns>Initialized value if condition is true; otherwise default value.</returns>
    public static T? ParseInit<T>(bool condition, Func<T> initialization)
    {
        return condition ? initialization() : default;
    }
    /// <summary>
    /// Parse potentially multiple statements
    ///
    /// Example
    ///  Parse a SQL string with 2 separate statements
    /// 
    ///     parser.ParseSql("SELECT * FROM foo; SELECT * FROM bar;")
    /// </summary>
    /// <returns>List of statements parsed into an Abstract Syntax Tree</returns>
    /// <exception cref="ParserException">ParserException thrown when the expected token or keyword is not encountered.</exception>
    public Sequence<Statement> ParseStatements()
    {
        var expectingStatementDelimiter = false;
        var statements = new Sequence<Statement>();

        while (true)
        {
            while (ConsumeToken<SemiColon>())
            {
                expectingStatementDelimiter = false;
            }

            var next = PeekToken();
            if (next is EOF) //PeekTokenIs<EOF>())
            {
                break;
            }

            if (next is Word { Keyword: Keyword.END })
            {
                break;
            }

            if (expectingStatementDelimiter)
            {
                throw Expected("end of statement", PeekToken());
            }

            var statement = ParseStatement();
            statements.Add(statement);
            expectingStatementDelimiter = true;
        }

        return statements;
    }
    /// <summary>
    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    /// </summary>
    /// <returns>Parsed statement</returns>
    public Statement ParseStatement()
    {
        using var guard = _depthGuard.Decrement();
        var statement = _dialect.ParseStatement(this);
        if (statement != null)
        {
            return statement;
        }

        var token = NextToken();

        return token switch
        {
            Word word => ParseKeywordStatement(word),
            LeftParen => ParseQuery(true),
            _ => throw Expected("a SQL statement", token)
        };
    }
    /// <summary>
    /// Match the Word token and parse the statement recursively
    /// </summary>
    /// <param name="word">Word token to parse</param>
    /// <returns>SQL Statement object</returns>
    public Statement ParseKeywordStatement(Word word)
    {
        return word.Keyword switch
        {
            Keyword.KILL => ParseKill(),
            Keyword.DESCRIBE => ParseExplain(true),
            Keyword.EXPLAIN => ParseExplain(false),
            Keyword.ANALYZE => ParseAnalyze(),
            Keyword.SELECT or Keyword.WITH or Keyword.VALUES => ParseQuery(true),
            Keyword.TRUNCATE => ParseTruncate(),
            Keyword.ATTACH => ParseAttachDatabase(),
            Keyword.MSCK => ParseMsck(),
            Keyword.CREATE => ParseCreate(),
            Keyword.CACHE => ParseCacheTable(),
            Keyword.DROP => ParseDrop(),
            Keyword.DISCARD => ParseDiscard(),
            Keyword.DECLARE => ParseDeclare(),
            Keyword.FETCH => ParseFetchStatement(),
            Keyword.DELETE => ParseDelete(),
            Keyword.INSERT => ParseInsert(),
            Keyword.UNCACHE => ParseUncacheTable(),
            Keyword.UPDATE => ParseUpdate(),
            Keyword.ALTER => ParseAlter(),
            Keyword.COPY => ParseCopy(),
            Keyword.CLOSE => ParseClose(),
            Keyword.SET => ParseSet(),
            Keyword.SHOW => ParseShow(),
            Keyword.USE => new Use(ParseIdentifier()),
            Keyword.GRANT => ParseGrant(),
            Keyword.REVOKE => ParseRevoke(),
            Keyword.START => ParseStartTransaction(),
            // `BEGIN` is a nonstandard but common alias for the
            // standard `START TRANSACTION` statement. It is supported
            // by at least PostgreSQL and MySQL.
            Keyword.BEGIN => ParseBegin(),
            Keyword.SAVEPOINT => new Savepoint(ParseIdentifier()),
            Keyword.COMMIT => new Commit(ParseCommitRollbackChain()),
            Keyword.ROLLBACK => new Rollback(ParseCommitRollbackChain()),
            Keyword.ASSERT => ParseAssert(),
            // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
            // syntax. They are used for Postgres prepared statement.
            Keyword.DEALLOCATE => ParseDeallocate(),
            Keyword.EXECUTE => ParseExecute(),
            Keyword.PREPARE => ParsePrepare(),
            Keyword.MERGE => ParseMerge(),
            Keyword.PRAGMA => ParsePragma(),

            _ => throw Expected("a SQL statement", PeekToken())
        };
    }

    public T ExpectParens<T>(Func<T> action)
    {
        ExpectLeftParen();
        var result = action();
        ExpectRightParen();
        return result;
    }

    public void ExpectLeftParen()
    {
        ExpectToken<LeftParen>();
    }

    public void ExpectRightParen()
    {
        ExpectToken<RightParen>();
    }

    public bool ParseIfNotExists()
    {
        return ParseKeywordSequence(Keyword.IF, Keyword.NOT, Keyword.EXISTS);
    }

    public bool ParseIfExists()
    {
        return ParseKeywordSequence(Keyword.IF, Keyword.EXISTS);
    }

    // ReSharper disable once IdentifierTypo
    public Msck ParseMsck()
    {
        var repair = ParseKeyword(Keyword.REPAIR);
        ExpectKeyword(Keyword.TABLE);
        var tableName = ParseObjectName();
        var partitionAction = MaybeParse(() =>
        {
            var keyword = ParseOneOfKeywords(Keyword.ADD, Keyword.DROP, Keyword.SYNC);

            var op = keyword switch
            {
                Keyword.ADD => AddDropSync.Add,
                Keyword.DROP => AddDropSync.Drop,
                Keyword.SYNC => AddDropSync.Sync,
                _ => AddDropSync.None,
            };

            ExpectKeyword(Keyword.PARTITIONS);
            return op;
        });

        return new Msck(tableName, repair, partitionAction);
    }

    public Truncate ParseTruncate()
    {
        var table = ParseKeyword(Keyword.TABLE);
        var tableName = ParseObjectName();
        var partitions = ParseInit(ParseKeyword(Keyword.PARTITION), () =>
        {
            return ExpectParens(() => ParseCommaSeparated(ParseExpr));
        });

        return new Truncate(tableName, partitions, table);
    }

    public Statement ParseAttachDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var databaseFileName = ParseExpr();
        ExpectKeyword(Keyword.AS);
        var schemaName = ParseIdentifier();

        return new AttachDatabase(schemaName, databaseFileName, database);
    }

    public Statement ParseAnalyze()
    {
        ExpectKeyword(Keyword.TABLE);
        var tableName = ParseObjectName();
        var forColumns = false;
        var cacheMetadata = false;
        var noScan = false;
        Sequence<Expression>? partitions = null;
        var computeStatistics = false;
        Sequence<Ident>? columns = null;

        var loop = true;
        while (loop)
        {
            var keyword = ParseOneOfKeywords(
                Keyword.PARTITION,
                Keyword.FOR,
                Keyword.CACHE,
                Keyword.NOSCAN,
                Keyword.COMPUTE);

            switch (keyword)
            {
                case Keyword.PARTITION:
                    partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
                    break;
                case Keyword.NOSCAN:
                    noScan = true;
                    break;
                case Keyword.FOR:
                    ExpectKeyword(Keyword.COLUMNS);
                    columns = MaybeParse(() => ParseCommaSeparated(ParseIdentifier));
                    forColumns = true;
                    break;
                case Keyword.CACHE:
                    ExpectKeyword(Keyword.METADATA);
                    cacheMetadata = true;
                    break;
                case Keyword.COMPUTE:
                    ExpectKeyword(Keyword.STATISTICS);
                    computeStatistics = true;
                    break;
                default:
                    loop = false;
                    break;
            }
        }

        return new Analyze(tableName)
        {
            ForColumns = forColumns,
            Columns = columns,
            Partitions = partitions,
            CacheMetadata = cacheMetadata,
            NoScan = noScan,
            ComputeStatistics = computeStatistics
        };
    }

    /// <summary>
    /// Parse a new expression including wildcard & qualified wildcard
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException">Wildcard Expression</exception>
    public WildcardExpression ParseWildcardExpr()
    {
        var index = _index;

        var nextToken = NextToken();

        switch (nextToken)
        {
            case Word or SingleQuotedString when PeekTokenIs<Period>():
                {
                    var ident = nextToken switch
                    {
                        Word w => w.ToIdent(),
                        SingleQuotedString s => new Ident(s.Value, Symbols.SingleQuote),
                        _ => throw Expected("identifier or quoted string", PeekToken())
                    };
                    var idParts = new Sequence<Ident> { ident };

                    while (ConsumeToken<Period>())
                    {
                        nextToken = NextToken();
                        switch (nextToken)
                        {
                            case Word w:
                                idParts.Add(w.ToIdent());
                                break;

                            case SingleQuotedString s:
                                idParts.Add(new Ident(s.Value, QuoteStyle: Symbols.SingleQuote));
                                break;

                            case Multiply:
                                return new WildcardExpression.QualifiedWildcard(new ObjectName(idParts));

                            default:
                                throw Expected("an identifier or a '*' after '.'");
                        }
                    }

                    break;
                }
            case Multiply:
                return new WildcardExpression.Wildcard();
        }

        _index = index;
        var expr = ParseExpr();
        return new WildcardExpression.Expr(expr);
    }

    /// <summary>
    /// Parse a new expression
    /// </summary>
    /// <returns>Expression</returns>
    public Expression ParseExpr()
    {
        using var guard = _depthGuard.Decrement();
        return ParseSubExpression(0);
    }

    /// <summary>
    /// Parse tokens until the precedence changes
    /// </summary>
    /// <param name="precedence">Precedence value</param>
    /// <returns>Parsed sub-expression</returns>
    public Expression ParseSubExpression(short precedence)
    {
        var expr = ParsePrefix();
        while (true)
        {
            var nextPrecedence = GetNextPrecedence();

            if (precedence >= nextPrecedence)
            {
                break;
            }
            expr = ParseInfix(expr, nextPrecedence);
        }
        return expr;
    }

    public Expression ParseIntervalExpr()
    {
        short precedence = 0;
        var expr = ParsePrefix();

        while (true)
        {
            var nextPrecedence = GetNextIntervalPrecedence();

            if (precedence >= nextPrecedence)
            {
                break;
            }

            expr = ParseInfix(expr, nextPrecedence);
        }

        return expr;
    }

    /// <summary>
    /// Get the precedence of the next token
    /// </summary>
    /// <returns>Precedence value</returns>
    public short GetNextIntervalPrecedence()
    {
        var token = PeekToken();

        return token switch
        {
            Word { Keyword: Keyword.AND or Keyword.OR or Keyword.XOR } => 0,
            _ => GetNextPrecedence()
        };
    }

    public Statement ParseAssert()
    {
        var condition = ParseExpr();
        var message = ParseKeywordSequence(Keyword.AS) ? ParseExpr() : null;

        return new Assert(condition, message);
    }


    /// <summary>
    /// Parse an expression prefix
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException">ParserException</exception>
    public Expression ParsePrefix()
    {
        // Allow dialects to override prefix parsing
        var dialectPrefix = _dialect.ParsePrefix(this);
        if (dialectPrefix != null)
        {
            return dialectPrefix;
        }

        // PostgreSQL allows any string literal to be preceded by a type name, indicating that the
        // string literal represents a literal of that type. Some examples:
        //
        //      DATE '2020-05-20'
        //      TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //      BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL extension. Complicating
        // matters is the fact that INTERVAL string literals may optionally be followed by special
        // keywords, e.g.:
        //
        //      INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error because the `date` type
        // name is not followed by a string literal, but in fact in PostgreSQL it is a valid
        // expression that should parse as the column name "date".

        var (parsed, result) = MaybeParseChecked(() =>
        {
            var dataType = ParseDataType();

            return dataType switch
            {
                DataType.Interval => ParseInterval(),
                DataType.Custom => throw new ParserException("dummy"),
                _ => new TypedString(ParseLiteralString(), dataType)
            };
        });

        if (parsed)
        {
            return result;
        }

        var token = NextToken();

        #region Prefix Expression Parsing
        LiteralValue ParseTokenValue()
        {
            PrevToken();
            return new LiteralValue(ParseValue());
        }

        Function ParseTimeFunctions(ObjectName name)
        {
            Sequence<FunctionArg>? args = null;
            Sequence<OrderByExpression>? orderBy = null;
            var special = true;

            if (ConsumeToken<LeftParen>())
            {
                (args, orderBy) = ParseOptionalArgsWithOrderBy();
                special = false;
            }

            return new Function(name)
            {
                Args = args.SafeAny() ? args : null,
                OrderBy = orderBy,
                Special = special
            };
        }

        Expression ParseCaseExpr()
        {
            Expression? operand = null;
            if (!ParseKeyword(Keyword.WHEN))
            {
                operand = ParseExpr();
                ExpectKeyword(Keyword.WHEN);
            }

            var conditions = new Sequence<Expression>();
            var results = new Sequence<Expression>();

            while (true)
            {
                conditions.Add(ParseExpr());
                ExpectKeyword(Keyword.THEN);
                results.Add(ParseExpr());
                if (!ParseKeyword(Keyword.WHEN))
                {
                    break;
                }
            }

            var elseResult = ParseInit(ParseKeyword(Keyword.ELSE), ParseExpr);
            ExpectKeyword(Keyword.END);

            return new Case(conditions, results)
            {
                Operand = operand,
                ElseResult = elseResult
            };
        }

        CastFormat? ParseOptionalCastFormat()
        {
            if (ParseKeyword(Keyword.FORMAT))
            {
                var value = ParseValue();

                if (ParseKeywordSequence(Keyword.AT, Keyword.TIME, Keyword.ZONE))
                {
                    return new CastFormat.ValueAtTimeZone(value, ParseValue());
                }

                return new CastFormat.Value(value);
            }

            return null;
        }

        Expression ParseCastExpression(Func<Expression, DataType, CastFormat?, Expression> create)
        {
            return ExpectParens(() =>
            {
                var expr = ParseExpr();
                ExpectKeyword(Keyword.AS);
                var dataType = ParseDataType();
                var format = ParseOptionalCastFormat();
                return create(expr, dataType, format);
            });
        }

        Expression ParseTryCastExpr()
        {
            return ParseCastExpression((expr, dataType, format) => new TryCast(expr, dataType, format));
        }

        Expression ParseSafeCastExpr()
        {
            return ParseCastExpression((expr, dataType, format) => new SafeCast(expr, dataType, format));
        }

        // Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
        Exists ParseExistsExpr(bool negated)
        {
            return ExpectParens(() => new Exists(ParseQuery(), negated));
        }

        Extract ParseExtractExpr()
        {
            return ExpectParens(() =>
            {
                var field = ParseDateTimeField();
                ExpectKeyword(Keyword.FROM);
                var expr = ParseExpr();
                return new Extract(expr, field);
            });
        }

        Expression ParseCeilFloorExpr(bool isCeiling)
        {
            return ExpectParens<Expression>(() =>
            {
                var expr = ParseExpr();
                // Parse `CEIL/FLOOR(expr)`
                var field = DateTimeField.NoDateTime;
                var keywordTo = ParseKeyword(Keyword.TO);
                if (keywordTo)
                {
                    // Parse `CEIL/FLOOR(Expression TO DateTimeField)`
                    field = ParseDateTimeField();
                }

                return isCeiling
                    ? new Ceil(expr, field)
                    : new Floor(expr, field);
            });
        }

        Position ParsePositionExpr()
        {
            ExpectLeftParen();

            var expr = ParseSubExpression(BetweenPrecedence);

            if (ParseKeyword(Keyword.IN))
            {
                var from = ParseExpr();
                ExpectRightParen();
                return new Position(expr, from);
            }

            throw new ParserException("Position function must include IN keyword");
        }

        Substring ParseSubstringExpr()
        {
            return ExpectParens(() =>
            {
                var expr = ParseExpr();
                Expression? fromExpr = null;
                Expression? toExpr = null;
                var special = false;

                if (_dialect.SupportsSubstringFromForExpr)
                {
                    // PARSE SUBSTRING (EXPR [FROM 1] [FOR 3])
                    if (ParseKeyword(Keyword.FROM) || ConsumeToken<Comma>())
                    {
                        fromExpr = ParseExpr();
                    }

                    if (ParseKeyword(Keyword.FOR) || ConsumeToken<Comma>())
                    {
                        toExpr = ParseExpr();
                    }
                }
                else
                {
                    // PARSE SUBSTRING(EXPR, start, length)
                    ExpectToken<Comma>();
                    fromExpr = ParseExpr();
                    ExpectToken<Comma>();
                    toExpr = ParseExpr();
                    special = true;
                }

                return new Substring(expr, fromExpr, toExpr, special);
            });
        }

        Overlay ParseOverlayExpr()
        {
            // PARSE OVERLAY (Expression PLACING Expression FROM 1 [FOR 3])
            return ExpectParens(() =>
            {
                var expr = ParseExpr();
                ExpectKeyword(Keyword.PLACING);
                var whatExpr = ParseExpr();
                ExpectKeyword(Keyword.FROM);
                var fromExpr = ParseExpr();

                Expression? forExpr = null;

                if (ParseKeyword(Keyword.FOR))
                {
                    forExpr = ParseExpr();
                }
                return new Overlay(expr, whatExpr, fromExpr, forExpr);
            });
        }

        Trim ParseTrimExpr()
        {
            return ExpectParens(() =>
            {
                var trimWhere = TrimWhereField.None;
                Sequence<Expression>? trimCharacters = null;
                if (PeekToken() is Word { Keyword: Keyword.BOTH or Keyword.LEADING or Keyword.TRAILING })
                {
                    trimWhere = ParseTrimWhere();
                }

                var expr = ParseExpr();
                Expression? trimWhat = null;

                if (ParseKeyword(Keyword.FROM))
                {
                    trimWhat = expr;
                    expr = ParseExpr();
                }
                else if (ConsumeToken<Comma>() && _dialect is SnowflakeDialect or BigQueryDialect or GenericDialect)
                {
                    trimCharacters = ParseCommaSeparated(ParseExpr);
                }

                return new Trim(expr, trimWhere, trimWhat, trimCharacters);
            });
        }

        TrimWhereField ParseTrimWhere()
        {
            if (NextToken() is Word w)
            {
                return w.Keyword switch
                {
                    Keyword.BOTH => TrimWhereField.Both,
                    Keyword.LEADING => TrimWhereField.Leading,
                    Keyword.TRAILING => TrimWhereField.Trailing,
                    _ => throw Expected("TrimWhere field")
                };
            }

            throw Expected("TrimWhere field");
        }

        // Parse a SQL LISTAGG expression, e.g. `LISTAGG(...) WITHIN GROUP (ORDER BY ...)`
        ListAgg ParseListAggExpr()
        {
            ExpectLeftParen();
            var distinct = ParseAllOrDistinct() != null;
            var expr = ParseExpr();
            // While ANSI SQL would would require the separator, Redshift makes this optional. Here we

            var separator = ParseInit(ConsumeToken<Comma>(), ParseExpr);
            ListAggOnOverflow? onOverflow = null;
            if (ParseKeywordSequence(Keyword.ON, Keyword.OVERFLOW))
            {
                if (ParseKeyword(Keyword.ERROR))
                {
                    onOverflow = new ListAggOnOverflow.Error();
                }
                else
                {
                    ExpectKeyword(Keyword.TRUNCATE);
                    Expression? filter = null;
                    var current = PeekToken();
                    if (current is Word { Keyword: Keyword.WITH or Keyword.WITHOUT })
                    {
                        //none
                    }
                    else if (current is SingleQuotedString or EscapedStringLiteral or NationalStringLiteral or HexStringLiteral)
                    {
                        filter = ParseExpr();
                    }
                    else
                    {
                        throw Expected("either filler, WITH, or WITHOUT in LISTAGG", current);
                    }

                    var withCount = ParseKeyword(Keyword.WITH);
                    if (!withCount && !ParseKeyword(Keyword.WITHOUT))
                    {
                        throw Expected("either WITH or WITHOUT in LISTAGG", current);
                    }
                    ExpectKeyword(Keyword.COUNT);
                    onOverflow = new ListAggOnOverflow.Truncate
                    {
                        Filler = filter,
                        WithCount = withCount
                    };
                }
            }
            ExpectRightParen();

            // Once again ANSI SQL requires WITHIN GROUP, but Redshift does not. Again we choose the
            // more general implementation.
            var withGroup = new Sequence<OrderByExpression>();
            if (ParseKeywordSequence(Keyword.WITHIN, Keyword.GROUP))
            {
                withGroup = ExpectParens(() =>
                {
                    ExpectKeywords(Keyword.ORDER, Keyword.BY);
                    return ParseCommaSeparated(ParseOrderByExpr);
                });
            }

            return new ListAgg(new ListAggregate(expr, distinct, separator, onOverflow, withGroup));
        }

        Expression.Array ParseLeftArray()
        {
            ExpectToken<LeftBracket>();
            return ParseArrayExpr(true);
        }

        Expression ParseLeftParen()
        {
            Expression expr;
            if (ParseKeywordSequence(Keyword.SELECT) || ParseKeyword(Keyword.WITH))
            {
                PrevToken();
                expr = new Subquery(ParseQuery());
            }
            else
            {
                var expressions = ParseCommaSeparated(ParseExpr);
                expr = expressions.Count switch
                {
                    0 => throw Expected("comma separated list with at least 1 item", PeekToken()),
                    1 => new Nested(expressions.First()),
                    _ => new Expression.Tuple(expressions)
                };
            }

            ExpectRightParen();
            if (!ConsumeToken<Period>())
            {
                return expr;
            }

            var key = NextToken() switch
            {
                Word word => word.ToIdent(),
                _ => throw Expected("identifier", PeekToken()),
            };

            return new CompositeAccess(expr, key);
        }

        ArrayAgg ParseArrayAggregateExpression()
        {
            ExpectLeftParen();
            var distinct = ParseKeyword(Keyword.DISTINCT);
            var expr = ParseExpr();
            // ANSI SQL and BigQuery define ORDER BY inside function.

            if (!_dialect.SupportsWithinAfterArrayAggregation())
            {
                var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));

                var limit = ParseInit(ParseKeyword(Keyword.LIMIT), ParseLimit);

                ExpectRightParen();
                return new ArrayAgg(new ArrayAggregate(expr)
                {
                    OrderBy = orderBy,
                    Limit = limit,
                    Distinct = distinct,
                    WithinGroup = false
                });
            }

            // Snowflake defines ORDERY BY in within group instead of inside the function like ANSI SQL
            ExpectRightParen();

            Sequence<OrderByExpression>? withinGroup = null;
            if (ParseKeywordSequence(Keyword.WITHIN, Keyword.GROUP))
            {
                ExpectLeftParen();

                withinGroup = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));

                ExpectRightParen();
            }

            return new ArrayAgg(new ArrayAggregate(expr)
            {
                OrderBy = withinGroup,
                Distinct = distinct,
                WithinGroup = true
            });
        }

        Expression ParseNot()
        {
            if (PeekToken() is Word { Keyword: Keyword.EXISTS })
            {
                ParseKeyword(Keyword.EXISTS);
                return ParseExistsExpr(true);
            }

            return new UnaryOp(ParseSubExpression(UnaryNotPrecedence), UnaryOperator.Not);
        }

        MatchAgainst ParseMatchAgainst()
        {
            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            ExpectKeyword(Keyword.AGAINST);
            ExpectLeftParen();
            // MySQL is too permissive about the value, IMO we can't validate it perfectly on syntax level.
            var match = ParseValue();

            var naturalLanguageKeywords = new[] { Keyword.IN, Keyword.NATURAL, Keyword.LANGUAGE, Keyword.MODE };
            var withQueryKeywords = new[] { Keyword.WITH, Keyword.QUERY, Keyword.EXPANSION };
            var booleanMode = new[] { Keyword.IN, Keyword.BOOLEAN, Keyword.MODE };

            var optSearchModifier = SearchModifier.None;

            if (ParseKeywordSequence(naturalLanguageKeywords))
            {
                optSearchModifier = ParseKeywordSequence(withQueryKeywords)
                    ? SearchModifier.InNaturalLanguageModeWithQueryExpansion
                    : SearchModifier.InNaturalLanguageMode;
            }
            else if (ParseKeywordSequence(booleanMode))
            {
                optSearchModifier = SearchModifier.InBooleanMode;
            }
            else if (ParseKeywordSequence(withQueryKeywords))
            {
                optSearchModifier = SearchModifier.WithQueryExpansion;
            }

            ExpectRightParen();

            return new MatchAgainst(columns, match, optSearchModifier);
        }

        Expression ParseMultipart(Word word)
        {
            var tkn = PeekToken();
            if (tkn is LeftParen or Period)
            {
                var idParts = new Sequence<Ident> { word.ToIdent() };
                while (ConsumeToken<Period>())
                {
                    switch (NextToken())
                    {
                        case Word w:
                            idParts.Add(w.ToIdent());
                            break;

                        case SingleQuotedString s:
                            idParts.Add(new Ident(s.Value, Symbols.SingleQuote));
                            break;

                        default:
                            throw Expected("an identifier or a '*' after '.'", PeekToken());
                    }
                }

                if (ConsumeToken<LeftParen>())
                {
                    PrevToken();
                    return ParseFunction(new ObjectName(idParts));
                }

                return new CompoundIdentifier(idParts);
            }

            if (tkn is SingleQuotedString or DoubleQuotedString or HexStringLiteral && word.Value.StartsWith("_"))
            {
                return new IntroducedString(word.Value, ParseIntroducedStringValue());
            }

            return new Identifier(word.ToIdent());
        }

        Expression.Array ParseArrayExpr(bool named)
        {
            if (PeekToken() is RightBracket)
            {
                NextToken();
                return new Expression.Array(new ArrayExpression(new Sequence<Expression>(), named));
            }

            var expressions = ParseCommaSeparated(ParseExpr);
            ExpectToken<RightBracket>();
            return new Expression.Array(new ArrayExpression(expressions, named));
        }

        ArraySubquery ParseArraySubquery()
        {
            return ExpectParens(() => new ArraySubquery(ParseQuery()));
        }

        UnaryOp ParsePostgresOperator(Token tokenOperator)
        {
            var op = tokenOperator switch
            {
                DoubleExclamationMark => UnaryOperator.PGPrefixFactorial,
                PGSquareRoot => UnaryOperator.PGSquareRoot,
                PGCubeRoot => UnaryOperator.PGCubeRoot,
                AtSign => UnaryOperator.PGAbs,
                Tilde => UnaryOperator.PGBitwiseNot,
                _ => throw Expected("Postgres operator", tokenOperator)
            };

            return new UnaryOp(ParseSubExpression(PlusMinusPrecedence), op);
        }

        UnaryOp ParseUnary()
        {
            try
            {
                var op = token is Plus ? UnaryOperator.Plus : UnaryOperator.Minus;
                return new UnaryOp(ParseSubExpression(MulDivModOpPrecedence), op);
            }
            catch (ParserException)
            {
                throw Expected("variable value", PeekToken());
            }
        }

        #endregion

        #region Prefix Expression Patterns
        var expr = token switch
        {
            Word { Keyword: Keyword.TRUE or Keyword.FALSE or Keyword.NULL } => ParseTokenValue(),
            Word { Keyword: Keyword.CURRENT_CATALOG or Keyword.CURRENT_USER or Keyword.SESSION_USER or Keyword.USER } word
                when _dialect is PostgreSqlDialect or GenericDialect
                    => new Function(new ObjectName(word.ToIdent()))
                    {
                        Special = true
                    },

            Word { Keyword: Keyword.CURRENT_TIMESTAMP or Keyword.CURRENT_TIME or Keyword.CURRENT_DATE or Keyword.LOCALTIME or Keyword.LOCALTIMESTAMP } word
                => ParseTimeFunctions(new ObjectName(word.ToIdent())),

            Word { Keyword: Keyword.CASE } => ParseCaseExpr(),
            Word { Keyword: Keyword.CAST } => ParseCastExpression((expr, dataType, format) => new Cast(expr, dataType, format)),
            Word { Keyword: Keyword.TRY_CAST } => ParseTryCastExpr(),
            Word { Keyword: Keyword.SAFE_CAST } => ParseSafeCastExpr(),
            Word { Keyword: Keyword.EXISTS } => ParseExistsExpr(false),
            Word { Keyword: Keyword.EXTRACT } => ParseExtractExpr(),
            Word { Keyword: Keyword.CEIL } => ParseCeilFloorExpr(true),
            Word { Keyword: Keyword.FLOOR } => ParseCeilFloorExpr(false),
            Word { Keyword: Keyword.POSITION } when PeekToken() is LeftParen => ParsePositionExpr(),
            Word { Keyword: Keyword.SUBSTRING } => ParseSubstringExpr(),
            Word { Keyword: Keyword.OVERLAY } => ParseOverlayExpr(),
            Word { Keyword: Keyword.TRIM } => ParseTrimExpr(),
            Word { Keyword: Keyword.INTERVAL } => ParseInterval(),
            Word { Keyword: Keyword.LISTAGG } => ParseListAggExpr(),
            // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
            Word { Keyword: Keyword.ARRAY } when PeekToken() is LeftBracket => ParseLeftArray(),
            Word { Keyword: Keyword.ARRAY } when PeekToken() is LeftParen && _dialect is not ClickHouseDialect => ParseArraySubquery(),
            Word { Keyword: Keyword.ARRAY_AGG } => ParseArrayAggregateExpression(),
            Word { Keyword: Keyword.NOT } => ParseNot(),
            Word { Keyword: Keyword.MATCH } when _dialect is MySqlDialect or GenericDialect => ParseMatchAgainst(),
            //  TODO  STRUCT bigquery literal
            //  
            // Here `word` is a word, check if it's a part of a multi-part
            // identifier, a function call, or a simple identifier
            Word word => ParseMultipart(word),

            LeftBracket => ParseArrayExpr(false),
            Minus or Plus => ParseUnary(),

            DoubleExclamationMark
                or PGSquareRoot
                or PGCubeRoot
                or AtSign
                or Tilde
                when _dialect is PostgreSqlDialect
                    => ParsePostgresOperator(token),

            EscapedStringLiteral when _dialect is PostgreSqlDialect or GenericDialect => ParseTokenValue(),

            Number
                or SingleQuotedString
                or DoubleQuotedString
                or DollarQuotedString
                or SingleQuotedByteStringLiteral
                or DoubleQuotedByteStringLiteral
                or RawStringLiteral
                or NationalStringLiteral
                or HexStringLiteral
                    => ParseTokenValue(),

            LeftParen => ParseLeftParen(),
            Placeholder or Colon or AtSign => ParseTokenValue(),

            _ => throw Expected("an expression", token)
        };
        #endregion

        if (ParseKeyword(Keyword.COLLATE))
        {
            return new Collate(expr, ParseObjectName());
        }

        return expr;
    }

    public Expression ParseFunction(ObjectName name)
    {
        ExpectLeftParen();
        var distinct = ParseAllOrDistinct() != null;
        var (args, orderBy) = ParseOptionalArgsWithOrderBy();

        Expression? filter = null;
        if (_dialect.SupportsFilterDuringAggregation() &&
            ParseKeyword(Keyword.FILTER) &&
            ConsumeToken<LeftParen>() &&
            ParseKeyword(Keyword.WHERE))
        {
            var filterExpression = ParseExpr();
            ExpectToken<RightParen>();
            filter = filterExpression;
        }


        WindowType? over = null;

        if (ParseKeyword(Keyword.OVER))
        {
            if (ConsumeToken<LeftParen>())
            {
                var windowSpec = ParseWindowSpec();
                over = new WindowType.WindowSpecType(windowSpec);
            }
            else
            {
                over = new WindowType.NamedWindow(ParseIdentifier());
            }
        }

        return new Function(name)
        {
            Args = args.Any() ? args : null,
            Filter = filter,
            Over = over,
            Distinct = distinct,
            Special = false,
            OrderBy = orderBy
        };
    }

    public WindowFrameUnit ParseWindowFrameUnits()
    {
        var token = NextToken();
        if (token is Word w)
        {
            return w.Keyword switch
            {
                Keyword.ROWS => WindowFrameUnit.Rows,
                Keyword.RANGE => WindowFrameUnit.Range,
                Keyword.GROUPS => WindowFrameUnit.Groups,
                _ => throw Expected("ROWS, RANGE, GROUPS", token)
            };
        }

        throw Expected("ROWS, RANGE, GROUPS", token);
    }

    public WindowFrame ParseWindowFrame()
    {
        var units = ParseWindowFrameUnits();
        WindowFrameBound? startBound;
        WindowFrameBound? endBound = null;

        if (ParseKeyword(Keyword.BETWEEN))
        {
            startBound = ParseWindowFrameBound();
            ExpectKeyword(Keyword.AND);
            endBound = ParseWindowFrameBound();
        }
        else
        {
            startBound = ParseWindowFrameBound();
        }

        return new WindowFrame(units, startBound, endBound);
    }
    /// <summary>
    /// Parse `CURRENT ROW` or `{ positive number | UNBOUNDED } { PRECEDING | FOLLOWING }`
    /// </summary>
    /// <returns>WindowFrameBound</returns>
    public WindowFrameBound ParseWindowFrameBound()
    {
        if (ParseKeywordSequence(Keyword.CURRENT, Keyword.ROW))
        {
            return new WindowFrameBound.CurrentRow();
        }

        Expression? rows;
        if (ParseKeyword(Keyword.UNBOUNDED))
        {
            rows = null;
        }
        else
        {
            var token = PeekToken();
            rows = token is SingleQuotedString
                ? ParseInterval()
                : ParseExpr();
        }

        if (ParseKeyword(Keyword.PRECEDING))
        {
            return new WindowFrameBound.Preceding(rows);
        }

        if (ParseKeyword(Keyword.FOLLOWING))
        {
            return new WindowFrameBound.Following(rows);
        }

        throw Expected("PRECEDING or FOLLOWING", PeekToken());
    }

    public NamedWindowDefinition ParseNamedWindow()
    {
        var ident = ParseIdentifier();
        ExpectKeyword(Keyword.AS);
        ExpectLeftParen();
        var windowSpec = ParseWindowSpec();
        return new NamedWindowDefinition(ident, windowSpec);
    }
    /// <summary>
    /// Parse window spec expression
    /// </summary>
    public WindowSpec ParseWindowSpec()
    {
        var partitionBy = ParseInit(ParseKeywordSequence(Keyword.PARTITION, Keyword.BY), () => ParseCommaSeparated(ParseExpr));
        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));
        var windowFrame = ParseInit(!ConsumeToken<RightParen>(), () =>
        {
            var windowFrame = ParseWindowFrame();
            ExpectRightParen();
            return windowFrame;
        });

        return new WindowSpec(partitionBy, orderBy, windowFrame);
    }

    public Statement ParseCreateProcedure(bool orAlter)
    {
        var name = ParseObjectName();
        var parameters = ParseOptionalProcedureParameters();
        ExpectKeywords(Keyword.AS, Keyword.BEGIN);
        var statements = ParseStatements();
        ExpectKeyword(Keyword.END);

        return new CreateProcedure(orAlter, name, parameters, statements);
    }

    public Sequence<ProcedureParam>? ParseOptionalProcedureParameters()
    {

        if (!ConsumeToken<LeftParen>() || ConsumeToken<RightParen>())
        {
            return null;
        }

        var parameters = new Sequence<ProcedureParam>();

        while (true)
        {
            var next = PeekToken();

            if (next is Word)
            {
                parameters.Add(ParseProcedureParam());
            }

            var comma = ConsumeToken<Comma>();
            if (ConsumeToken<RightParen>())
            {
                break;
            }

            if (!comma)
            {
                throw Expected("',' or ')' after parameter definition", PeekToken());
            }
        }

        return parameters;
    }

    public ProcedureParam ParseProcedureParam()
    {
        var name = ParseIdentifier();
        var dataType = ParseDataType();
        return new ProcedureParam(name, dataType);
    }
    /// <summary>
    /// Parse create type expression
    /// </summary>
    public Statement ParseCreateType()
    {
        var name = ParseObjectName();
        ExpectKeyword(Keyword.AS);

        var attributes = new Sequence<UserDefinedTypeCompositeAttributeDef>();

        if (!ConsumeToken<LeftParen>() || ConsumeToken<RightParen>())
        {
            return new CreateType(name, new UserDefinedTypeRepresentation.Composite(attributes));
        }


        while (true)
        {
            var attributeName = ParseIdentifier();
            var attributeDataType = ParseDataType();
            ObjectName? attributeCollation = null;

            if (ParseKeyword(Keyword.COLLATE))
            {
                attributeCollation = ParseObjectName();
            }

            attributes.Add(new UserDefinedTypeCompositeAttributeDef(attributeName, attributeDataType, attributeCollation));
            var comma = ConsumeToken<Comma>();
            if (ConsumeToken<RightParen>())
            {
                // Allow trailing comma
                break;
            }

            if (!comma)
            {
                throw Expected("',' or ')' after attribute definition", PeekToken());
            }
        }

        return new CreateType(name, new UserDefinedTypeRepresentation.Composite(attributes));
    }
    /// <summary>
    /// Parse a group by expr. a group by Expression can be one of group sets, roll up, cube, or simple
    /// </summary>
    /// <returns>Expression</returns>
    public Expression ParseGroupByExpr()
    {
        //if (_dialect is not (PostgreSqlDialect or DuckDbDialect or GenericDialect))
        //{
        //    return ParseExpr();
        //}
        if (_dialect.SupportsGroupByExpression)
        {
            if (ParseKeywordSequence(Keyword.GROUPING, Keyword.SETS))
            {
                return CreateGroupExpr(false, true, e => new GroupingSets(e));
            }

            if (ParseKeyword(Keyword.CUBE))
            {
                return CreateGroupExpr(true, true, e => new Cube(e));
            }

            return ParseKeyword(Keyword.ROLLUP)
                ? CreateGroupExpr(true, true, e => new Rollup(e))
                : ParseExpr();
        }

        return ParseExpr();


        Expression CreateGroupExpr(bool liftSingleton, bool allowEmpty,
            Func<Sequence<Sequence<Expression>>, Expression> create)
        {
            return ExpectParens(() =>
            {
                var result = ParseCommaSeparated(() => ParseTuple(liftSingleton, allowEmpty));
                return create(result);
            });
        }
    }
    /// <summary>
    /// parse a tuple with `(` and `)`.
    /// If `lift_singleton` is true, then a singleton tuple is lifted to a tuple of length 1, otherwise it will fail.
    /// If `allow_empty` is true, then an empty tuple is allowed.
    /// </summary>
    public Sequence<Expression> ParseTuple(bool liftSingleton, bool allowEmpty)
    {
        if (liftSingleton)
        {
            if (ConsumeToken<LeftParen>())
            {
                Sequence<Expression> result;
                if (allowEmpty && ConsumeToken<RightParen>())
                {
                    result = new Sequence<Expression>();
                }
                else
                {
                    var expressions = ParseCommaSeparated(ParseExpr);
                    ExpectRightParen();
                    result = expressions;
                }

                return result;
            }

            return new Sequence<Expression> { ParseExpr() };
        }

        ExpectLeftParen();
        Sequence<Expression> exprList;

        if (allowEmpty && ConsumeToken<RightParen>())
        {
            exprList = new Sequence<Expression>();
        }
        else
        {
            var expressions = ParseCommaSeparated(ParseExpr);
            ExpectRightParen();
            exprList = expressions;
        }

        return exprList;
    }

    public DateTimeField ParseDateTimeField()
    {
        var token = NextToken();

        if (token is not Word word)
        {
            throw Expected("date/time field", token);
        }

        return word.Keyword switch
        {
            Keyword.YEAR => DateTimeField.Year,
            Keyword.MONTH => DateTimeField.Month,
            Keyword.WEEK => DateTimeField.Week,
            Keyword.DAY => DateTimeField.Day,
            Keyword.DAYOFWEEK => DateTimeField.DayOfWeek,
            Keyword.DAYOFYEAR => DateTimeField.DayOfYear,
            Keyword.DATE => DateTimeField.Date,
            Keyword.HOUR => DateTimeField.Hour,
            Keyword.MINUTE => DateTimeField.Minute,
            Keyword.SECOND => DateTimeField.Second,
            Keyword.CENTURY => DateTimeField.Century,
            Keyword.DECADE => DateTimeField.Decade,
            Keyword.DOY => DateTimeField.Doy,
            Keyword.DOW => DateTimeField.Dow,
            Keyword.EPOCH => DateTimeField.Epoch,
            Keyword.ISODOW => DateTimeField.Isodow,
            Keyword.ISOYEAR => DateTimeField.Isoyear,
            Keyword.ISOWEEK => DateTimeField.IsoWeek,
            Keyword.JULIAN => DateTimeField.Julian,
            Keyword.MICROSECOND => DateTimeField.Microsecond,
            Keyword.MICROSECONDS => DateTimeField.Microseconds,
            Keyword.MILLENIUM => DateTimeField.Millenium,
            Keyword.MILLENNIUM => DateTimeField.Millennium,
            Keyword.MILLISECOND => DateTimeField.Millisecond,
            Keyword.MILLISECONDS => DateTimeField.Milliseconds,
            Keyword.NANOSECOND => DateTimeField.Nanosecond,
            Keyword.NANOSECONDS => DateTimeField.Nanoseconds,
            Keyword.QUARTER => DateTimeField.Quarter,
            Keyword.TIME => DateTimeField.Time,
            Keyword.TIMEZONE => DateTimeField.Timezone,
            Keyword.TIMEZONE_HOUR => DateTimeField.TimezoneHour,
            Keyword.TIMEZONE_MINUTE => DateTimeField.TimezoneMinute,
            _ => throw Expected("date/time field", token)
        };
    }

    public Expression ParseInterval()
    {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        var value = ParseIntervalExpr();

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.

        var token = PeekToken();

        var leadingField = ParseInit(token is Word, () => GetDateTimeField((token as Word)!.Keyword));

        ulong? leadingPrecision;
        ulong? fractionalPrecision = null;
        var lastField = DateTimeField.None;

        if (leadingField == DateTimeField.Second)
        {
            // SQL mandates special syntax for `SECOND TO SECOND` literals.
            // Instead of
            //     `SECOND [(<leading precision>)] TO SECOND[(<fractional seconds precision>)]`
            // one must use the special format:
            //     `SECOND [( <leading precision> [ , <fractional seconds precision>] )]`
            (leadingPrecision, fractionalPrecision) = ParseOptionalPrecisionScale();
        }
        else
        {
            leadingPrecision = ParseOptionalPrecision();
            if (ParseKeyword(Keyword.TO))
            {
                lastField = ParseDateTimeField();
                if (lastField == DateTimeField.Second)
                {
                    fractionalPrecision = ParseOptionalPrecision();
                }

            }
        }

        return new Interval(value, leadingField, lastField)
        {
            LeadingPrecision = leadingPrecision,
            FractionalSecondsPrecision = fractionalPrecision
        };

    }

    public DateTimeField GetDateTimeField(Keyword keyword)
    {
        return Extensions.DateTimeFields.Any(kwd => kwd == keyword) ? ParseDateTimeField() : DateTimeField.None;
    }
    /// <summary>
    /// Parse an operator following an expression
    /// </summary>
    /// <param name="expr">Expression</param>
    /// <param name="precedence">Next precedence</param>
    /// <returns>Parsed expression</returns>
    public Expression ParseInfix(Expression expr, short precedence)
    {
        var infix = _dialect.ParseInfix(this, expr, precedence);

        if (infix != null)
        {
            return infix;
        }

        var token = NextToken();

        Sequence<string?>? pgOptions = null;

        #region Binary operator
        var regularBinaryOperator = token switch
        {
            Spaceship => BinaryOperator.Spaceship,
            DoubleEqual => BinaryOperator.Eq,
            Equal => BinaryOperator.Eq,
            NotEqual => BinaryOperator.NotEq,
            GreaterThan => BinaryOperator.Gt,
            GreaterThanOrEqual => BinaryOperator.GtEq,
            LessThan => BinaryOperator.Lt,
            LessThanOrEqual => BinaryOperator.LtEq,
            Plus => BinaryOperator.Plus,
            Minus => BinaryOperator.Minus,
            Multiply => BinaryOperator.Multiply,
            Modulo => BinaryOperator.Modulo,
            StringConcat => BinaryOperator.StringConcat,
            Pipe => BinaryOperator.BitwiseOr,
            Caret when _dialect is PostgreSqlDialect => BinaryOperator.PGExp,
            Caret => BinaryOperator.BitwiseXor,
            Ampersand => BinaryOperator.BitwiseAnd,
            Divide => BinaryOperator.Divide,
            DuckIntDiv when _dialect is DuckDbDialect or GenericDialect => BinaryOperator.DuckIntegerDivide,
            ShiftLeft when _dialect is PostgreSqlDialect or DuckDbDialect or GenericDialect => BinaryOperator.PGBitwiseShiftLeft,
            ShiftRight when _dialect is PostgreSqlDialect or DuckDbDialect or GenericDialect => BinaryOperator.PGBitwiseShiftRight,
            Hash when _dialect is PostgreSqlDialect => BinaryOperator.PGBitwiseXor,
            Overlap when _dialect is PostgreSqlDialect or DuckDbDialect => BinaryOperator.PGOverlap,
            Tilde => BinaryOperator.PGRegexMatch,
            TildeAsterisk => BinaryOperator.PGRegexIMatch,
            ExclamationMarkTilde => BinaryOperator.PGRegexNotMatch,
            ExclamationMarkTildeAsterisk => BinaryOperator.PGRegexNotIMatch,
            Word wrd => MatchKeyword(wrd.Keyword),
            _ => BinaryOperator.None
        };
        #endregion

        if (regularBinaryOperator is not BinaryOperator.None)
        {
            var keyword = ParseOneOfKeywords(Keyword.ANY, Keyword.ALL);

            if (keyword != Keyword.undefined)
            {
                var right = ExpectParens(() => ParseSubExpression(precedence));

                if (regularBinaryOperator != BinaryOperator.Gt &&
                    regularBinaryOperator != BinaryOperator.Lt &&
                    regularBinaryOperator != BinaryOperator.GtEq &&
                    regularBinaryOperator != BinaryOperator.LtEq &&
                    regularBinaryOperator != BinaryOperator.Eq &&
                    regularBinaryOperator != BinaryOperator.NotEq)
                {
                    throw Expected($"one of [=, >, <, =>, =<, !=] as comparison operator, found: {regularBinaryOperator}");
                }

                return keyword switch
                {
                    Keyword.ALL => new AllOp(expr, regularBinaryOperator, right),
                    Keyword.ANY => new AnyOp(expr, regularBinaryOperator, right),
                    _ => right
                };

                //return new BinaryOp(expr, regularBinaryOperator, right)
                //{
                //    PgOptions = pgOptions
                //};
            }

            return new BinaryOp(expr, regularBinaryOperator, ParseSubExpression(precedence))
            {
                PgOptions = pgOptions
            };
        }

        if (token is Word w)
        {
            var keyword = w.Keyword;

            switch (keyword)
            {
                case Keyword.IS when ParseKeyword(Keyword.NULL):
                    return new IsNull(expr);

                case Keyword.IS when ParseKeywordSequence(Keyword.NOT, Keyword.NULL):
                    return new IsNotNull(expr);

                case Keyword.IS when ParseKeyword(Keyword.TRUE):
                    return new IsTrue(expr);

                case Keyword.IS when ParseKeywordSequence(Keyword.NOT, Keyword.TRUE):
                    return new IsNotTrue(expr);

                case Keyword.IS when ParseKeyword(Keyword.FALSE):
                    return new IsFalse(expr);

                case Keyword.IS when ParseKeywordSequence(Keyword.NOT, Keyword.FALSE):
                    return new IsNotFalse(expr);

                case Keyword.IS when ParseKeyword(Keyword.UNKNOWN):
                    return new IsUnknown(expr);

                case Keyword.IS when ParseKeywordSequence(Keyword.NOT, Keyword.UNKNOWN):
                    return new IsNotUnknown(expr);

                case Keyword.IS when ParseKeywordSequence(Keyword.DISTINCT, Keyword.FROM):
                    return new IsDistinctFrom(expr, ParseExpr());

                case Keyword.IS when ParseKeywordSequence(Keyword.NOT, Keyword.DISTINCT, Keyword.FROM):
                    return new IsNotDistinctFrom(expr, ParseExpr());

                case Keyword.IS:
                    throw Expected("[NOT] NULL or TRUE|FALSE or [NOT] DISTINCT FROM after IS", PeekToken());

                case Keyword.AT:
                    {
                        if (ParseKeywordSequence(Keyword.TIME, Keyword.ZONE))
                        {
                            var timeZone = NextToken();
                            if (timeZone is SingleQuotedString s)
                            {
                                return new AtTimeZone(expr, s.Value);
                            }
                        }

                        throw new ParserException($"No infix parser for token {token}");
                    }
                case Keyword.NOT or
                    Keyword.IN or
                    Keyword.BETWEEN or
                    Keyword.LIKE or
                    Keyword.ILIKE or
                    Keyword.SIMILAR or
                    Keyword.REGEXP or
                    Keyword.RLIKE:
                    {
                        PrevToken();
                        var negated = ParseKeyword(Keyword.NOT);
                        var regexp = ParseKeyword(Keyword.REGEXP);
                        var rlike = ParseKeyword(Keyword.RLIKE);
                        if (regexp || rlike)
                        {
                            return new RLike(negated, expr, ParseSubExpression(LikePrecedence), regexp);
                        }
                        if (ParseKeyword(Keyword.IN))
                        {
                            return ParseIn(expr, negated);
                        }
                        if (ParseKeyword(Keyword.BETWEEN))
                        {
                            return ParseBetween(expr, negated);
                        }
                        if (ParseKeyword(Keyword.LIKE))
                        {
                            return new Like(expr, negated, ParseSubExpression(LikePrecedence), ParseEscapeChar());
                        }
                        if (ParseKeyword(Keyword.ILIKE))
                        {
                            return new ILike(expr, negated, ParseSubExpression(LikePrecedence), ParseEscapeChar());
                        }
                        if (ParseKeywordSequence(Keyword.SIMILAR, Keyword.TO))
                        {
                            return new SimilarTo(expr, negated, ParseSubExpression(LikePrecedence), ParseEscapeChar());
                        }

                        throw Expected("IN or BETWEEN after NOT", PeekToken());
                    }
                default:
                    // Can only happen if `get_next_precedence` got out of sync with this function
                    throw new ParserException($"No infix parser for token {token}");
            }
        }

        if (token is DoubleColon)
        {
            return ParsePgCast(expr);
        }

        if (token is ExclamationMark)
        {
            return new UnaryOp(expr, UnaryOperator.PGPostfixFactorial);
        }

        if (token is LeftBracket)
        {
            return _dialect is PostgreSqlDialect or GenericDialect
                ? ParseArrayIndex(expr)
                : ParseMapAccess(expr);
        }

        if (token is Colon)
        {
            return new JsonAccess(expr, JsonOperator.Colon, new LiteralValue(ParseValue()));
        }

        if (token is Arrow
                 or LongArrow
                 or HashArrow
                 or HashLongArrow
                 or AtArrow
                 or ArrowAt
                 or HashMinus
                 or AtQuestion
                 or AtAt)
        {
            var op = token switch
            {
                Arrow => JsonOperator.Arrow,
                LongArrow => JsonOperator.LongArrow,
                HashArrow => JsonOperator.HashArrow,
                HashLongArrow => JsonOperator.HashLongArrow,
                AtArrow => JsonOperator.AtArrow,
                ArrowAt => JsonOperator.ArrowAt,
                HashMinus => JsonOperator.HashMinus,
                AtQuestion => JsonOperator.AtQuestion,
                AtAt => JsonOperator.AtAt,
                _ => JsonOperator.None,
            };
            return new JsonAccess(expr, op, ParseExpr());
        }

        throw new ParserException($"No infix parser token for {token}");

        BinaryOperator MatchKeyword(Keyword keyword)
        {
            var op = keyword switch
            {
                Keyword.AND => BinaryOperator.And,
                Keyword.OR => BinaryOperator.Or,
                Keyword.XOR => BinaryOperator.Xor,
                Keyword.OPERATOR when _dialect is PostgreSqlDialect or GenericDialect => ParseOperator(),
                _ => BinaryOperator.None
            };

            return op;
        }

        BinaryOperator ParseOperator()
        {
            pgOptions = ExpectParens(() =>
            {
                var values = new Sequence<string?>();

                while (true)
                {
                    values.Add(NextToken().ToString());
                    if (!ConsumeToken<Period>())
                    {
                        break;
                    }
                }

                return values;
            });

            return BinaryOperator.PGCustomBinaryOperator;
        }
    }
    /// <summary>
    /// Parse the ESCAPE CHAR portion of LIKE, ILIKE, and SIMILAR TO
    /// </summary>
    public char? ParseEscapeChar()
    {
        if (ParseKeyword(Keyword.ESCAPE))
        {
            return ParseLiteralChar();
        }

        return null;
    }

    public Expression ParseArrayIndex(Expression expr)
    {
        var index = ParseExpr();
        ExpectToken<RightBracket>();
        var indexes = new Sequence<Expression> { index };

        while (ConsumeToken<LeftBracket>())
        {
            var innerIndex = ParseExpr();
            ExpectToken<RightBracket>();
            indexes.Add(innerIndex);
        }

        return new ArrayIndex(expr, indexes);
    }

    public Expression ParseMapAccess(Expression expr)
    {
        var key = ParseMapKey();

        ConsumeToken<RightBracket>();
        var keyParts = new Sequence<Expression> { key };
        while (ConsumeToken<LeftBracket>())
        {
            var innerKey = ParseMapKey();
            ConsumeToken<RightBracket>();
            keyParts.Add(innerKey);
        }

        if (expr is Identifier or CompoundIdentifier)
        {
            return new MapAccess(expr, keyParts);
        }

        return expr;
    }

    /// <summary>
    /// Parses the parens following the `[ NOT ] IN` operator
    /// </summary>
    /// <param name="expr"></param>
    /// <param name="negated"></param>
    /// <returns></returns>
    public Expression ParseIn(Expression expr, bool negated)
    {
        // BigQuery allows `IN UNNEST(array_expression)`
        if (ParseKeyword(Keyword.UNNEST))
        {
            var arrayExpr = ExpectParens(ParseExpr);
            return new InUnnest(expr, arrayExpr, negated);
        }

        var inOp = ExpectParens<Expression>(() =>
        {
            if (!ParseKeyword(Keyword.SELECT) && !ParseKeyword(Keyword.WITH))
            {
                return new InList(expr, ParseCommaSeparated(ParseExpr), negated);
            }

            PrevToken();
            return new InSubquery(ParseQuery(), negated, expr);
        });

        return inOp;
    }
    /// <summary>
    /// Parses `BETWEEN low AND high`, assuming the `BETWEEN` keyword was already consumed
    /// </summary>
    public Expression ParseBetween(Expression expr, bool negated)
    {
        var low = ParseSubExpression(BetweenPrecedence);
        ExpectKeyword(Keyword.AND);
        var high = ParseSubExpression(BetweenPrecedence);

        return new Between(expr, negated, low, high);
    }
    /// <summary>
    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
    public Expression ParsePgCast(Expression expr)
    {
        return new Cast(expr, ParseDataType());
    }
    /// <summary>
    /// Get the precedence of the next token
    /// </summary>
    /// <returns>Precedence value</returns>
    public short GetNextPrecedence()
    {
        var dialectPrecedence = _dialect.GetNextPrecedence(this);
        if (dialectPrecedence != null)
        {
            return dialectPrecedence.Value;
        }

        var token = PeekToken();

        // use https://www.postgresql.org/docs/7.0/operators.htm#AEN2026 as a reference
        return token switch
        {
            Word { Keyword: Keyword.OR } => OrPrecedence,
            Word { Keyword: Keyword.AND } => AndPrecedence,
            Word { Keyword: Keyword.XOR } => XOrPrecedence,
            Word { Keyword: Keyword.AT } => GetAtPrecedence(),
            Word { Keyword: Keyword.NOT } => GetNotPrecedence(),
            Word { Keyword: Keyword.IS } => IsPrecedence,
            Word { Keyword: Keyword.IN or Keyword.BETWEEN or Keyword.OPERATOR } => BetweenPrecedence,
            Word { Keyword: Keyword.LIKE or Keyword.ILIKE or Keyword.SIMILAR or Keyword.REGEXP or Keyword.RLIKE } => LikePrecedence,
            Word { Keyword: Keyword.DIV } => MulDivModOpPrecedence,

            Equal
                or LessThan
                or LessThanOrEqual
                or NotEqual
                or GreaterThan
                or GreaterThanOrEqual
                or DoubleEqual
                or Tilde
                or TildeAsterisk
                or ExclamationMarkTilde
                or ExclamationMarkTildeAsterisk
                or Spaceship
                => BetweenPrecedence,

            Pipe => PipePrecedence,

            Caret
                or Hash
                or ShiftRight
                or ShiftLeft
                => CaretPrecedence,

            Ampersand => AmpersandPrecedence,
            Plus or Minus => PlusMinusPrecedence,

            Multiply
                or Divide
                or DuckIntDiv
                or Modulo
                or StringConcat
                => MulDivModOpPrecedence, // MultiplyPrecedence,


            DoubleColon
                or Colon
                or ExclamationMark => ArrowPrecedence,

            LeftBracket
                or LongArrow
                or Arrow
                or Overlap
                or HashArrow
                or HashLongArrow
                or AtArrow
                or ArrowAt
                or HashMinus
                or AtQuestion
                or AtAt
                => ArrowPrecedence,

            _ => 0
        };

        short GetAtPrecedence()
        {
            if (PeekNthToken(1) is Word && PeekNthToken(2) is Word)
            {
                return 20; // time zone precedence
            }

            return 0;
        }

        // The precedence of NOT varies depending on keyword that
        // follows it. If it is followed by IN, BETWEEN, or LIKE,
        // it takes on the precedence of those tokens. Otherwise it
        // is not an infix operator, and therefore has zero
        // precedence.
        short GetNotPrecedence()
        {
            return PeekNthToken(1) switch
            {
                Word { Keyword: Keyword.IN or Keyword.BETWEEN } => BetweenPrecedence,
                Word { Keyword: Keyword.LIKE or Keyword.ILIKE or Keyword.SIMILAR or Keyword.REGEXP or Keyword.RLIKE } => LikePrecedence,
                _ => 0
            };
        }
    }
    /// <summary>
    /// Gets the next token in the queue without advancing the current
    /// location.  If an overrun would exist, an EOF token is returned.
    /// </summary>
    /// <returns>Returns the next token</returns>
    public Token PeekToken()
    {
        return PeekNthToken(0);
    }
    /// <summary>
    /// Gets a token at the Nth position from the current parser location.
    /// If an overrun would exist, an EOF token is returned.
    /// </summary>
    /// <param name="nth">Number of index values to skip</param>
    /// <returns>Returns the Nth next token</returns>
    public Token PeekNthToken(int nth)
    {
        var n = nth;
        var index = _index;
        while (true)
        {
            index++;
            var position = index - 1;

            if (position >= _tokens.Count)
            {
                return new EOF();
            }

            var token = _tokens[position];

            // Whitespace is ignored while building the AST
            if (token is Whitespace)
            {
                continue;
            }

            if (n == 0)
            {
                return token;
            }

            n--;
        }
    }
    /// <summary>
    /// Checks if the Nth peeked token is of a given type
    /// </summary>
    /// <typeparam name="T">Token type to check</typeparam>
    /// <param name="nth">Number of index values to skip</param>
    /// <returns>True if the type matches the peeked token; otherwise false.</returns>
    public bool PeekNthTokenIs<T>(int nth) where T : Token
    {
        var token = PeekNthToken(nth);

        return token.GetType() == typeof(T);
    }
    /// <summary>
    /// Checks if the peeked token is of a given type
    /// </summary>
    /// <typeparam name="T">Token type to check</typeparam>
    /// <returns>True if the type matches the peeked token; otherwise false.</returns>
    public bool PeekTokenIs<T>() where T : Token
    {
        return PeekToken().GetType() == typeof(T);
    }
    /// <summary>
    /// Gets the next token in the queue.
    /// </summary>
    /// <returns>Returns the next token or EOF</returns>
    public Token NextToken()
    {
        while (true)
        {
            _index++;
            var position = _index - 1;
            if (position >= _tokens.Count)
            {
                return new EOF();
            }

            var token = _tokens[_index - 1];
            if (token is Whitespace)
            {
                continue;
            }

            return token;
        }
    }

    public Token? NextTokenNoSkip()
    {
        _index++;
        return _index - 1 >= _tokens.Count ? null : _tokens[_index - 1];
    }
    /// <summary>
    /// Rewinds the token queue to the previous non-whitespace token
    /// </summary>
    public void PrevToken()
    {
        while (true)
        {
            _index--;

            if (_index >= _tokens.Count)
            {
                return;
            }

            var token = _tokens[_index];

            if (token is Whitespace)
            {
                continue;
            }

            return;
        }
    }
    /// <summary>
    /// Look for an expected sequence of keywords and consume them if they exist
    /// </summary>
    /// <param name="expected">Expected keyword</param>
    /// <returns>True if found; otherwise false</returns>
    public bool ParseKeyword(Keyword expected)
    {
        var token = PeekToken();
        if (token is not Word word || word.Keyword != expected)
        {
            return false;
        }

        NextToken();
        return true;
    }
    /// <summary>
    /// Look for an expected keyword and consume it if it exists
    /// </summary>
    /// <param name="expected">Expected keyword kind</param>
    /// <returns>True if the expected keyword exists; otherwise false.</returns>
    public bool ParseKeywordSequence(params Keyword[] expected)
    {
        var index = _index;

        if (expected.All(ParseKeyword)) return true;

        _index = index;
        return false;
    }
    /// <summary>
    /// Parses the keyword list and returns the first keyword
    /// found that matches one of the input keywords
    /// </summary>
    /// <param name="keywords">Keyword list to check</param>
    /// <returns>Keyword if found; otherwise Keyword.undefined</returns>
    public Keyword ParseOneOfKeywords(params Keyword[] keywords)
    {
        return ParseOneOfKeywords((IEnumerable<Keyword>)keywords);
    }
    /// <summary>
    /// Look for an expected keyword and consume it if it exists
    /// </summary>
    /// <param name="keywords"></param>
    /// <returns>Parsed keyword</returns>
    public Keyword ParseOneOfKeywords(IEnumerable<Keyword> keywords)
    {
        var token = PeekToken();
        var keywordList = keywords.ToList();

        if (token is Word word)
        {
            var found = keywordList.Any(k => k == word.Keyword);

            if (found)
            {
                var keyword = keywordList.First(k => k == word.Keyword);

                NextToken();
                return keyword;
            }
        }

        return Keyword.undefined;
    }
    /// <summary>
    /// Expects the next keyword to be of a specific type.  If the keyword
    /// is not found, an exception is thrown. This makes it possible
    /// to notify the user of the unexpected SQL condition and well
    /// as break the parsing control flow
    /// </summary>
    /// <param name="keyword">Expected keyword</param>
    /// <exception cref="ParserException">Parser exception with expectation detail</exception>
    public void ExpectKeyword(Keyword keyword)
    {
        if (!ParseKeyword(keyword))
        {
            throw Expected($"{keyword}", PeekToken());
        }
    }
    /// <summary>
    /// Expect at of the keywords exist in sequence
    /// </summary>
    /// <param name="keywords">Expected keyword</param>
    /// <exception cref="ParserException">Parser exception with expectation detail</exception>
    public void ExpectKeywords(params Keyword[] keywords)
    {
        foreach (var keyword in keywords)
        {
            ExpectKeyword(keyword);
        }
    }
    /// <summary>
    /// Expect one lf the keywords exist in sequence
    /// </summary>
    /// <param name="keywords">Expected keyword</param>
    /// <exception cref="ParserException">Parser exception with expectation detail</exception>
    public Keyword ExpectOneOfKeywords(params Keyword[] keywords)
    {
        var keyword = ParseOneOfKeywords(keywords);
        if (keyword != Keyword.undefined)
        {
            return keyword;
        }

        var expected = string.Join(',', keywords);
        throw Expected($"one of the keywords {expected}");
    }
    /// <summary>
    ///  Consume the next token to check for a matching type.  
    /// </summary>
    /// <typeparam name="T">Expected token type</typeparam>
    /// <returns>True if it matches the expected token; otherwise false</returns>
    public bool ConsumeToken<T>() where T : Token
    {
        if (!PeekTokenIs<T>())
        {
            return false;
        }

        NextToken();
        return true;
    }
    /// <summary>
    /// Attempts to consume the current token and checks the next token type.
    /// If the type is not a match, and exception is thrown.  This makes
    /// it possible to notify the user of the unexpected SQL condition
    /// and well as break the parser control flow
    /// </summary>
    /// <typeparam name="T">Expected Token type</typeparam>
    /// <exception cref="ParserException">Parser exception with expectation detail</exception>
    public void ExpectToken<T>() where T : Token, new()
    {
        if (!ConsumeToken<T>())
        {
            var token = new T();
            ThrowExpectedToken(token, PeekToken());
        }
    }
    /// <summary>
    /// Parse a comma-separated list of 1+ SelectItem
    /// </summary>
    /// <returns>:ist of select items</returns>
    public Sequence<SelectItem> ParseProjection()
    {
        // BigQuery allows trailing commas, but only in project lists
        // e.g. `SELECT 1, 2, FROM t`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#trailing_commas
        //
        // This pattern could be captured better with RAII type semantics, but it's quite a bit of
        // code to add for just one case, so we'll just do it manually here.
        var oldValue = _options.TrailingCommas;

        _options.TrailingCommas |= _dialect is BigQueryDialect;

        var result = ParseCommaSeparated(ParseSelectItem);
        _options.TrailingCommas = oldValue;

        return result;
    }
    /// <summary>
    /// Parse a comma-separated list of 1+ items accepted by type T
    /// </summary>
    /// <typeparam name="T">Type of item to parse</typeparam>
    /// <param name="action">Parse action</param>
    /// <returns>List of T instances</returns>
    public Sequence<T> ParseCommaSeparated<T>(Func<T> action)
    {
        var values = new Sequence<T>();

        while (true)
        {
            values.Add(action());
            if (!ConsumeToken<Comma>())
            {
                break;
            }

            if (_options.TrailingCommas)
            {
                var token = PeekToken();
                if (token is Word w)
                {
                    if (Keywords.ReservedForColumnAlias.Any(k => k == w.Keyword))
                    {
                        break;
                    }
                }
                else if (token is RightParen or SemiColon or EOF or RightBracket or RightBrace)
                {
                    break;
                }

                // continue
            }
        }

        return values;
    }
    /// <summary>
    /// Run a parser method reverting back to the current position if unsuccessful.
    /// 
    /// </summary>
    /// <param name="action">Expression to parse</param>
    /// <returns>Instance of type T if successful; otherwise default T</returns>
    public T? MaybeParse<T>(Func<T> action)
    {
        var index = _index;

        try
        {
            return action();
        }
        catch (ParserException)
        {
            // failed; reset the parser index.
        }

        _index = index;
        return default;
    }
    /// <summary>
    /// Run a parser method reverting back to the current position if unsuccessful.
    /// The result is checked against the default value for genetic type T. The
    /// returned tuple is true if the result is not the default value, otherwise
    /// the return value is false.  This allows for control flow after the method
    /// call where Rust uses a macro to handle control flow.  C# cannot return from
    /// the surrounding method from within a helper.
    ///
    /// Rust has other control flow features this project would benefit from.  In
    /// particular, Rust can break out of a nested context (loop, match, etc) to a
    /// specified outer context by label name.  There are areas in this project that
    /// create a variable for the sole purpose of breaking an outer loop.  This
    /// helper method attempts to give the same context without the language feature
    /// of intercepting control flow
    /// </summary>
    /// <typeparam name="T">Type of parser method return value. </typeparam>
    /// <param name="action">Parser action to return</param>
    /// <returns>Parser Check with containing generic value.  True if T is not default; otherwise false</returns>
    public MaybeParsed<T> MaybeParseChecked<T>(Func<T> action)
    {
        var result = MaybeParse(action);
        var isDefault = EqualityComparer<T>.Default.Equals(result, default);
        return new MaybeParsed<T>(!isDefault, result!);
    }
    /// <summary>
    /// Parse either `ALL` or `DISTINCT`. Returns `true` if `DISTINCT` is parsed and results in a
    /// </summary>
    /// <returns>True if All or Distinct</returns>
    /// <exception cref="ParserException"></exception>
    public DistinctFilter? ParseAllOrDistinct()
    {
        //var location = PeekToken();
        var all = ParseKeyword(Keyword.ALL);
        var distinct = ParseKeyword(Keyword.DISTINCT);

        if (!distinct)
        {
            return null;
        }

        if (all)
        {
            throw new ParserException("Cannot specify both ALL and DISTINCT");
        }

        var on = ParseKeyword(Keyword.ON);

        if (!on)
        {
            return new DistinctFilter.Distinct();
        }

        ExpectLeftParen();
        Sequence<Expression> columnNames;

        if (ConsumeToken<RightParen>())
        {
            PrevToken();
            columnNames = new Sequence<Expression>();
        }
        else
        {
            columnNames = ParseCommaSeparated(ParseExpr);
        }

        ExpectRightParen();
        return new DistinctFilter.On(columnNames);
    }
    /// <summary>
    /// Parse a SQL CREATE statement
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Statement ParseCreate()
    {
        var orReplace = ParseKeywordSequence(Keyword.OR, Keyword.REPLACE);
        var orAlter = ParseKeywordSequence(Keyword.OR, Keyword.ALTER);
        var local = ParseOneOfKeywords(Keyword.LOCAL) != Keyword.undefined;
        var parsedGlobal = ParseOneOfKeywords(Keyword.GLOBAL) != Keyword.undefined;
        var transient = ParseOneOfKeywords(Keyword.TRANSIENT) != Keyword.undefined;
        bool? global = null;

        if (parsedGlobal)
        {
            global = true;
        }
        else if (local)
        {
            global = false;
        }

        var temporary = ParseOneOfKeywords(Keyword.TEMP, Keyword.TEMPORARY) != Keyword.undefined;

        if (ParseKeyword(Keyword.TABLE))
        {
            return ParseCreateTable(orReplace, temporary, global, transient);
        }

        if (ParseKeyword(Keyword.MATERIALIZED) || ParseKeyword(Keyword.VIEW))
        {
            PrevToken();
            return ParseCreateView(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.EXTERNAL))
        {
            return ParseCreateExternalTable(orReplace);
        }

        if (ParseKeyword(Keyword.FUNCTION))
        {
            return ParseCreateFunction(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.MACRO))
        {
            return ParseCreateMacro(orReplace, temporary);
        }

        if (orReplace)
        {
            ThrowExpected("[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE", PeekToken());
        }

        if (ParseKeyword(Keyword.INDEX))
        {
            return ParseCreateIndex(false);
        }

        if (ParseKeywordSequence(Keyword.UNIQUE, Keyword.INDEX))
        {
            return ParseCreateIndex(true);
        }

        if (ParseKeyword(Keyword.VIRTUAL))
        {
            return ParseCreateVirtualTable();
        }

        if (ParseKeyword(Keyword.SCHEMA))
        {
            return ParseCreateSchema();
        }

        if (ParseKeyword(Keyword.DATABASE))
        {
            return ParseCrateDatabase();
        }

        if (ParseKeyword(Keyword.ROLE))
        {
            return ParseCrateRole();
        }

        if (ParseKeyword(Keyword.SEQUENCE))
        {
            return ParseCrateSequence(temporary);
        }

        if (ParseKeyword(Keyword.TYPE))
        {
            return ParseCreateType();
        }

        if (ParseKeyword(Keyword.PROCEDURE))
        {
            return ParseCreateProcedure(orAlter);
        }

        throw Expected("Expected an object type after CREATE", PeekToken());
    }
    /// <summary>
    /// Parse a CACHE TABLE statement
    /// </summary>
    /// <returns></returns>
    public Cache ParseCacheTable()
    {
        ObjectName? tableFlag = null;
        Sequence<SqlOption>? options = null;
        var hasAs = false;
        Statement.Select? query = null;

        if (ParseKeyword(Keyword.TABLE))
        {
            var tableName = ParseObjectName();
            var token = PeekToken();
            switch (token)
            {
                case EOF:
                    return new Cache(tableName)
                    {
                        TableFlag = tableFlag,
                        HasAs = hasAs,
                        Options = options,
                        Query = query
                    };
                case Word { Keyword: Keyword.OPTIONS }:
                    options = ParseOptions(Keyword.OPTIONS);
                    break;
            }

            if (PeekToken() is not EOF)
            {
                (hasAs, query) = ParseAsQuery();
            }

            return new Cache(tableName)
            {
                TableFlag = tableFlag,
                HasAs = hasAs,
                Options = options,
                Query = query
            };
        }

        tableFlag = ParseObjectName();
        if (ParseKeyword(Keyword.TABLE))
        {
            var tableName = ParseObjectName();
            var token = PeekToken();
            switch (token)
            {
                case EOF:
                    return new Cache(tableName)
                    {
                        TableFlag = tableFlag,
                        HasAs = hasAs,
                        Options = options,
                        Query = query
                    };
                case Word { Keyword: Keyword.OPTIONS }:
                    options = ParseOptions(Keyword.OPTIONS);
                    break;
            }

            if (PeekToken() is not EOF)
            {
                (hasAs, query) = ParseAsQuery();
            }

            return new Cache(tableName)
            {
                TableFlag = tableFlag,
                HasAs = hasAs,
                Options = options,
                Query = query
            };

        }
        else
        {
            var token = PeekToken();
            if (token is EOF)
            {
                PrevToken();
            }

            throw Expected("a TABLE keyword", PeekToken());
        }
    }
    /// <summary>
    /// Parse as Select statement
    /// </summary>
    /// <returns>True if parsed and parsed Select statement</returns>
    public (bool, Statement.Select) ParseAsQuery()
    {
        var token = PeekToken();
        if (token is Word word)
        {
            if (word.Keyword == Keyword.AS)
            {
                NextToken();
                return (true, ParseQuery());
            }
            return (false, ParseQuery());
        }

        throw Expected("Expected a QUERY statement", token);
    }
    // ReSharper disable once IdentifierTypo
    /// <summary>
    /// Parse a UNCACHE TABLE statement
    /// </summary>
    /// <returns></returns>
    public UNCache ParseUncacheTable()
    {
        var hasTable = ParseKeyword(Keyword.TABLE);
        if (hasTable)
        {
            var ifExists = ParseIfExists();
            var tableName = ParseObjectName();

            if (PeekToken() is EOF)
            {
                return new UNCache(tableName, ifExists);
            }

            throw Expected("EOF", PeekToken());
        }

        throw Expected("'TABLE' keyword", PeekToken());
    }
    /// <summary>
    /// SQLite-specific `CREATE VIRTUAL TABLE`
    /// </summary>
    /// <returns></returns>
    public CreateVirtualTable ParseCreateVirtualTable()
    {
        ExpectKeyword(Keyword.TABLE);
        var ifNotExists = ParseIfNotExists();
        var tableName = ParseObjectName();
        ExpectKeyword(Keyword.USING);

        var moduleName = ParseIdentifier();
        // SQLite docs note that module "arguments syntax is sufficiently
        // general that the arguments can be made to appear as column
        // definitions in a traditional CREATE TABLE statement", but
        // we don't implement that.
        var moduleArgs = ParseParenthesizedColumnList(IsOptional.Optional, false);

        return new CreateVirtualTable(tableName)
        {
            IfNotExists = ifNotExists,
            ModuleName = moduleName,
            ModuleArgs = moduleArgs
        };
    }

    public CreateSchema ParseCreateSchema()
    {
        var ifNotExists = ParseIfNotExists();
        var schemaName = ParseSchemaName();
        return new CreateSchema(schemaName, ifNotExists);
    }

    public SchemaName ParseSchemaName()
    {
        if (ParseKeyword(Keyword.AUTHORIZATION))
        {
            return new SchemaName.UnnamedAuthorization(ParseIdentifier());
        }

        var name = ParseObjectName();
        if (ParseKeywordSequence(Keyword.AUTHORIZATION))
        {
            return new SchemaName.NamedAuthorization(name, ParseIdentifier());
        }

        return new SchemaName.Simple(name);
    }

    public CreateDatabase ParseCrateDatabase()
    {
        var ifNotExists = ParseIfNotExists();
        var dbName = ParseObjectName();
        string? location = null;
        string? managedLocation = null;

        while (true)
        {
            var keyword = ParseOneOfKeywords(Keyword.LOCATION, Keyword.MANAGEDLOCATION);

            if (keyword == Keyword.LOCATION)
            {
                location = ParseLiteralString();
            }
            else if (keyword == Keyword.MANAGEDLOCATION)
            {
                managedLocation = ParseLiteralString();
            }
            else
            {
                break;
            }
        }

        return new CreateDatabase(dbName)
        {
            IfNotExists = ifNotExists,
            Location = location,
            ManagedLocation = managedLocation
        };
    }

    public CreateFunctionUsing ParseOptionalCreateFunctionUsing()
    {
        if (!ParseKeyword(Keyword.USING))
        {
            return new CreateFunctionUsing.None();
        }

        var keyword = ExpectOneOfKeywords(Keyword.JAR, Keyword.FILE, Keyword.ARCHIVE);
        var uri = ParseLiteralString();

        return keyword switch
        {
            Keyword.JAR => new CreateFunctionUsing.Jar(uri),
            Keyword.FILE => new CreateFunctionUsing.File(uri),
            Keyword.ARCHIVE => new CreateFunctionUsing.Archive(uri),
            _ => throw Expected($"JAR, FILE, or ARCHIVE, found {keyword}")
        };
    }

    public CreateFunction ParseCreateFunction(bool orReplace, bool temporary)
    {
        if (_dialect is HiveDialect)
        {
            var name = ParseObjectName();
            ExpectKeyword(Keyword.AS);
            var className = ParseFunctionDefinition();
            var parameters = new CreateFunctionBody
            {
                As = className,
                Using = ParseOptionalCreateFunctionUsing()
            };

            return new CreateFunction(name, parameters)
            {
                OrReplace = orReplace,
                Temporary = temporary,
            };
        }

        if (_dialect is PostgreSqlDialect)
        {
            var name = ParseObjectName();

            var args = ExpectParens(() =>
            {
                Sequence<OperateFunctionArg>? fnArgs = null;
                if (ConsumeToken<RightParen>())
                {
                    PrevToken();
                }
                else
                {
                    fnArgs = ParseCommaSeparated(ParseFunctionArg);
                }

                return fnArgs;
            });

            var returnType = ParseInit(ParseKeyword(Keyword.RETURNS), ParseDataType);
            var parameters = ParseCreateFunctionBody();

            return new CreateFunction(name, parameters)
            {
                OrReplace = orReplace,
                Temporary = temporary,
                Args = args,
                ReturnType = returnType,
            };
        }

        PrevToken();
        throw Expected("an object type after CREATE", PeekToken());
    }

    public OperateFunctionArg ParseFunctionArg()
    {
        var mode = ArgMode.None;
        if (ParseKeyword(Keyword.IN))
        {
            mode = ArgMode.In;
        }
        else if (ParseKeyword(Keyword.OUT))
        {
            mode = ArgMode.Out;
        }
        else if (ParseKeyword(Keyword.INOUT))
        {
            mode = ArgMode.InOut;
        }

        // parse: [ argname ] argtype
        Ident? name = null;
        var dataType = ParseDataType();

        if (dataType is DataType.Custom d)
        {
            name = d.Name.Values.First();
            dataType = ParseDataType();
        }

        var defaultExpr = ParseInit(ParseKeyword(Keyword.DEFAULT) || ConsumeToken<Equal>(), ParseExpr);
        return new OperateFunctionArg(mode)
        {
            Name = name,
            DataType = dataType,
            DefaultExpr = defaultExpr,
        };
    }

    public CreateFunctionBody ParseCreateFunctionBody()
    {
        var body = new CreateFunctionBody();
        const string immutable = "IMMUTABLE | STABLE | VOLATILE";

        while (true)
        {
            if (ParseKeyword(Keyword.AS))
            {
                EnsureNotSet(body.As, "AS");

                body.As = ParseFunctionDefinition();
            }
            else if (ParseKeyword(Keyword.LANGUAGE))
            {
                EnsureNotSet(body.Language, "LANGUAGE");
                body.Language = ParseIdentifier();
            }
            else if (ParseKeyword(Keyword.IMMUTABLE))
            {
                EnsureNotSet(body.Behavior, immutable);
                body.Behavior = FunctionBehavior.Immutable;
            }
            else if (ParseKeyword(Keyword.STABLE))
            {
                EnsureNotSet(body.Behavior, immutable);
                body.Behavior = FunctionBehavior.Stable;
            }
            else if (ParseKeyword(Keyword.VOLATILE))
            {
                EnsureNotSet(body.Behavior, immutable);
                body.Behavior = FunctionBehavior.Volatile;
            }
            else if (ParseKeyword(Keyword.RETURN))
            {
                EnsureNotSet(body.Return, "RETURN");
                body.Return = ParseExpr();
            }
            else
            {
                return body;
            }
        }

        void EnsureNotSet(object? field, string fieldName)
        {
            if (field is not null)
            {
                throw new ParserException($"{fieldName} specified more than once");
            }
        }
    }
    /// <summary>
    /// DuckDb create macro statement
    /// </summary>
    /// <param name="orReplace"></param>
    /// <param name="temporary"></param>
    /// <returns>DuckDb CreateMacro statement</returns>
    public Statement ParseCreateMacro(bool orReplace, bool temporary)
    {
        if (_dialect is DuckDbDialect or GenericDialect)
        {
            var name = ParseObjectName();
            ExpectLeftParen();
            Sequence<MacroArg>? args = null;

            if (ConsumeToken<RightParen>())
            {
                PrevToken();
            }
            else
            {
                args = ParseCommaSeparated(ParseMacroArg);
            }

            ExpectRightParen();
            ExpectKeyword(Keyword.AS);

            MacroDefinition def = ParseKeyword(Keyword.TABLE)
                ? new MacroDefinition.MacroTable(ParseQuery())
                : new MacroDefinition.MacroExpression(ParseExpr());

            return new CreateMacro(orReplace, temporary, name, args, def);
        }

        PrevToken();
        throw Expected("an object type after CREATE", PeekToken());
    }
    /// <summary>
    /// Parse DuckDb macro argument
    /// </summary>
    /// <returns>Macro argument</returns>
    public MacroArg ParseMacroArg()
    {
        var name = ParseIdentifier();
        Expression? defaultExpression = null;

        if (ConsumeToken<DuckAssignment>() || ConsumeToken<RightArrow>())
        {
            defaultExpression = ParseExpr();
        }

        return new MacroArg(name, defaultExpression);
    }

    public CreateTable ParseCreateExternalTable(bool orReplace)
    {
        ExpectKeyword(Keyword.TABLE);

        var ifNotExists = ParseIfNotExists();
        var tableName = ParseObjectName();
        var (columns, constraints) = ParseColumns();

        var hiveDistribution = ParseHiveDistribution();
        var hiveFormats = ParseHiveFormats();

        var fileFormat = FileFormat.None;
        if (hiveFormats?.Storage is HiveIOFormat.FileFormat f)
        {
            fileFormat = f.Format;
        }

        var location = hiveFormats?.Location;
        var tableProperties = ParseOptions(Keyword.TBLPROPERTIES);

        return new CreateTable(tableName, columns)
        {
            Constraints = constraints.Any() ? constraints : null,
            HiveDistribution = hiveDistribution,
            HiveFormats = hiveFormats,
            TableProperties = tableProperties.Any() ? tableProperties : null,
            OrReplace = orReplace,
            IfNotExists = ifNotExists,
            External = true,
            FileFormat = fileFormat,
            Location = location,
        };
    }

    public FileFormat ParseFileFormat()
    {
        var token = NextToken();

        return token switch
        {
            Word { Keyword: Keyword.AVRO } => FileFormat.Avro,
            Word { Keyword: Keyword.JSONFILE } => FileFormat.JsonFile,
            Word { Keyword: Keyword.ORC } => FileFormat.Orc,
            Word { Keyword: Keyword.PARQUET } => FileFormat.Parquet,
            Word { Keyword: Keyword.RCFILE } => FileFormat.RcFile,
            Word { Keyword: Keyword.SEQUENCEFILE } => FileFormat.SequenceFile,
            Word { Keyword: Keyword.TEXTFILE } => FileFormat.TextFile,
            _ => throw Expected("fileformat", token)
        };
    }

    public CreateView ParseCreateView(bool orReplace, bool temporary)
    {
        var materialized = ParseKeyword(Keyword.MATERIALIZED);
        ExpectKeyword(Keyword.VIEW);

        var ifNotExists = _dialect is SQLiteDialect or GenericDialect && ParseIfNotExists();

        var name = ParseObjectName();
        var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);
        var withOptions = ParseOptions(Keyword.WITH);

        var clusterBy = ParseInit(ParseKeyword(Keyword.CLUSTER), () =>
        {
            ExpectKeyword(Keyword.BY);
            return ParseParenthesizedColumnList(IsOptional.Optional, false);
        });

        ExpectKeyword(Keyword.AS);
        var query = ParseQuery();

        var withNoBinding = _dialect is RedshiftDialect or GenericDialect &&
                          ParseKeywordSequence(Keyword.WITH, Keyword.NO, Keyword.SCHEMA, Keyword.BINDING);

        return new CreateView(name, query)
        {
            Columns = columns,
            WithOptions = withOptions,
            ClusterBy = clusterBy,
            Materialized = materialized,
            OrReplace = orReplace,
            WithNoSchemaBinding = withNoBinding,
            IfNotExists = ifNotExists,
            Temporary = temporary
        };
    }

    public CreateRole ParseCrateRole()
    {
        var ifNotExists = ParseIfNotExists();
        var names = ParseCommaSeparated(ParseObjectName);

        _ = ParseKeyword(Keyword.WITH);

        Sequence<Keyword> optionalKeywords = new();

        if (_dialect is MsSqlDialect)
        {
            optionalKeywords.Add(Keyword.AUTHORIZATION);
        }
        else if (_dialect is PostgreSqlDialect)
        {
            optionalKeywords.AddRange(new[]
            {
                Keyword.LOGIN,
                Keyword.NOLOGIN,
                Keyword.INHERIT,
                Keyword.NOINHERIT,
                Keyword.BYPASSRLS,
                Keyword.NOBYPASSRLS,
                Keyword.PASSWORD,
                Keyword.CREATEDB,
                Keyword.NOCREATEDB,
                Keyword.CREATEROLE,
                Keyword.NOCREATEROLE,
                Keyword.SUPERUSER,
                Keyword.NOSUPERUSER,
                Keyword.REPLICATION,
                Keyword.NOREPLICATION,
                Keyword.CONNECTION,
                Keyword.VALID,
                Keyword.IN,
                Keyword.ROLE,
                Keyword.ADMIN,
                Keyword.USER,
            });
        }

        // MSSQL
        ObjectName? authorizationOwner = null;
        // Postgres
        bool? login = null;
        bool? inherit = null;
        bool? bypassrls = null;
        Password? password = null;
        bool? createDb = null;
        bool? createRole = null;
        bool? superuser = null;
        bool? replication = null;
        Expression? connectionLimit = null;
        Expression? validUntil = null;
        Sequence<Ident>? inRole = null;
        Sequence<Ident>? inGroup = null;
        Sequence<Ident>? role = null;
        Sequence<Ident>? user = null;
        Sequence<Ident>? admin = null;

        Keyword keyword;
        var loop = true;
        while (loop && (keyword = ParseOneOfKeywords(optionalKeywords)) != Keyword.undefined)
        {
            switch (keyword)
            {
                case Keyword.AUTHORIZATION:
                    EnsureObjectNotSet(authorizationOwner, "AUTHORIZATION");
                    authorizationOwner = ParseObjectName();
                    break;

                case Keyword.LOGIN or Keyword.NOLOGIN:
                    EnsureObjectNotSet(login, "LOGIN or NOLOGIN");
                    login = keyword == Keyword.LOGIN;
                    break;

                case Keyword.INHERIT or Keyword.NOINHERIT:
                    EnsureObjectNotSet(inherit, "INHERIT or NOINHERIT");
                    inherit = keyword == Keyword.INHERIT;
                    break;

                case Keyword.BYPASSRLS or Keyword.NOBYPASSRLS:
                    EnsureObjectNotSet(bypassrls, "BYPASSRLS or NOBYPASSRLS");
                    bypassrls = keyword == Keyword.BYPASSRLS;
                    break;

                case Keyword.CREATEDB or Keyword.NOCREATEDB:
                    EnsureObjectNotSet(createDb, "CREATEDB or NOCREATEDB");
                    createDb = keyword == Keyword.CREATEDB;
                    break;

                case Keyword.CREATEROLE or Keyword.NOCREATEROLE:
                    EnsureObjectNotSet(createRole, "CREATEROLE or NOCREATEROLE");
                    createRole = keyword == Keyword.CREATEROLE;
                    break;

                case Keyword.SUPERUSER or Keyword.NOSUPERUSER:
                    EnsureObjectNotSet(superuser, "SUPERUSER or NOSUPERUSER");
                    superuser = keyword == Keyword.SUPERUSER;
                    break;

                case Keyword.REPLICATION or Keyword.NOREPLICATION:
                    EnsureObjectNotSet(replication, "REPLICATION or NOREPLICATION");
                    replication = keyword == Keyword.REPLICATION;
                    break;

                case Keyword.PASSWORD:
                    EnsureObjectNotSet(password, "PASSWORD");

                    if (ParseKeyword(Keyword.NULL))
                    {
                        password = new Password.NullPassword();
                    }
                    else
                    {
                        password = new Password.ValidPassword(new LiteralValue(ParseValue()));
                    }

                    break;

                case Keyword.CONNECTION:
                    ExpectKeyword(Keyword.LIMIT);
                    EnsureObjectNotSet(connectionLimit, "CONNECTION LIMIT");
                    connectionLimit = new LiteralValue(ParseNumberValue());
                    break;

                case Keyword.VALID:
                    ExpectKeyword(Keyword.UNTIL);
                    EnsureObjectNotSet(validUntil, "VALID UNTIL");
                    validUntil = new LiteralValue(ParseValue());
                    break;

                case Keyword.IN:
                    if (ParseKeyword(Keyword.ROLE))
                    {
                        EnsureObjectNotSet(inRole, "IN ROLE");
                        inRole = ParseCommaSeparated(ParseIdentifier);

                    }
                    else if (ParseKeyword(Keyword.GROUP))
                    {
                        EnsureObjectNotSet(inGroup, "IN GROUP");
                        inGroup = ParseCommaSeparated(ParseIdentifier);
                    }
                    else
                    {
                        ThrowExpected("ROLE or GROUP after IN", PeekToken());
                    }

                    break;

                case Keyword.ROLE:
                    EnsureObjectNotSet(role, "ROLE");
                    role = ParseCommaSeparated(ParseIdentifier);
                    break;

                case Keyword.USER:
                    EnsureObjectNotSet(user, "USER");
                    user = ParseCommaSeparated(ParseIdentifier);
                    break;

                case Keyword.ADMIN:
                    EnsureObjectNotSet(admin, "ADMIN");
                    admin = ParseCommaSeparated(ParseIdentifier);
                    break;

                default:
                    loop = false;
                    break;
            }
        }

        return new CreateRole(names)
        {
            IfNotExists = ifNotExists,
            Login = login,
            Inherit = inherit,
            BypassRls = bypassrls,
            Password = password,
            CreateDb = createDb,
            CreateDbRole = createRole,
            Replication = replication,
            Superuser = superuser,
            ConnectionLimit = connectionLimit,
            ValidUntil = validUntil,
            InRole = inRole,
            InGroup = inGroup,
            Role = role,
            User = user,
            Admin = admin,
            AuthorizationOwner = authorizationOwner
        };

        void EnsureObjectNotSet(object? field, string name)
        {
            if (field != null)
            {
                throw new ParserException($"Found multiple {name}");
            }
        }
    }

    public Statement ParseDrop()
    {
        if (ParseKeyword(Keyword.FUNCTION))
        {
            return ParseDropFunction();
        }

        var temporary = _dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.TEMPORARY);

        var objectType =
            ParseKeyword(Keyword.TABLE) ? ObjectType.Table :
            ParseKeyword(Keyword.VIEW) ? ObjectType.View :
            ParseKeyword(Keyword.INDEX) ? ObjectType.Index :
            ParseKeyword(Keyword.ROLE) ? ObjectType.Role :
            ParseKeyword(Keyword.SCHEMA) ? ObjectType.Schema :
            ParseKeyword(Keyword.SEQUENCE) ? ObjectType.Sequence :
            ParseKeyword(Keyword.STAGE) ? ObjectType.Stage :
            throw Expected("TABLE, VIEW, INDEX, ROLE, SCHEMA, FUNCTION or SEQUENCE after DROP", PeekToken());

        // Many dialects support the non standard `IF EXISTS` clause and allow
        // specifying multiple objects to delete in a single statement
        var ifExists = ParseIfExists();
        var names = ParseCommaSeparated(ParseObjectName);
        var cascade = ParseKeyword(Keyword.CASCADE);
        var restrict = ParseKeyword(Keyword.RESTRICT);
        var purge = ParseKeyword(Keyword.PURGE);

        if (cascade && restrict)
        {
            throw new ParserException("Cannot specify both CASCADE and RESTRICT in DROP");
        }

        if (objectType == ObjectType.Role && (cascade || restrict || purge))
        {
            throw new ParserException("Cannot specify CASCADE, RESTRICT, or PURGE in DROP ROLE");
        }

        return new Drop(names)
        {
            ObjectType = objectType,
            IfExists = ifExists,
            Cascade = cascade,
            Restrict = restrict,
            Purge = purge,
            Temporary = temporary
        };
    }
    /// <summary>
    ///  DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// </summary>
    public DropFunction ParseDropFunction()
    {
        var ifExists = ParseIfExists();
        var funcDesc = ParseCommaSeparated(ParseDropFunctionDesc);
        var keyword = ParseOneOfKeywords(Keyword.CASCADE, Keyword.RESTRICT);
        var option = keyword switch
        {
            Keyword.CASCADE => ReferentialAction.Cascade,
            Keyword.RESTRICT => ReferentialAction.Restrict,
            _ => ReferentialAction.None
        };

        return new DropFunction(ifExists, funcDesc, option);

        DropFunctionDesc ParseDropFunctionDesc()
        {
            var name = ParseObjectName();
            Sequence<OperateFunctionArg>? args = null;

            if (ConsumeToken<LeftParen>())
            {
                if (ConsumeToken<RightParen>())
                {
                    return new DropFunctionDesc(name);
                }

                var opArgs = ParseCommaSeparated(ParseFunctionArg);
                ExpectRightParen();
                args = opArgs;
            }

            return new DropFunctionDesc(name, args);
        }
    }
    /// <summary>
    /// DECLARE name [ BINARY ] [ ASENSITIVE | INSENSITIVE ] [ [ NO ] SCROLL ]
    ///     CURSOR [ { WITH | WITHOUT } HOLD ] FOR query
    /// </summary>
    /// <returns></returns>
    public Declare ParseDeclare()
    {
        var name = ParseIdentifier();
        var binary = ParseKeyword(Keyword.BINARY);
        bool? sensitive =
            ParseKeyword(Keyword.INSENSITIVE) ? true :
            ParseKeyword(Keyword.ASENSITIVE) ? false :
            null;

        bool? scroll =
            ParseKeyword(Keyword.SCROLL) ? true :
            ParseKeywordSequence(Keyword.NO, Keyword.SCROLL) ? false :
            null;

        ExpectKeyword(Keyword.CURSOR);

        bool? hold = null;
        var keyword = ParseOneOfKeywords(Keyword.WITH, Keyword.WITHOUT);
        if (keyword != Keyword.undefined)
        {
            ExpectKeyword(Keyword.HOLD);
            hold = keyword switch
            {
                Keyword.WITH => true,
                Keyword.WITHOUT => false,
                _ => null
            };
        }

        ExpectKeyword(Keyword.FOR);

        var query = ParseQuery();

        return new Declare(name)
        {
            Binary = binary,
            Sensitive = sensitive,
            Scroll = scroll,
            Hold = hold,
            Query = query
        };
    }
    /// <summary>
    /// FETCH [ direction { FROM | IN } ] cursor INTO target;
    /// </summary>
    /// <returns></returns>
    public Statement.Fetch ParseFetchStatement()
    {
        FetchDirection ParseForward()
        {
            if (ParseKeyword(Keyword.ALL))
            {
                return new FetchDirection.ForwardAll();
            }

            return new FetchDirection.Forward(ParseNumberValue());
        }

        FetchDirection ParseBackward()
        {
            if (ParseKeyword(Keyword.ALL))
            {
                return new FetchDirection.BackwardAll();
            }

            return new FetchDirection.Backward(ParseNumberValue());
        }

        var direction = ParseKeyword(Keyword.NEXT) ? new FetchDirection.Next() :
            ParseKeyword(Keyword.PRIOR) ? new FetchDirection.Prior() :
            ParseKeyword(Keyword.FIRST) ? new FetchDirection.First() :
            ParseKeyword(Keyword.LAST) ? new FetchDirection.Last() :
            ParseKeyword(Keyword.ABSOLUTE) ? new FetchDirection.Absolute(ParseNumberValue()) :
            ParseKeyword(Keyword.RELATIVE) ? new FetchDirection.Relative(ParseNumberValue()) :
            ParseKeyword(Keyword.FORWARD) ? ParseForward() :
            ParseKeyword(Keyword.BACKWARD) ? ParseBackward() :
            ParseKeyword(Keyword.ALL) ? new FetchDirection.All() :
            new FetchDirection.Count(ParseNumberValue());

        ExpectOneOfKeywords(Keyword.FROM, Keyword.IN);

        var name = ParseIdentifier();
        var into = ParseKeyword(Keyword.INTO) ? ParseObjectName() : null;

        return new Statement.Fetch(name, direction, into);
    }

    public Discard ParseDiscard()
    {
        var objectType =
            ParseKeyword(Keyword.ALL) ? DiscardObject.All :
            ParseKeyword(Keyword.PLANS) ? DiscardObject.Plans :
            ParseKeyword(Keyword.SEQUENCES) ? DiscardObject.Sequences :
            (ParseKeyword(Keyword.TEMP) || ParseKeyword(Keyword.TEMPORARY)) ? DiscardObject.Temp :
            throw Expected("ALL, PLANS, SEQUENCES, TEMP or TEMPORARY after DISCARD", PeekToken());

        return new Discard(objectType);
    }

    public CreateIndex ParseCreateIndex(bool unique)
    {
        var concurrently = ParseKeyword(Keyword.CONCURRENTLY);
        var ifNotExists = ParseIfNotExists();
        ObjectName? indexName = null;

        if (ifNotExists || !ParseKeyword(Keyword.ON))
        {
            indexName = ParseObjectName();
            ExpectKeyword(Keyword.ON);
        }

        var tableName = ParseObjectName();
        var @using = ParseInit(ParseKeyword(Keyword.USING), ParseIdentifier);
        var columns = ExpectParens(() => ParseCommaSeparated(ParseOrderByExpr));

        Sequence<Ident>? include = null;
        if (ParseKeyword(Keyword.INCLUDE))
        {
            include = ExpectParens(() => ParseCommaSeparated(ParseIdentifier));
        }

        bool? nullsDistinct = null;
        if (ParseKeyword(Keyword.NULLS))
        {
            var not = ParseKeyword(Keyword.NOT);
            ExpectKeyword(Keyword.DISTINCT);
            nullsDistinct = !not;
        }

        Expression? predicate = null;
        if (ParseKeyword(Keyword.WHERE))
        {
            predicate = ParseExpr();
        }

        return new CreateIndex(indexName, tableName)
        {
            Using = @using,
            Columns = columns,
            Unique = unique,
            IfNotExists = ifNotExists,
            Concurrently = concurrently,
            Include = include,
            NullsDistinct = nullsDistinct,
            Predicate = predicate
        };
    }

    public HiveDistributionStyle ParseHiveDistribution()
    {
        if (ParseKeywordSequence(Keyword.PARTITIONED, Keyword.BY))
        {
            var columns = ExpectParens(() => ParseCommaSeparated(ParseColumnDef));

            return new HiveDistributionStyle.Partitioned(columns);
        }

        return new HiveDistributionStyle.None();
    }

    public HiveFormat? ParseHiveFormats()
    {
        HiveFormat? hiveFormat = null;

        var loop = true;

        while (loop)
        {
            switch (ParseOneOfKeywords(Keyword.ROW, Keyword.STORED, Keyword.LOCATION))
            {
                case Keyword.ROW:
                    hiveFormat ??= new HiveFormat();
                    hiveFormat.RowFormat = ParseRowFormat();
                    break;

                case Keyword.STORED:
                    ExpectKeyword(Keyword.AS);
                    hiveFormat ??= new HiveFormat();
                    if (ParseKeyword(Keyword.INPUTFORMAT))
                    {
                        var inputFormat = ParseExpr();
                        ExpectKeyword(Keyword.OUTPUTFORMAT);
                        var outputFormat = ParseExpr();
                        hiveFormat.Storage = new HiveIOFormat.IOF(inputFormat, outputFormat);
                    }
                    else
                    {
                        var format = ParseFileFormat();
                        hiveFormat.Storage = new HiveIOFormat.FileFormat
                        {
                            Format = format
                        };
                    }

                    break;

                case Keyword.LOCATION:
                    hiveFormat ??= new HiveFormat();
                    hiveFormat.Location = ParseLiteralString();
                    break;

                default:
                    loop = false;
                    break;
            }
        }

        return hiveFormat;
    }

    public HiveRowFormat ParseRowFormat()
    {
        ExpectKeyword(Keyword.FORMAT);
        if (ParseOneOfKeywords(Keyword.SERDE, Keyword.DELIMITED) == Keyword.SERDE)
        {
            var @class = ParseLiteralString();

            return new HiveRowFormat.Serde(@class);
        }

        return new HiveRowFormat.Delimited();
    }

    public CreateTable ParseCreateTable(bool orReplace, bool temporary, bool? global, bool transient)
    {
        var ifNotExists = ParseIfNotExists();
        var tableName = ParseObjectName();

        // Clickhouse has `ON CLUSTER 'cluster'` syntax for DDLs
        string? onCluster = null;
        if (ParseKeywordSequence(Keyword.ON, Keyword.CLUSTER))
        {
            var token = NextToken();
            onCluster = token switch
            {
                SingleQuotedString s => s.Value,
                Word w => w.Value,
                _ => throw Expected("identifier or cluster literal", token)
            };
        }

        var like = ParseInit<ObjectName?>(ParseKeyword(Keyword.LIKE) || ParseKeyword(Keyword.ILIKE), ParseObjectName);

        var clone = ParseInit<ObjectName?>(ParseKeyword(Keyword.CLONE), ParseObjectName);

        var (columns, constraints) = ParseColumns();

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        var withoutRowId = ParseKeywordSequence(Keyword.WITHOUT, Keyword.ROWID);

        var hiveDistribution = ParseHiveDistribution();
        var hiveFormats = ParseHiveFormats();

        // PostgreSQL supports `WITH ( options )`, before `AS`
        var withOptions = ParseOptions(Keyword.WITH);
        var tableProperties = ParseOptions(Keyword.TBLPROPERTIES);

        var engine = ParseInit(ParseKeyword(Keyword.ENGINE), () =>
        {
            ExpectToken<Equal>();
            var token = NextToken();

            if (token is Word w)
            {
                return w.Value;
            }

            throw Expected("identifier", token);
        });

        var comment = ParseInit(ParseKeyword(Keyword.COMMENT), () =>
        {
            ConsumeToken<Equal>();
            var next = NextToken();
            if (next is SingleQuotedString str)
            {
                return str.Value;
            }

            throw Expected("Comment", PeekToken());
        });

        int? autoIncrementOffset = null;

        if (ParseKeyword(Keyword.AUTO_INCREMENT))
        {
            ConsumeToken<Equal>();
            var next = NextToken();
            if (next is Number number)
            {
                if (int.TryParse(number.Value, out var increment))
                {
                    autoIncrementOffset = increment;
                }
            }
        }

        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () =>
        {
            if (!ConsumeToken<LeftParen>())
            {
                return new Sequence<Ident> { ParseIdentifier() };
            }

            var cols = ParseInit(PeekToken() is not RightParen, () => ParseCommaSeparated(ParseIdentifier));
            ExpectRightParen();
            return cols;

        });

        // Parse optional `AS ( query )`
        var query = ParseInit<Query>(ParseKeyword(Keyword.AS), () => ParseQuery());

        var defaultCharset = ParseInit(ParseKeywordSequence(Keyword.DEFAULT, Keyword.CHARSET), () =>
        {
            ExpectToken<Equal>();
            var token = NextToken();

            if (token is Word w)
            {
                return w.Value;
            }

            throw Expected("identifier", token);
        });

        var collation = ParseInit(ParseKeyword(Keyword.COLLATE), () =>
        {
            ExpectToken<Equal>();
            var token = NextToken();

            if (token is Word w)
            {
                return w.Value;
            }

            throw Expected("identifier", token);
        });

        var onCommit = OnCommit.None;

        if (ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.DELETE, Keyword.ROWS))
        {
            onCommit = OnCommit.DeleteRows;
        }
        else if (ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.PRESERVE, Keyword.ROWS))
        {
            onCommit = OnCommit.PreserveRows;
        }
        else if (ParseKeywordSequence(Keyword.ON, Keyword.COMMIT, Keyword.DROP))
        {
            onCommit = OnCommit.Drop;
        }

        var strict = ParseKeyword(Keyword.STRICT);

        return new CreateTable(tableName, columns)
        {
            Temporary = temporary,
            Constraints = constraints.Any() ? constraints : null,
            WithOptions = withOptions.Any() ? withOptions : null,
            TableProperties = tableProperties.Any() ? tableProperties : null,
            OrReplace = orReplace,
            IfNotExists = ifNotExists,
            Transient = transient,
            HiveDistribution = hiveDistribution,
            HiveFormats = hiveFormats,
            Global = global,
            Query = query,
            WithoutRowId = withoutRowId,
            Like = like,
            CloneClause = clone,
            Engine = engine,
            Comment = comment,
            OrderBy = orderBy,
            DefaultCharset = defaultCharset,
            Collation = collation,
            OnCommit = onCommit,
            OnCluster = onCluster,
            Strict = strict,
            AutoIncrementOffset = autoIncrementOffset
        };
    }

    public (Sequence<ColumnDef>, Sequence<TableConstraint>) ParseColumns()
    {
        var columns = new Sequence<ColumnDef>();
        var constraints = new Sequence<TableConstraint>();

        if (!ConsumeToken<LeftParen>() || ConsumeToken<RightParen>())
        {
            return (columns, constraints);
        }

        while (true)
        {
            var constraint = ParseOptionalTableConstraint();
            if (constraint != null)
            {
                constraints.Add(constraint);
            }
            else if (PeekToken() is Word)
            {
                columns.Add(ParseColumnDef());
            }
            else
            {
                ThrowExpected("column name or constraint definition", PeekToken());
            }

            var commaFound = ConsumeToken<Comma>();

            if (ConsumeToken<RightParen>())
            {
                // allow a trailing comma, even though it's not in standard
                break;
            }

            if (!commaFound)
            {
                ThrowExpected("',' or ')' after column definition", PeekToken());
            }
        }

        return (columns, constraints);
    }

    public ColumnDef ParseColumnDef()
    {
        var name = ParseIdentifier();
        var dataType = ParseDataType();
        var collation = ParseInit(ParseKeyword(Keyword.COLLATE), ParseObjectName);
        Sequence<ColumnOptionDef>? options = null;

        while (true)
        {
            ColumnOption? opt;
            if (ParseKeyword(Keyword.CONSTRAINT))
            {
                var ident = ParseIdentifier();
                if ((opt = ParseOptionalColumnOption()) != null)
                {
                    options ??= new Sequence<ColumnOptionDef>();
                    options.Add(new ColumnOptionDef(opt, ident));
                }
                else
                {
                    throw Expected("constraint details after CONSTRAINT <name>", PeekToken());
                }
            }
            else if ((opt = ParseOptionalColumnOption()) is { })
            {
                options ??= new Sequence<ColumnOptionDef>();
                options.Add(new ColumnOptionDef(opt));
            }
            else
            {
                break;
            }
        }

        return new ColumnDef(name, dataType, collation, options);
    }

    public ColumnOption? ParseOptionalColumnOption()
    {
        if (ParseKeywordSequence(Keyword.CHARACTER, Keyword.SET))
        {
            return new ColumnOption.CharacterSet(ParseObjectName());
        }

        if (ParseKeywordSequence(Keyword.NOT, Keyword.NULL))
        {
            return new ColumnOption.NotNull();
        }

        if (ParseKeywordSequence(Keyword.COMMENT))
        {
            var token = NextToken();
            if (token is SingleQuotedString s)
            {
                return new ColumnOption.Comment(s.Value);
            }

            ThrowExpected("string", PeekToken());
        }

        if (ParseKeyword(Keyword.NULL))
        {
            return new ColumnOption.Null();
        }

        if (ParseKeyword(Keyword.DEFAULT))
        {
            return new ColumnOption.Default(ParseExpr());
        }

        if (ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY))
        {
            return new ColumnOption.Unique(true);
        }

        if (ParseKeyword(Keyword.UNIQUE))
        {
            return new ColumnOption.Unique(false);
        }

        if (ParseKeyword(Keyword.REFERENCES))
        {
            var foreignTable = ParseObjectName();
            var referredColumns = ParseParenthesizedColumnList(IsOptional.Optional, false);
            var onDelete = ReferentialAction.None;
            var onUpdate = ReferentialAction.None;

            while (true)
            {
                if (onDelete == ReferentialAction.None && ParseKeywordSequence(Keyword.ON, Keyword.DELETE))
                {
                    onDelete = ParseReferentialAction();
                }
                else if (onUpdate == ReferentialAction.None && ParseKeywordSequence(Keyword.ON, Keyword.UPDATE))
                {
                    onUpdate = ParseReferentialAction();
                }
                else
                {
                    break;
                }
            }

            return new ColumnOption.ForeignKey(
                foreignTable,
                referredColumns.Any() ? referredColumns : null,
                onDelete,
                onUpdate);
        }

        if (ParseKeyword(Keyword.CHECK))
        {
            var expr = ExpectParens(ParseExpr);

            return new ColumnOption.Check(expr);
        }

        if (ParseKeyword(Keyword.AUTO_INCREMENT) && _dialect is MySqlDialect or GenericDialect)
        {
            return new ColumnOption.DialectSpecific(new[] { new Word("AUTO_INCREMENT") });
        }

        if (ParseKeyword(Keyword.AUTOINCREMENT) && _dialect is MySqlDialect or GenericDialect)
        {
            return new ColumnOption.DialectSpecific(new[] { new Word("AUTOINCREMENT") });
        }

        if (ParseKeywordSequence(Keyword.ON, Keyword.UPDATE) && _dialect is MySqlDialect or GenericDialect)
        {
            return new ColumnOption.OnUpdate(ParseExpr());
        }

        if (ParseKeyword(Keyword.GENERATED))
        {
            return ParseOptionalColumnOptionGenerated();
        }

        return null;
    }

    public ColumnOption? ParseOptionalColumnOptionGenerated()
    {
        if (ParseKeywordSequence(Keyword.ALWAYS, Keyword.AS, Keyword.IDENTITY))
        {
            var sequenceOptions = new Sequence<SequenceOptions>();

            try
            {
                ExpectLeftParen();
                sequenceOptions = ParseCreateSequenceOptions();
                ExpectRightParen();
            }
            catch (ParserException) { }

            return new ColumnOption.Generated(GeneratedAs.Always, sequenceOptions);
        }

        if (ParseKeywordSequence(Keyword.BY, Keyword.DEFAULT, Keyword.AS, Keyword.IDENTITY))
        {
            var sequenceOptions = new Sequence<SequenceOptions>();

            try
            {
                ExpectLeftParen();
                sequenceOptions = ParseCreateSequenceOptions();
                ExpectRightParen();
            }
            catch (ParserException) { }

            return new ColumnOption.Generated(GeneratedAs.ByDefault, sequenceOptions);
        }

        if (ParseKeywordSequence(Keyword.ALWAYS, Keyword.AS))
        {
            try
            {
                ExpectLeftParen();
                var expr = ParseExpr();
                ExpectRightParen();
                ParseKeyword(Keyword.STORED);

                return new ColumnOption.Generated(GeneratedAs.ExpStored, GenerationExpr: expr);
            }
            catch (ParserException)
            {
                return null;
            }
        }

        return null;
    }

    public ReferentialAction ParseReferentialAction()
    {
        if (ParseKeyword(Keyword.RESTRICT))
        {
            return ReferentialAction.Restrict;
        }

        if (ParseKeyword(Keyword.CASCADE))
        {
            return ReferentialAction.Cascade;
        }

        if (ParseKeywordSequence(Keyword.SET, Keyword.NULL))
        {
            return ReferentialAction.SetNull;
        }

        if (ParseKeywordSequence(Keyword.NO, Keyword.ACTION))
        {
            return ReferentialAction.NoAction;
        }

        if (ParseKeywordSequence(Keyword.SET, Keyword.DEFAULT))
        {
            return ReferentialAction.SetDefault;
        }

        throw Expected("Expected one of RESTRICT, CASCADE, SET NULL, NO ACTION or SET DEFAULT", PeekToken());
    }

    public TableConstraint? ParseOptionalTableConstraint()
    {
        var name = ParseInit(ParseKeyword(Keyword.CONSTRAINT), ParseIdentifier);

        var token = NextToken();

        return token switch
        {
            Word { Keyword: Keyword.PRIMARY or Keyword.UNIQUE } w => ParsePrimary(w),
            Word { Keyword: Keyword.FOREIGN } => ParseForeign(),
            Word { Keyword: Keyword.CHECK } => ParseCheck(),
            Word { Keyword: Keyword.INDEX or Keyword.KEY } w when _dialect is GenericDialect or MySqlDialect => ParseIndex(w),
            Word { Keyword: Keyword.FULLTEXT or Keyword.SPATIAL } w when _dialect is GenericDialect or MySqlDialect => ParseText(w),
            _ => ParseDefault()
        };

        TableConstraint ParsePrimary(Word word)
        {
            var isPrimary = word.Keyword == Keyword.PRIMARY;
            ParseKeyword(Keyword.KEY);

            // Optional constraint name
            var identName = MaybeParse(ParseIdentifier) ?? name;

            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            return new TableConstraint.Unique(columns)
            {
                Name = identName,
                IsPrimaryKey = isPrimary,
            };
        }

        TableConstraint ParseForeign()
        {
            ExpectKeyword(Keyword.KEY);
            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            ExpectKeyword(Keyword.REFERENCES);
            var foreignTable = ParseObjectName();
            var referredColumns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            var onDelete = ReferentialAction.None;
            var onUpdate = ReferentialAction.None;

            while (true)
            {
                if (onDelete == ReferentialAction.None && ParseKeywordSequence(Keyword.ON, Keyword.DELETE))
                {
                    onDelete = ParseReferentialAction();
                }
                else if (onUpdate == ReferentialAction.None && ParseKeywordSequence(Keyword.ON, Keyword.UPDATE))
                {
                    onUpdate = ParseReferentialAction();
                }
                else
                {
                    break;
                }
            }

            return new TableConstraint.ForeignKey(foreignTable, columns)
            {
                Name = name,
                ReferredColumns = referredColumns,
                OnDelete = onDelete,
                OnUpdate = onUpdate,
            };
        }

        TableConstraint ParseCheck()
        {
            var expr = ExpectParens(ParseExpr);
            return new TableConstraint.Check(expr, name!);
        }

        TableConstraint ParseIndex(Word word)
        {
            var displayAsKey = word.Keyword == Keyword.KEY;

            var ident = ParseInit(PeekToken() is not Word { Keyword: Keyword.USING }, () => MaybeParse(ParseIdentifier));

            var indexType = ParseInit(ParseKeyword(Keyword.USING), ParseIndexType);

            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);

            return new TableConstraint.Index(columns)
            {
                DisplayAsKey = displayAsKey,
                Name = ident,
                IndexType = indexType
            };
        }

        TableConstraint ParseText(Word word)
        {
            if (name != null)
            {
                ThrowExpected("FULLTEXT or SPATIAL option without constraint name", token);
            }

            var fullText = word.Keyword == Keyword.FULLTEXT;
            var indexTypeDisplay = KeyOrIndexDisplay.None;

            if (ParseKeyword(Keyword.KEY))
            {
                indexTypeDisplay = KeyOrIndexDisplay.Key;
            }
            else if (ParseKeyword(Keyword.INDEX))
            {
                indexTypeDisplay = KeyOrIndexDisplay.Index;
            }

            var optIndexName = MaybeParse(ParseIdentifier);
            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            return new TableConstraint.FulltextOrSpatial(columns)
            {
                FullText = fullText,
                IndexTypeDisplay = indexTypeDisplay,
                OptIndexName = optIndexName
            };
        }

        TableConstraint? ParseDefault()
        {
            if (name != null)
            {
                ThrowExpected("PRIMARY, UNIQUE, FOREIGN, or CHECK", token);
            }

            PrevToken();
            return null;
        }
    }

    public Sequence<SqlOption> ParseOptions(Keyword keyword)
    {
        if (!ParseKeyword(keyword))
        {
            return new Sequence<SqlOption>();
        }

        return ExpectParens(() => ParseCommaSeparated(ParseSqlOption));
    }

    public IndexType ParseIndexType()
    {
        if (ParseKeyword(Keyword.BTREE))
        {
            return IndexType.BTree;
        }

        if (ParseKeyword(Keyword.HASH))
        {
            return IndexType.Hash;
        }

        throw Expected("index type {{BTREE | HASH}}", PeekToken());
    }

    public SqlOption ParseSqlOption()
    {
        var name = ParseIdentifier();
        ExpectToken<Equal>();
        var value = ParseValue();
        return new SqlOption(name, value);
    }

    public Statement ParseAlter()
    {
        var objectType = ExpectOneOfKeywords(Keyword.VIEW, Keyword.TABLE, Keyword.INDEX, Keyword.ROLE);

        switch (objectType)
        {
            case Keyword.VIEW:
                {
                    var name = ParseObjectName();
                    var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);
                    var withOptions = ParseOptions(Keyword.WITH);
                    ExpectKeyword(Keyword.AS);
                    var query = ParseQuery();

                    return new AlterView(name, columns, query, withOptions);
                }

            case Keyword.TABLE:
                {
                    var ifExists = ParseIfExists();
                    var only = ParseKeyword(Keyword.ONLY);
                    var tableName = ParseObjectName();
                    var operations = ParseCommaSeparated(ParseAlterTableOperation);
                    return new AlterTable(tableName, ifExists, only, operations);
                }

            case Keyword.INDEX:
                {
                    var indexName = ParseObjectName();
                    AlterIndexOperation operation;

                    if (ParseKeyword(Keyword.RENAME))
                    {
                        if (ParseKeyword(Keyword.TO))
                        {
                            var name = ParseObjectName();
                            operation = new AlterIndexOperation.RenameIndex(name);
                        }
                        else
                        {
                            throw Expected("after RENAME", PeekToken());
                        }
                    }
                    else
                    {
                        var found = PeekToken();
                        throw Expected($"RENAME after ALTER INDEX, found {found}, {found.Location}");
                    }

                    return new AlterIndex(indexName, operation);
                }

            case Keyword.ROLE:
                return ParseAlterRole();

            default:
                throw new ParserException("ParseAlter");
        }
    }

    //private AlterTableOperation ParseAlterTableOperation()
    //{
    //    AlterTableOperation operation = null!;

    //    if (ParseKeyword(Keyword.ADD))
    //    {
    //        var constraint = ParseOptionalTableConstraint();
    //        if (constraint != null)
    //        {
    //            operation = new AddConstraint(constraint);
    //        }
    //        else
    //        {
    //            var ifNotExists = ParseIfNotExists();
    //            if (ParseKeywordSequence(Keyword.PARTITION))
    //            {
    //                var partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
    //                operation = new AddPartitions(ifNotExists, partitions);
    //            }
    //            else
    //            {
    //                var columnKeyword = ParseKeyword(Keyword.COLUMN);

    //                var ifNotExistsInner = false;
    //                if (_dialect is PostgreSqlDialect or BigQueryDialect or DuckDbDialect or GenericDialect)
    //                {
    //                    ifNotExistsInner = ParseIfNotExists() || ifNotExists;
    //                }

    //                var columnDef = ParseColumnDef();
    //                operation = new AddColumn(columnKeyword, ifNotExistsInner, columnDef);
    //            }
    //        }
    //    }
    //    else if (ParseKeyword(Keyword.RENAME))
    //    {
    //        if (_dialect is PostgreSqlDialect && ParseKeyword(Keyword.CONSTRAINT))
    //        {
    //            var oldName = ParseIdentifier();
    //            ExpectKeyword(Keyword.TO);

    //            var newName = ParseIdentifier();
    //            operation = new RenameConstraint(oldName, newName);
    //        }
    //        else if (ParseKeyword(Keyword.TO))
    //        {
    //            var newName = ParseObjectName();
    //            operation = new RenameTable(newName);
    //        }
    //        else
    //        {
    //            ParseKeyword(Keyword.COLUMN);
    //            var oldColumnName = ParseIdentifier();
    //            ExpectKeyword(Keyword.TO);
    //            var newColumnName = ParseIdentifier();
    //            operation = new RenameColumn(oldColumnName, newColumnName);
    //        }
    //    }
    //    else if (ParseKeyword(Keyword.DROP))
    //    {
    //        if (ParseKeywordSequence(Keyword.IF, Keyword.EXISTS, Keyword.PARTITION))
    //        {
    //            var partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
    //            operation = new DropPartitions(partitions, true);
    //        }
    //        else if (ParseKeyword(Keyword.PARTITION))
    //        {
    //            var partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
    //            operation = new DropPartitions(partitions, false);
    //        }
    //        else if (ParseKeyword(Keyword.CONSTRAINT))
    //        {
    //            var ifExists = ParseIfExists();
    //            var name = ParseIdentifier();
    //            var cascade = ParseKeyword(Keyword.CASCADE);
    //            operation = new DropConstraint(name, ifExists, cascade);
    //        }
    //        else if (ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY) && _dialect is MySqlDialect or GenericDialect)
    //        {
    //            operation = new DropPrimaryKey();
    //        }
    //        else
    //        {
    //            ParseKeyword(Keyword.COLUMN);
    //            var ifExists = ParseIfExists();
    //            var columnName = ParseIdentifier();
    //            var cascade = ParseKeyword(Keyword.CASCADE);
    //            operation = new DropColumn(columnName, ifExists, cascade);
    //        }
    //    }
    //    else if (ParseKeyword(Keyword.PARTITION))
    //    {
    //        var before = ExpectParens(() => ParseCommaSeparated(ParseExpr));
    //        ExpectKeyword(Keyword.RENAME);
    //        ExpectKeywords(Keyword.TO, Keyword.PARTITION);

    //        var renames = ExpectParens(() => ParseCommaSeparated(ParseExpr));
    //        operation = new RenamePartitions(before, renames);
    //    }
    //    else if (ParseKeyword(Keyword.CHANGE))
    //    {
    //        ParseKeyword(Keyword.COLUMN);
    //        var oldName = ParseIdentifier();
    //        var newName = ParseIdentifier();
    //        var dataType = ParseDataType();
    //        var options = new Sequence<ColumnOption>();

    //        while (ParseOptionalColumnOption() is { } option)
    //        {
    //            options.Add(option);
    //        }

    //        operation = new ChangeColumn(oldName, newName, dataType, options);
    //    }
    //    else if (ParseKeyword(Keyword.ALTER))
    //    {
    //        ParseKeyword(Keyword.COLUMN);
    //        var columnName = ParseIdentifier();
    //        var isPostgresql = _dialect is PostgreSqlDialect;
    //        AlterColumnOperation op;

    //        if (ParseKeywordSequence(Keyword.SET, Keyword.NOT, Keyword.NULL))
    //        {
    //            op = new AlterColumnOperation.SetNotNull();
    //        }
    //        else if (ParseKeywordSequence(Keyword.DROP, Keyword.NOT, Keyword.NULL))
    //        {
    //            op = new AlterColumnOperation.DropNotNull();
    //        }
    //        else if (ParseKeywordSequence(Keyword.SET, Keyword.DEFAULT))
    //        {
    //            op = new AlterColumnOperation.SetDefault(ParseExpr());
    //        }
    //        else if (ParseKeywordSequence(Keyword.DROP, Keyword.DEFAULT))
    //        {
    //            op = new AlterColumnOperation.DropDefault();
    //        }
    //        else if (ParseKeywordSequence(Keyword.SET, Keyword.DATA, Keyword.TYPE) || (isPostgresql && ParseKeyword(Keyword.TYPE)))
    //        {
    //            var dataType = ParseDataType();
    //            var @using = ParseInit(isPostgresql && ParseKeyword(Keyword.USING), ParseExpr);
    //            op = new AlterColumnOperation.SetDataType(dataType) { Using = @using };
    //        }
    //        else
    //        {
    //            throw Expected("SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE after ALTER COLUMN", PeekToken());
    //        }

    //        operation = new AlterColumn(columnName, op);
    //    }
    //    else if (ParseKeyword(Keyword.SWAP))
    //    {
    //        ExpectKeyword(Keyword.WITH);
    //        var name = ParseObjectName();
    //        operation = new AlterTableOperation.SwapWith(name);
    //    }
    //    else
    //    {
    //        ThrowExpected("ADD, RENAME, PARTITION, SWAP or DROP after ALTER TABLE", PeekToken());
    //    }

    //    return operation;
    //}

    private AlterTableOperation ParseAlterTableOperation()
    {
        AlterTableOperation operation;

        if (ParseKeyword(Keyword.ADD))
        {
            var constraint = ParseOptionalTableConstraint();
            if (constraint != null)
            {
                operation = new AddConstraint(constraint);
            }
            else
            {
                var ifNotExists = ParseIfNotExists();

                var newPartitions = new Sequence<Partition>();

                while (true)
                {
                    if (ParseKeyword(Keyword.PARTITION))
                    {
                        var partition = ExpectParens(() => ParseCommaSeparated(ParseExpr));
                        newPartitions.Add(new Partition(partition));
                    }
                    else
                    {
                        break;
                    }
                }

                if (newPartitions.Any())
                {
                    return new AddPartitions(ifNotExists, newPartitions);
                }

                var columnKeyword = ParseKeyword(Keyword.COLUMN);

                var ine = false;

                if (_dialect is PostgreSqlDialect or BigQueryDialect or DuckDbDialect or GenericDialect)
                {
                    ine = ParseIfNotExists() || ifNotExists;
                }

                var columnDef = ParseColumnDef();
                return new AddColumn(columnKeyword, ine, columnDef);
            }
        }
        else if (ParseKeyword(Keyword.RENAME))
        {
            if (_dialect is PostgreSqlDialect && ParseKeyword(Keyword.CONSTRAINT))
            {
                var oldName = ParseIdentifier();
                ExpectKeyword(Keyword.TO);
                var newName = ParseIdentifier();
                operation = new RenameConstraint(oldName, newName);
            }
            else if (ParseKeyword(Keyword.TO))
            {
                operation = new RenameTable(ParseObjectName());
            }
            else
            {
                ParseKeyword(Keyword.COLUMN);
                var oldColumnName = ParseIdentifier();
                ExpectKeyword(Keyword.TO);
                var newColumnName = ParseIdentifier();
                operation = new RenameColumn(oldColumnName, newColumnName);
            }
        }
        else if (ParseKeyword(Keyword.DROP))
        {
            if (ParseKeywordSequence(Keyword.IF, Keyword.EXISTS, Keyword.PARTITION))
            {
                var partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
                operation = new DropPartitions(partitions, true);
            }
            else if (ParseKeyword(Keyword.PARTITION))
            {
                var partitions = ExpectParens(() => ParseCommaSeparated(ParseExpr));
                operation = new DropPartitions(partitions, false);
            }
            else if (ParseKeyword(Keyword.CONSTRAINT))
            {
                var ifExists = ParseIfExists();
                var name = ParseIdentifier();
                var cascasde = ParseKeyword(Keyword.CASCADE);
                operation = new DropConstraint(name, ifExists, cascasde);
            }
            else if (ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY) && _dialect is MySqlDialect or GenericDialect)
            {
                operation = new DropPrimaryKey();
            }
            else
            {
                ParseKeyword(Keyword.COLUMN);
                var ifExists = ParseIfExists();
                var columnName = ParseIdentifier();
                var cascade = ParseKeyword(Keyword.CASCADE);
                operation = new DropColumn(columnName, ifExists, cascade);
            }
        }
        else if (ParseKeyword(Keyword.PARTITION))
        {
            var before = ExpectParens(() => ParseCommaSeparated(ParseExpr));
            ExpectKeyword(Keyword.RENAME);
            ExpectKeywords(Keyword.TO, Keyword.PARTITION);
            var renames = ExpectParens(() => ParseCommaSeparated(ParseExpr));
            operation = new RenamePartitions(before, renames);
        }
        else if (ParseKeyword(Keyword.CHANGE))
        {
            ParseKeyword(Keyword.COLUMN);
            var oldName = ParseIdentifier();
            var newName = ParseIdentifier();
            var dataType = ParseDataType();
            var options = new Sequence<ColumnOption>();

            while (ParseOptionalColumnOption() is { } option)
            {
                options.Add(option);
            }

            operation = new ChangeColumn(oldName, newName, dataType, options);
        }
        else if (ParseKeyword(Keyword.ALTER))
        {
            ParseKeyword(Keyword.COLUMN);
            var columnName = ParseIdentifier();
            var isPostgres = _dialect is PostgreSqlDialect;

            AlterColumnOperation? op;

            if (ParseKeywordSequence(Keyword.SET, Keyword.NOT, Keyword.NULL))
            {
                op = new AlterColumnOperation.SetNotNull();
            }
            else if (ParseKeywordSequence(Keyword.DROP, Keyword.NOT, Keyword.NULL))
            {
                op = new AlterColumnOperation.DropNotNull();
            }
            else if (ParseKeywordSequence(Keyword.SET, Keyword.DEFAULT))
            {
                op = new AlterColumnOperation.SetDefault(ParseExpr());
            }
            else if (ParseKeywordSequence(Keyword.DROP, Keyword.DEFAULT))
            {
                op = new AlterColumnOperation.DropDefault();
            }
            else if (
                ParseKeywordSequence(Keyword.SET, Keyword.DATA, Keyword.TYPE) ||
                (isPostgres && ParseKeyword(Keyword.TYPE)))
            {
                var dataType = ParseDataType();
                Expression? @using = null;
                if (isPostgres && ParseKeyword(Keyword.USING))
                {
                    @using = ParseExpr();
                }

                op = new AlterColumnOperation.SetDataType(dataType, @using);
            }
            else
            {
                throw Expected("SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE after ALTER COLUMN", PeekToken());
            }

            operation = new AlterColumn(columnName, op);
        }
        else if (ParseKeyword(Keyword.SWAP))
        {
            ExpectKeyword(Keyword.WITH);
            operation = new SwapWith(ParseObjectName());
        }
        else
        {
            throw Expected("ADD, RENAME, PARTITION, SWAP or DROP after ALTER TABLE", PeekToken());
        }

        return operation;
    }

    public Copy ParseCopy()
    {
        CopySource source;

        if (ConsumeToken<LeftParen>())
        {
            source = new CopySource.CopySourceQuery(ParseQuery());
            ExpectRightParen();
        }
        else
        {
            var tableName = ParseObjectName();
            var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);
            source = new CopySource.Table(tableName, columns);
        }

        var to = ParseOneOfKeywords(Keyword.FROM, Keyword.TO) switch
        {
            Keyword.FROM => false,
            Keyword.TO => true,
            _ => throw Expected("FROM or TO", PeekToken())
        };

        if (!to && source is CopySource.CopySourceQuery)
        {
            throw new ParserException("COPY ... FROM does not support query as a source");
        }

        CopyTarget target =
            ParseKeyword(Keyword.STDIN) ? new CopyTarget.Stdin() :
            ParseKeyword(Keyword.STDOUT) ? new CopyTarget.Stdout() :
            ParseKeyword(Keyword.PROGRAM) ? new CopyTarget.Program(ParseLiteralString()) :
            new CopyTarget.File(ParseLiteralString());

        ParseKeyword(Keyword.WITH);
        Sequence<CopyOption>? options = null;
        if (ConsumeToken<LeftParen>())
        {
            options = ParseCommaSeparated(ParseCopyOption);
            ExpectRightParen();
        }

        var legacyOptions = new Sequence<CopyLegacyOption>();
        while (MaybeParse(ParseCopyLegacyOption) is { } opt)
        {
            legacyOptions.Add(opt);
        }

        Sequence<string?>? values = null;
        if (target is CopyTarget.Stdin)
        {
            ExpectToken<SemiColon>();
            values = ParseTabValue();
        }

        return new Copy(source, to, target)
        {
            Options = options.SafeAny() ? options : null,
            LegacyOptions = legacyOptions.Any() ? legacyOptions : null,
            Values = values
        };
    }

    public Close ParseClose()
    {
        CloseCursor cursor = ParseKeyword(Keyword.ALL) ? new CloseCursor.All() : new CloseCursor.Specific(ParseIdentifier());

        return new Close(cursor);
    }

    public CopyOption ParseCopyOption()
    {
        var keyword = ParseOneOfKeywords(
            Keyword.FORMAT,
            Keyword.FREEZE,
            Keyword.DELIMITER,
            Keyword.NULL,
            Keyword.HEADER,
            Keyword.QUOTE,
            Keyword.ESCAPE,
            Keyword.FORCE_QUOTE,
            Keyword.FORCE_NOT_NULL,
            Keyword.FORCE_NULL,
            Keyword.ENCODING
        );

        return keyword switch
        {
            Keyword.FORMAT => new CopyOption.Format(ParseIdentifier()),
            Keyword.FREEZE => ParseFreeze(),
            Keyword.DELIMITER => new CopyOption.Delimiter(ParseLiteralChar()),
            Keyword.NULL => new CopyOption.Null(ParseLiteralString()),
            Keyword.HEADER => ParseHeader(),
            Keyword.QUOTE => new CopyOption.Quote(ParseLiteralChar()),
            Keyword.ESCAPE => new CopyOption.Escape(ParseLiteralChar()),
            Keyword.FORCE_QUOTE => new CopyOption.ForceQuote(ParseParenthesizedColumnList(IsOptional.Mandatory, false)),
            Keyword.FORCE_NOT_NULL => new CopyOption.ForceNotNull(ParseParenthesizedColumnList(IsOptional.Mandatory, false)),
            Keyword.FORCE_NULL => new CopyOption.ForceNull(ParseParenthesizedColumnList(IsOptional.Mandatory, false)),
            Keyword.ENCODING => new CopyOption.Encoding(ParseLiteralString()),
            _ => throw Expected("option", PeekToken())
        };

        CopyOption.Freeze ParseFreeze()
        {
            return ParseOneOfKeywords(Keyword.TRUE, Keyword.FALSE) == Keyword.FALSE
                ? new CopyOption.Freeze(false)
                : new CopyOption.Freeze(true);
        }

        CopyOption.Header ParseHeader()
        {
            return ParseOneOfKeywords(Keyword.TRUE, Keyword.FALSE) == Keyword.FALSE
                ? new CopyOption.Header(false)
                : new CopyOption.Header(true);
        }
    }

    public CopyLegacyOption ParseCopyLegacyOption()
    {
        var keyword = ParseOneOfKeywords(
            Keyword.BINARY,
            Keyword.DELIMITER,
            Keyword.NULL,
            Keyword.CSV);

        switch (keyword)
        {
            case Keyword.BINARY:
                return new CopyLegacyOption.Binary();

            case Keyword.DELIMITER:
                ParseKeyword(Keyword.AS);
                return new CopyLegacyOption.Delimiter(ParseLiteralChar());
            case Keyword.NULL:
                ParseKeyword(Keyword.AS);
                return new CopyLegacyOption.Null(ParseLiteralString());

            case Keyword.CSV:
                var options = new Sequence<CopyLegacyCsvOption>();
                while (MaybeParse(ParseCopyLegacyCsvOption) is { } option)
                {
                    options.Add(option);
                }

                return new CopyLegacyOption.Csv(options);

            default:
                throw Expected("option", PeekToken());
        }
    }

    public CopyLegacyCsvOption ParseCopyLegacyCsvOption()
    {
        var keyword = ParseOneOfKeywords(
            Keyword.HEADER,
            Keyword.QUOTE,
            Keyword.ESCAPE,
            Keyword.FORCE);

        switch (keyword)
        {
            case Keyword.HEADER:
                return new CopyLegacyCsvOption.Header();

            case Keyword.QUOTE:
                ParseKeyword(Keyword.AS);
                return new CopyLegacyCsvOption.Quote(ParseLiteralChar());

            case Keyword.ESCAPE:
                ParseKeyword(Keyword.AS);
                return new CopyLegacyCsvOption.Escape(ParseLiteralChar());

            case Keyword.FORCE when ParseKeywordSequence(Keyword.NOT, Keyword.NULL):
                return new CopyLegacyCsvOption.ForceNotNull(ParseCommaSeparated(ParseIdentifier));

            case Keyword.FORCE when ParseKeyword(Keyword.QUOTE):
                return new CopyLegacyCsvOption.ForceQuote(ParseCommaSeparated(ParseIdentifier));

            default:
                throw Expected("csv option", PeekToken());

        }
    }

    public char ParseLiteralChar()
    {
        var s = ParseLiteralString();
        if (s.Length != 1)
        {
            throw Expected($"a single character, found {s}");
        }
        return s[0];
    }

    public Sequence<string?> ParseTabValue()
    {
        var values = new Sequence<string?>();
        var content = StringBuilderPool.Get();

        while (NextTokenNoSkip() is { } token)
        {
            switch (token)
            {
                case Whitespace { WhitespaceKind: WhitespaceKind.Tab }:
                    values.Add(content.ToString());
                    content.Clear();
                    break;

                case Whitespace { WhitespaceKind: WhitespaceKind.NewLine }:
                    values.Add(content.ToString());
                    content.Clear();
                    break;

                case Backslash when ConsumeToken<Period>():
                    return values;

                case Backslash:
                    {
                        if (NextToken() is Word { Value: "N" })
                        {
                            values.Add(null);
                        }

                        break;
                    }
                default:
                    content.Append(token);
                    break;
            }
        }

        StringBuilderPool.Return(content);
        return values;
    }

    /// <summary>
    /// Parse a literal value (numbers, strings, date/time, booleans)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Value ParseValue()
    {
        var token = NextToken();

        switch (token)
        {
            case Word w:
                return w.Keyword switch
                {
                    Keyword.TRUE => new Value.Boolean(true),
                    Keyword.FALSE => new Value.Boolean(false),
                    Keyword.NULL => new Value.Null(),
                    Keyword.undefined when w.QuoteStyle == Symbols.DoubleQuote => new Value.DoubleQuotedString(w.Value),
                    Keyword.undefined when w.QuoteStyle == Symbols.SingleQuote => new Value.SingleQuotedString(w.Value),
                    Keyword.undefined when w.QuoteStyle != null => throw Expected("a value", PeekToken()),
                    // Case when Snowflake Semi-structured data like key:value
                    Keyword.undefined or Keyword.LOCATION or Keyword.TYPE when _dialect is SnowflakeDialect or GenericDialect => new Value.UnQuotedString(w.Value),
                    _ => throw Expected("a concrete value", PeekToken())
                };

            case Number n:
                return ParseNumeric(n);

            case SingleQuotedString s:
                return new Value.SingleQuotedString(s.Value);

            case DoubleQuotedString s:
                return new Value.DoubleQuotedString(s.Value);

            case DollarQuotedString s:
                return new Value.DollarQuotedString(new DollarQuotedStringValue(s.Value, s.Tag));

            case SingleQuotedByteStringLiteral s:
                return new Value.SingleQuotedByteStringLiteral(s.Value);

            case DoubleQuotedByteStringLiteral s:
                return new Value.DoubleQuotedByteStringLiteral(s.Value);

            case RawStringLiteral r:
                return new Value.RawStringLiteral(r.Value);

            case NationalStringLiteral n:
                return new Value.NationalStringLiteral(n.Value);

            case EscapedStringLiteral e:
                return new Value.EscapedStringLiteral(e.Value);

            case HexStringLiteral h:
                return new Value.HexStringLiteral(h.Value);

            case Placeholder p:
                return new Value.Placeholder(p.Value);

            case Colon c:
                var colonIdent = ParseIdentifier();
                return new Value.Placeholder(c.Character + colonIdent.Value);

            case AtSign a:
                var atIdent = ParseIdentifier();
                return new Value.Placeholder(a.Character + atIdent.Value);

            default:
                throw Expected("a value", PeekToken());
        }
    }

    public static Value.Number ParseNumeric(Number number)
    {
        var value = number.Value;
        var parsed = double.TryParse(value, CultureInfo.InvariantCulture, out _);

        if (!parsed)
        {
            parsed = Regex.IsMatch(value, "\\d+(\\.\\d+)?e( ([-+])?\\d+)?");
        }

        if (parsed)
        {
            return new Value.Number(value, number.Long);
        }

        throw new ParserException($"Could not parse '{value}'");
    }

    public Value ParseNumberValue()
    {
        return ParseValue() switch
        {
            Value.Number v => v,
            Value.Placeholder p => p,
            _ => Fail()
        };

        Value Fail()
        {
            PrevToken();
            throw Expected("literal number", PeekToken());
        }
    }

    public Value ParseIntroducedStringValue()
    {
        var token = NextToken();

        return token switch
        {
            SingleQuotedString s => new Value.SingleQuotedString(s.Value),
            DoubleQuotedString s => new Value.DoubleQuotedString(s.Value),
            HexStringLiteral s => new Value.HexStringLiteral(s.Value),
            _ => throw Expected(" a string value, found", token)
        };
    }

    /// <summary>
    ///  Parse an unsigned literal integer/long
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public ulong ParseLiteralUnit()
    {
        var token = NextToken();

        if (token is Number n)
        {
            var parsed = ulong.TryParse(n.Value, out var val);

            if (!parsed)
            {
                throw new ParserException($"Count not parse {n.Value} as numeric (u64) value.");
            }

            return val;
        }

        throw Expected("literal int", token);
    }

    public FunctionDefinition ParseFunctionDefinition()
    {
        var token = PeekToken();
        return token switch
        {
            DollarQuotedString s when _dialect is PostgreSqlDialect => DoubleDollar(s.Value),
            _ => new FunctionDefinition.SingleQuotedDef(ParseLiteralString())
        };

        FunctionDefinition DoubleDollar(string value)
        {
            NextToken();
            return new FunctionDefinition.DoubleDollarDef(value);
        }
    }
    /// <summary>
    /// Parse a literal string
    /// </summary>
    /// <returns></returns>
    public string ParseLiteralString()
    {
        var token = NextToken();

        return token switch
        {
            Word { Keyword: Keyword.undefined } word => word.Value,
            SingleQuotedString s => s.Value,
            DoubleQuotedString s => s.Value,
            EscapedStringLiteral s when _dialect is PostgreSqlDialect or GenericDialect => s.Value,
            _ => throw Expected("literal string", token)
        };
    }
    /// <summary>
    /// Parse a map key string
    /// </summary>
    /// <returns></returns>
    public Expression ParseMapKey()
    {
        var token = NextToken();
        return token switch
        {
            // handle BigQuery offset subscript operator which overlaps with OFFSET operator
            Word { Keyword: Keyword.OFFSET } w when _dialect is BigQueryDialect => ParseFunction(new ObjectName(new Ident(w.Value))),
            Word { Keyword: Keyword.undefined } w => PeekToken() is LeftParen
                ? ParseFunction(new ObjectName(new Ident(w.Value)))
                : new LiteralValue(new Value.SingleQuotedString(w.Value)),

            SingleQuotedString s => new LiteralValue(new Value.SingleQuotedString(s.Value)),
            Number n => new LiteralValue(new Value.Number(n.Value)),
            _ => throw Expected("literal string, number, or function", token)
        };
    }
    /// <summary>
    /// Parse a SQL data type (in the context of a CREATE TABLE statement for example)
    /// </summary>
    /// <returns></returns>
    public DataType ParseDataType()
    {
        var token = NextToken();

        var data = token switch
        {
            Word { Keyword: Keyword.BOOLEAN } => new DataType.Boolean(),
            Word { Keyword: Keyword.BOOL } => new DataType.Bool(),
            Word { Keyword: Keyword.FLOAT } => new DataType.Float(ParseOptionalPrecision()),
            Word { Keyword: Keyword.REAL } => new DataType.Real(),
            Word { Keyword: Keyword.FLOAT4 } => new DataType.Float4(),
            Word { Keyword: Keyword.FLOAT8 } => new DataType.Float8(),
            Word { Keyword: Keyword.DOUBLE } => ParseDouble(),
            Word { Keyword: Keyword.TINYINT } => ParseTinyInt(),
            Word { Keyword: Keyword.INT2 } => ParseInt2(),
            Word { Keyword: Keyword.SMALLINT } => ParseSmallInt(),
            Word { Keyword: Keyword.MEDIUMINT } => ParseMediumInt(),
            Word { Keyword: Keyword.INT } => ParseInt(),
            Word { Keyword: Keyword.INT4 } => ParseInt4(),

            Word { Keyword: Keyword.INTEGER } => ParseInteger(),
            Word { Keyword: Keyword.BIGINT } => ParseBigInt(),
            Word { Keyword: Keyword.INT8 } => ParseInt8(),

            Word { Keyword: Keyword.VARCHAR } => new DataType.Varchar(ParseOptionalCharacterLength()),
            Word { Keyword: Keyword.NVARCHAR } => new DataType.Nvarchar(ParseOptionalPrecision()),
            Word { Keyword: Keyword.CHARACTER } => ParseCharacter(),
            Word { Keyword: Keyword.CHAR } => ParseChar(),
            Word { Keyword: Keyword.CLOB } => new DataType.Clob(ParseOptionalPrecision()),
            Word { Keyword: Keyword.BINARY } => new DataType.Binary(ParseOptionalPrecision()),
            Word { Keyword: Keyword.VARBINARY } => new DataType.Varbinary(ParseOptionalPrecision()),
            Word { Keyword: Keyword.BLOB } => new DataType.Blob(ParseOptionalPrecision()),
            Word { Keyword: Keyword.UUID } => new DataType.Uuid(),
            Word { Keyword: Keyword.DATE } => new DataType.Date(),
            Word { Keyword: Keyword.DATETIME } => new DataType.Datetime(ParseOptionalPrecision()),
            Word { Keyword: Keyword.TIMESTAMP } => ParseTimestamp(),
            Word { Keyword: Keyword.TIMESTAMPTZ } => new DataType.Timestamp(TimezoneInfo.Tz, ParseOptionalPrecision()),

            Word { Keyword: Keyword.TIME } => ParseTime(),
            Word { Keyword: Keyword.TIMETZ } => new DataType.Time(TimezoneInfo.Tz, ParseOptionalPrecision()),
            // Interval types can be followed by a complicated interval
            // qualifier that we don't currently support. See
            // parse_interval for a taste.
            Word { Keyword: Keyword.INTERVAL } => new DataType.Interval(),
            Word { Keyword: Keyword.JSON } => new DataType.Json(),
            Word { Keyword: Keyword.REGCLASS } => new DataType.Regclass(),
            Word { Keyword: Keyword.STRING } => new DataType.StringType(),
            Word { Keyword: Keyword.TEXT } => new DataType.Text(),
            Word { Keyword: Keyword.BYTEA } => new DataType.Bytea(),
            Word { Keyword: Keyword.NUMERIC } => new DataType.Numeric(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.DECIMAL } => new DataType.Decimal(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.DEC } => new DataType.Dec(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.BIGNUMERIC } => new DataType.BigNumeric(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.ENUM } => new DataType.Enum(ParseStringValue()),
            Word { Keyword: Keyword.SET } => new DataType.Set(ParseStringValue()),
            Word { Keyword: Keyword.ARRAY } => ParseArray(),
            _ => ParseUnmatched()
        };

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword::ARRAY syntax from above
        while (ConsumeToken<LeftBracket>())
        {
            ExpectToken<RightBracket>();
            data = new DataType.Array(data);
        }

        return data;

        #region Data Type Parsers

        DataType ParseDouble()
        {
            return ParseKeyword(Keyword.PRECISION) ? new DataType.DoublePrecision() : new DataType.Double();
        }

        DataType ParseTinyInt()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedTinyInt(precision) : new DataType.TinyInt(precision);
        }

        DataType ParseInt2()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedInt2(precision) : new DataType.Int2(precision);
        }

        DataType ParseSmallInt()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedSmallInt(precision) : new DataType.SmallInt(precision);
        }

        DataType ParseMediumInt()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedMediumInt(precision) : new DataType.MediumInt(precision);
        }

        DataType ParseInt()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedInt(precision) : new DataType.Int(precision);
        }

        DataType ParseInt4()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedInt4(precision) : new DataType.Int4(precision);
        }

        DataType ParseInt8()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedInt8(precision) : new DataType.Int8(precision);
        }

        DataType ParseInteger()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedInteger(precision) : new DataType.Integer(precision);
        }

        DataType ParseBigInt()
        {
            var precision = ParseOptionalPrecision();
            return ParseKeyword(Keyword.UNSIGNED) ? new DataType.UnsignedBigInt(precision) : new DataType.BigInt(precision);
        }

        DataType ParseCharacter()
        {
            if (ParseKeywordSequence(Keyword.VARYING))
            {
                return new DataType.CharacterVarying(ParseOptionalCharacterLength());
            }

            if (ParseKeywordSequence(Keyword.LARGE, Keyword.OBJECT))
            {
                return new DataType.CharacterLargeObject(ParseOptionalPrecision());
            }

            return new DataType.Character(ParseOptionalCharacterLength());
        }

        DataType ParseChar()
        {
            if (ParseKeywordSequence(Keyword.VARYING))
            {
                return new DataType.CharVarying(ParseOptionalCharacterLength());
            }

            if (ParseKeywordSequence(Keyword.LARGE, Keyword.OBJECT))
            {
                return new DataType.CharLargeObject(ParseOptionalPrecision());
            }

            return new DataType.Char(ParseOptionalCharacterLength());
        }

        DataType ParseTimestamp()
        {
            var precision = ParseOptionalPrecision();
            var tz = TimezoneInfo.None;

            if (ParseKeyword(Keyword.WITH))
            {
                ExpectKeywords(Keyword.TIME, Keyword.ZONE);
                tz = TimezoneInfo.WithTimeZone;
            }
            else if (ParseKeyword(Keyword.WITHOUT))
            {
                ExpectKeywords(Keyword.TIME, Keyword.ZONE);
                tz = TimezoneInfo.WithoutTimeZone;
            }

            return new DataType.Timestamp(tz, precision);
        }

        DataType ParseTime()
        {
            var precision = ParseOptionalPrecision();
            var tz = TimezoneInfo.None;

            if (ParseKeyword(Keyword.WITH))
            {
                ExpectKeywords(Keyword.TIME, Keyword.ZONE);
                tz = TimezoneInfo.WithTimeZone;
            }
            else if (ParseKeyword(Keyword.WITHOUT))
            {
                ExpectKeywords(Keyword.TIME, Keyword.ZONE);
                tz = TimezoneInfo.WithoutTimeZone;
            }

            return new DataType.Time(tz, precision);
        }

        DataType ParseArray()
        {
            if (_dialect is SnowflakeDialect)
            {
                return new DataType.Array(new DataType.None());
            }

            ExpectToken<LessThan>();
            var insideType = ParseDataType();
            ExpectToken<GreaterThan>();
            return new DataType.Array(insideType);
        }

        DataType ParseUnmatched()
        {
            PrevToken();
            var typeName = ParseObjectName();
            var modifiers = ParseOptionalTypeModifiers();
            return modifiers != null
                ? new DataType.Custom(typeName, modifiers)
                : new DataType.Custom(typeName);
        }
        #endregion
    }
    /// <summary>
    /// Parse a string value
    /// </summary>
    /// <returns></returns>
    public Sequence<string> ParseStringValue()
    {
        ExpectLeftParen();
        var values = new Sequence<string>();

        while (true)
        {
            var token = NextToken();
            if (token is SingleQuotedString s)
            {
                values.Add(s.Value);
            }
            else
            {
                throw Expected("a string", token);
            }

            token = NextToken();
            if (token is Comma)
            {
                continue;
            }

            if (token is RightParen)
            {
                break;
            }

            throw Expected(", or }", token);
        }

        return values;
    }
    /// <summary>
    /// Strictly parse identifier AS identifier
    /// </summary>
    /// <returns></returns>
    public IdentWithAlias ParseIdentifierWithAlias()
    {
        var ident = ParseIdentifier();
        ExpectKeyword(Keyword.AS);
        var alias = ParseIdentifier();
        return new IdentWithAlias(ident, alias);
    }
    /// <summary>
    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// </summary>
    /// <param name="reservedKeywords"></param>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Ident? ParseOptionalAlias(IEnumerable<Keyword> reservedKeywords)
    {
        var afterAs = ParseKeyword(Keyword.AS);
        var token = NextToken();

        return token switch
        {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Word word when afterAs || !reservedKeywords.Contains(word.Keyword) => word.ToIdent(),

            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            //
            // Note, that this conflicts with an obscure rule from the SQL
            // standard, which we don't implement:
            //    "[Obscure Rule] SQL allows you to break a long <character
            //    string literal> up into two or more smaller <character string
            //    literal>s, split by a <separator> that includes a newline
            //    character. Length it sees such a <literal>, your DBMS will
            //    ignore the <separator> and treat the multiple strings as
            //    a single <literal>."
            SingleQuotedString s => new Ident(s.Value, Symbols.SingleQuote),
            // Support for MySql dialect double quoted string, `AS "HOUR"` for example
            DoubleQuotedString s => new Ident(s.Value, Symbols.DoubleQuote),
            _ when afterAs => throw Expected("an identifier after AS", token),

            _ => None()
        };

        Ident? None()
        {
            PrevToken();
            return null;
        }
    }
    /// <summary>
    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table it
    /// </summary>
    /// <param name="keywords"></param>
    /// <returns></returns>
    public TableAlias? ParseOptionalTableAlias(IEnumerable<Keyword> keywords)
    {
        var alias = ParseOptionalAlias(keywords);

        if (alias != null)
        {
            var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);

            if (!columns.Any())
            {
                columns = null;
            }

            return new TableAlias(alias, columns);
        }

        return null;
    }
    /// <summary>
    ///  Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `schema."table"
    /// </summary>
    /// <returns></returns>
    public ObjectName ParseObjectName()
    {
        var idents = new Sequence<Ident>();
        while (true)
        {
            idents.Add(ParseIdentifier());
            if (!ConsumeToken<Period>())
            {
                break;
            }
        }

        if (_dialect is BigQueryDialect && idents.Any(i => i.Value.Contains(Symbols.Dot)))
        {
            idents = idents.SelectMany(i => i.Value.Split(Symbols.Dot)
                    .Select(part => i with { Value = part }))
                .ToSequence();
        }

        return new ObjectName(idents);
    }
    /// <summary>
    /// Parse identifiers
    /// </summary>
    public Sequence<Ident> ParseIdentifiers()
    {
        var idents = new Sequence<Ident>();

        while (true)
        {
            var token = PeekToken();
            if (token is Word w)
            {
                idents.Add(w.ToIdent());
            }
            else if (token is EOF or Equal)
            {
                break;
            }

            NextToken();
        }

        return idents;
    }
    /// <summary>
    ///  Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Ident ParseIdentifier()
    {
        var token = NextToken();
        return token switch
        {
            Word word => word.ToIdent(),
            SingleQuotedString s => new Ident(s.Value, Symbols.SingleQuote),
            DoubleQuotedString s => new Ident(s.Value, Symbols.DoubleQuote),
            _ => throw Expected("identifier", token)
        };
    }
    /// <summary>
    ///  Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    /// </summary>
    /// <param name="optional"></param>
    /// <param name="allowEmpty"></param>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Sequence<Ident> ParseParenthesizedColumnList(IsOptional optional, bool allowEmpty)
    {
        if (ConsumeToken<LeftParen>())
        {
            if (allowEmpty && PeekTokenIs<RightParen>())
            {
                NextToken();
                return new Sequence<Ident>();
            }

            var cols = ParseCommaSeparated(ParseIdentifier);
            ExpectRightParen();
            return cols;
        }

        if (optional == IsOptional.Optional)
        {
            return new Sequence<Ident>();
        }

        throw Expected("a list of columns in parenthesis", PeekToken());
    }

    public Statement ParseAlterRole()
    {
        return _dialect switch
        {
            PostgreSqlDialect => ParsePgAlterRole(),
            MsSqlDialect => ParseMsSqlAlterRole(),
            _ => throw new ParserException("ALTER ROLE is only support for PostgreSqlDialect and MsSqlDialect")
        };
    }

    private Statement ParseMsSqlAlterRole()
    {
        var roleName = ParseIdentifier();
        AlterRoleOperation operation = null!;

        if (ParseKeywordSequence(Keyword.ADD, Keyword.MEMBER))
        {
            operation = new AlterRoleOperation.AddMember(ParseIdentifier());
        }
        else if (ParseKeywordSequence(Keyword.DROP, Keyword.MEMBER))
        {
            operation = new AlterRoleOperation.DropMember(ParseIdentifier());
        }
        else if (ParseKeywordSequence(Keyword.WITH, Keyword.NAME))
        {
            if (ConsumeToken<Equal>())
            {
                operation = new AlterRoleOperation.RenameRole(ParseIdentifier());
            }
            else
            {
                throw Expected("= after WITH NAME", PeekToken());
            }
        }

        return new AlterRole(roleName, operation);
    }

    private Statement ParsePgAlterRole()
    {
        var roleName = ParseIdentifier();
        ObjectName? inDatabase = null;
        AlterRoleOperation operation;

        if (ParseKeywordSequence(Keyword.IN, Keyword.DATABASE))
        {
            inDatabase = ParseObjectName();
        }

        if (ParseKeyword(Keyword.RENAME))
        {
            if (ParseKeyword(Keyword.TO))
            {
                operation = new AlterRoleOperation.RenameRole(ParseIdentifier());
            }
            else
            {
                throw Expected("TO after RENAME", PeekToken());
            }
        }
        else if (ParseKeyword(Keyword.SET))
        {
            var configName = ParseObjectName();
            // FROM CURRENT
            if (ParseKeywordSequence(Keyword.FROM, Keyword.CURRENT))
            {
                operation = new AlterRoleOperation.Set(configName, new SetConfigValue.FromCurrent(), inDatabase);
            }
            // { TO | = } { value | DEFAULT }
            else if (ConsumeToken<Equal>() || ParseKeyword(Keyword.TO))
            {
                if (ParseKeyword(Keyword.DEFAULT))
                {
                    operation = new AlterRoleOperation.Set(configName, new SetConfigValue.Default(), inDatabase);
                }
                else
                {
                    var expression = ParseExpr();

                    operation = new AlterRoleOperation.Set(configName, new SetConfigValue.Value(expression), inDatabase);
                }
            }
            else
            {
                throw Expected("'TO' or '=' or 'FROM CURRENT'", PeekToken());
            }
        }
        else if (ParseKeyword(Keyword.RESET))
        {
            operation = ParseKeyword(Keyword.ALL)
                ? new AlterRoleOperation.Reset(new ResetConfig.All(), inDatabase)
                : new AlterRoleOperation.Reset(new ResetConfig.ConfigName(ParseObjectName()), inDatabase);
        }
        else
        {
            _ = ParseKeyword(Keyword.WITH);

            var options = new Sequence<RoleOption>();

            while (MaybeParse(ParsePgRoleOption) is { } parsed)
            {
                options.Add(parsed);
            }

            if (!options.Any())
            {
                throw Expected("option", PeekToken());
            }

            operation = new AlterRoleOperation.WithOptions(options);
        }

        return new AlterRole(roleName, operation);
    }

    private RoleOption ParsePgRoleOption()
    {
        var keywords = new[]
        {
            Keyword.BYPASSRLS,
            Keyword.NOBYPASSRLS,
            Keyword.CONNECTION,
            Keyword.CREATEDB,
            Keyword.NOCREATEDB,
            Keyword.CREATEROLE,
            Keyword.NOCREATEROLE,
            Keyword.INHERIT,
            Keyword.NOINHERIT,
            Keyword.LOGIN,
            Keyword.NOLOGIN,
            Keyword.PASSWORD,
            Keyword.REPLICATION,
            Keyword.NOREPLICATION,
            Keyword.SUPERUSER,
            Keyword.NOSUPERUSER,
            Keyword.VALID
        };
        var option = ParseOneOfKeywords(keywords);

        return option switch
        {
            Keyword.BYPASSRLS => new RoleOption.BypassRls(true),
            Keyword.NOBYPASSRLS => new RoleOption.BypassRls(false),
            Keyword.CONNECTION => ParseAlterConnection(),
            Keyword.CREATEDB => new RoleOption.CreateDb(true),
            Keyword.NOCREATEDB => new RoleOption.CreateDb(false),
            Keyword.CREATEROLE => new RoleOption.CreateRole(true),
            Keyword.NOCREATEROLE => new RoleOption.CreateRole(false),
            Keyword.INHERIT => new RoleOption.Inherit(true),
            Keyword.NOINHERIT => new RoleOption.Inherit(false),
            Keyword.LOGIN => new RoleOption.Login(true),
            Keyword.NOLOGIN => new RoleOption.Login(false),
            Keyword.PASSWORD => new RoleOption.PasswordOption(ParseAlterPassword()),
            Keyword.REPLICATION => new RoleOption.Replication(true),
            Keyword.NOREPLICATION => new RoleOption.Replication(false),
            Keyword.SUPERUSER => new RoleOption.SuperUser(true),
            Keyword.NOSUPERUSER => new RoleOption.SuperUser(false),
            Keyword.VALID => ParseAlterValid(),
            _ => throw Expected("option", PeekToken())
        };

        RoleOption ParseAlterConnection()
        {
            ExpectKeyword(Keyword.LIMIT);
            return new RoleOption.ConnectionLimit(new LiteralValue(ParseNumberValue()));
        }

        Password ParseAlterPassword()
        {
            if (ParseKeyword(Keyword.NULL))
            {
                return new Password.NullPassword();
            }

            return new Password.ValidPassword(new LiteralValue(ParseValue()));
        }

        RoleOption ParseAlterValid()
        {
            ExpectKeyword(Keyword.UNTIL);
            return new RoleOption.ValidUntil(new LiteralValue(ParseValue()));
        }
    }

    public ulong? ParseOptionalPrecision()
    {
        if (!ConsumeToken<LeftParen>())
        {
            return null;
        }

        var precision = ParseLiteralUnit();
        ExpectRightParen();
        return precision;

    }

    public CharacterLength? ParseOptionalCharacterLength()
    {
        if (!ConsumeToken<LeftParen>())
        {
            return null;
        }

        var length = ParseCharacterLength();
        ExpectRightParen();

        return length;
    }

    public CharacterLength ParseCharacterLength()
    {
        var length = ParseLiteralUnit();
        var unit = CharLengthUnit.None;
        if (ParseKeyword(Keyword.CHARACTERS))
        {
            unit = CharLengthUnit.Characters;
        }

        if (ParseKeyword(Keyword.OCTETS))
        {
            unit = CharLengthUnit.Octets;
        }

        return new CharacterLength(length, unit);
    }

    public (ulong?, ulong?) ParseOptionalPrecisionScale()
    {
        if (!ConsumeToken<LeftParen>())
        {
            return (null, null);
        }

        var n = ParseLiteralUnit();
        var scale = ParseInit(ConsumeToken<Comma>(), ParseLiteralUnit);
        ExpectRightParen();
        return (n, scale);

    }

    public ExactNumberInfo ParseExactNumberOptionalPrecisionScale()
    {
        if (ConsumeToken<LeftParen>())
        {
            var precision = ParseLiteralUnit();
            ulong? scale = null;

            if (ConsumeToken<Comma>())
            {
                scale = ParseLiteralUnit();
            }

            ExpectRightParen();

            if (scale != null)
            {
                return new PrecisionAndScale(precision, scale.Value);
            }

            return new Precision(precision);
        }

        return new ExactNumberInfo.None();
    }

    public Sequence<string>? ParseOptionalTypeModifiers()
    {
        if (!ConsumeToken<LeftParen>())
        {
            return null;
        }

        var modifiers = new Sequence<string>();
        var iterate = true;
        while (iterate)
        {
            var token = NextToken();

            switch (token)
            {
                case Word w:
                    modifiers.Add(w.Value);
                    break;

                case Number n:
                    modifiers.Add(n.Value);
                    break;

                case SingleQuotedString s:
                    modifiers.Add(s.Value);
                    break;

                case Comma:
                    continue;

                case RightParen:
                    iterate = false;
                    break;

                default:
                    throw Expected("type modifiers", PeekToken());
            }
        }

#pragma warning disable CS0162
        return modifiers;
#pragma warning restore CS0162

    }

    public Delete ParseDelete()
    {
        Sequence<ObjectName>? tables = null;

        if (!ParseKeyword(Keyword.FROM))
        {
            tables = ParseCommaSeparated(ParseObjectName);
            ExpectKeyword(Keyword.FROM);
        }

        var from = ParseCommaSeparated(ParseTableAndJoins);
        var @using = ParseInit(ParseKeyword(Keyword.USING), ParseTableFactor);

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);
        var returning = ParseInit(ParseKeyword(Keyword.RETURNING), () => ParseCommaSeparated(ParseSelectItem));
        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));
        var limit = ParseInit(ParseKeyword(Keyword.LIMIT), ParseLimit);

        return new Delete(tables, from, orderBy, @using, selection, returning, limit);
    }
    /// <summary>
    /// KILL[CONNECTION | QUERY | MUTATION] processlist_id
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Kill ParseKill()
    {
        var modifier = ParseOneOfKeywords(Keyword.CONNECTION, Keyword.QUERY, Keyword.MUTATION);

        var id = ParseLiteralUnit();

        var modifierKeyword = modifier switch
        {
            Keyword.CONNECTION => KillType.Connection,
            Keyword.QUERY => KillType.Query,
            Keyword.MUTATION => ParseMutation(),
            _ => KillType.None
        };

        KillType ParseMutation()
        {
            if (_dialect is ClickHouseDialect or GenericDialect)
            {
                return KillType.Mutation;
            }

            throw new ParserException("Unsupported type for KILL. Allowed: CONNECTION or QUERY");
        }

        return new Kill(modifierKeyword, id);
    }
    /// <summary>
    /// KILL [CONNECTION | QUERY | MUTATION] processlist_id
    /// </summary>
    /// <param name="describeAlias"></param>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Statement ParseExplain(bool describeAlias)
    {
        var analyze = ParseKeyword(Keyword.ANALYZE);

        var verbose = ParseKeyword(Keyword.VERBOSE);

        var format = ParseInit(ParseKeyword(Keyword.FORMAT), ParseAnalyzeFormat);

        var parsed = MaybeParse(ParseStatement);

        return parsed switch
        {
            Explain or ExplainTable => throw new ParserException("Explain must be root of the plan."),
            not null => new Explain(parsed)
            {
                DescribeAlias = describeAlias,
                Analyze = analyze,
                Verbose = verbose,
                Format = format,
            },
            _ => new ExplainTable(describeAlias, ParseObjectName())
        };

        AnalyzeFormat ParseAnalyzeFormat()
        {
            var token = NextToken();
            if (token is Word word)
            {
                return word.Keyword switch
                {
                    Keyword.TEXT => AnalyzeFormat.Text,
                    Keyword.GRAPHVIZ => AnalyzeFormat.Graphviz,
                    Keyword.JSON => AnalyzeFormat.Json,
                    // ReSharper disable once StringLiteralTypo
                    _ => throw Expected("fileformat", token)
                };
            }

            // ReSharper disable once StringLiteralTypo
            throw Expected("fileformat", token);
        }
    }
    /// <summary>
    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    /// </summary>
    /// <param name="rewind"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Statement.Select ParseQuery(bool rewind = false)
    {
        if (rewind)
        {
            PrevToken();
        }

        using var guard = _depthGuard.Decrement();

        var with = ParseInit(ParseKeyword(Keyword.WITH), () =>
        {
            var recursive = ParseKeyword(Keyword.RECURSIVE);
            var cteTables = ParseCommaSeparated(ParseCommonTableExpression);
            return new With(recursive, cteTables);
        });

        Expression? limit = null;
        Offset? offset = null;
        Ast.Fetch? fetch = null;
        Sequence<LockClause>? locks = null;
        Sequence<Expression>? limitBy = null;

        if (!ParseKeyword(Keyword.INSERT))
        {
            var body = ParseQueryBody(0);

            Sequence<OrderByExpression>? orderBy = null;
            if (ParseKeywordSequence(Keyword.ORDER, Keyword.BY))
            {
                orderBy = ParseCommaSeparated(ParseOrderByExpr);
            }

            for (var i = 0; i < 2; i++)
            {
                if (limit == null && ParseKeyword(Keyword.LIMIT))
                {
                    limit = ParseLimit();
                }

                if (offset == null && ParseKeyword(Keyword.OFFSET))
                {
                    offset = ParseOffset();
                }

                if (_dialect is GenericDialect or MySqlDialect or ClickHouseDialect && limit != null && offset == null && ConsumeToken<Comma>())
                {
                    // MySQL style LIMIT x,y => LIMIT y OFFSET x.
                    // Check <https://dev.mysql.com/doc/refman/8.0/en/select.html> for more details.
                    offset = new Offset(limit, OffsetRows.None);
                    limit = ParseExpr();
                }
            }

            if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.BY))
            {
                limitBy = ParseCommaSeparated(ParseExpr);
            }

            if (ParseKeyword(Keyword.FETCH))
            {
                fetch = ParseFetch();
            }

            while (ParseKeyword(Keyword.FOR))
            {
                locks ??= new Sequence<LockClause>();
                locks.Add(ParseLock());
            }

            return new Statement.Select(new Query(body)
            {
                With = with,
                OrderBy = orderBy,
                Limit = limit,
                Offset = offset,
                Fetch = fetch,
                Locks = locks,
                LimitBy = limitBy
            });
        }

        var insert = ParseInsert();

        return new Statement.Select(new Query(new SetExpression.Insert(insert))
        {
            With = with,
        });

        Offset ParseOffset()
        {
            var value = ParseExpr();
            var rows = OffsetRows.None;

            if (ParseKeyword(Keyword.ROW))
            {
                rows = OffsetRows.Row;
            }
            else if (ParseKeyword(Keyword.ROWS))
            {
                rows = OffsetRows.Rows;
            }

            return new Offset(value, rows);
        }

        LockClause ParseLock()
        {
            var lockType = LockType.None;
            var nonBlock = NonBlock.None;

            if (ParseKeyword(Keyword.UPDATE))
            {
                lockType = LockType.Update;
            }
            else if (ParseKeyword(Keyword.SHARE))
            {
                lockType = LockType.Share;
            }

            var name = ParseInit(ParseKeyword(Keyword.OF), ParseObjectName);

            if (ParseKeyword(Keyword.NOWAIT))
            {
                nonBlock = NonBlock.Nowait;
            }
            else if (ParseKeywordSequence(Keyword.SKIP, Keyword.LOCKED))
            {
                nonBlock = NonBlock.SkipLocked;
            }

            return new LockClause(lockType, nonBlock, name);
        }
    }

    public CommonTableExpression ParseCommonTableExpression()
    {
        var name = ParseIdentifier();

        CommonTableExpression? cte;

        if (ParseKeyword(Keyword.AS))
        {
            var query = ExpectParens(() => ParseQuery());
            var alias = new TableAlias(name);
            cte = new CommonTableExpression(alias, query.Query);
        }
        else
        {
            var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);
            if (!columns.Any())
            {
                columns = null;
            }

            ExpectKeyword(Keyword.AS);
            var query = ExpectParens(() => ParseQuery());

            var alias = new TableAlias(name, columns);
            cte = new CommonTableExpression(alias, query.Query);
        }

        if (ParseKeyword(Keyword.FROM))
        {
            cte.From = ParseIdentifier();
        }

        return cte;
    }

    /// <summary>
    ///  /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// `sql
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// `
    /// </summary>
    /// <param name="precedence"></param>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public SetExpression ParseQueryBody(int precedence)
    {
        // Parse the expression using a Pratt parser, as in `parse_expr()`.
        // With by parsing a restricted SELECT or a `(subquery)`:
        SetExpression? expr;

        if (ParseKeyword(Keyword.SELECT))
        {
            expr = new SetExpression.SelectExpression(ParseSelect());
        }
        else if (ConsumeToken<LeftParen>())
        {
            // CTEs are not allowed here, but the parser currently accepts them
            var subQuery = ParseQuery();
            ExpectRightParen();
            expr = new SetExpression.QueryExpression(subQuery.Query);
        }
        else if (ParseKeyword(Keyword.VALUES))
        {
            expr = new SetExpression.ValuesExpression(ParseValues(_dialect is MySqlDialect));
        }
        else if (ParseKeyword(Keyword.TABLE))
        {
            expr = new SetExpression.TableExpression(ParseAsTable());
        }
        else
        {
            throw Expected("SELECT, VALUES, or a subquery in the query body", PeekToken());
        }

        while (true)
        {
            var @operator = ParseSetOperator();
            var nextPrecedence = @operator switch
            {
                SetOperator.Union or SetOperator.Except => 10,
                SetOperator.Intersect => 20,
                _ => -1
            };

            if (nextPrecedence == -1 || precedence >= nextPrecedence)
            {
                break;
            }

            NextToken();
            var setQualifier = ParseSetQualifier(@operator);
            expr = new SetExpression.SetOperation(expr, @operator, ParseQueryBody(nextPrecedence), setQualifier);
        }

        return expr;

        SetOperator ParseSetOperator()
        {
            return PeekToken() switch
            {
                Word { Keyword: Keyword.UNION } => SetOperator.Union,
                Word { Keyword: Keyword.EXCEPT } => SetOperator.Except,
                Word { Keyword: Keyword.INTERSECT } => SetOperator.Intersect,
                _ => SetOperator.None
                //_ => null
            };
        }

        SetQuantifier ParseSetQualifier(SetOperator op)
        {
            return op switch
            {
                SetOperator.Union when ParseKeywordSequence(Keyword.DISTINCT, Keyword.BY, Keyword.NAME) => SetQuantifier.DistinctByName,
                SetOperator.Union when ParseKeywordSequence(Keyword.BY, Keyword.NAME) => SetQuantifier.ByName,
                SetOperator.Union when ParseKeyword(Keyword.ALL) => ParseKeywordSequence(Keyword.BY, Keyword.NAME)
                    ? SetQuantifier.AllByName
                    : SetQuantifier.All,
                SetOperator.Union when ParseKeyword(Keyword.DISTINCT) => SetQuantifier.Distinct,
                SetOperator.Union => SetQuantifier.None,
                SetOperator.Except or SetOperator.Intersect when ParseKeyword(Keyword.ALL) => SetQuantifier.All,
                SetOperator.Except or SetOperator.Intersect when ParseKeyword(Keyword.DISTINCT) => SetQuantifier.Distinct,
                SetOperator.Except or SetOperator.Intersect => SetQuantifier.None,
                _ => SetQuantifier.None
            };
        }
    }

    /// <summary>
    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Select ParseSelect()
    {
        var distinct = ParseAllOrDistinct();

        var top = ParseInit(ParseKeyword(Keyword.TOP), ParseTop);

        var projection = ParseProjection();

        var into = ParseInit<SelectInto?>(ParseKeyword(Keyword.INTO), () =>
        {
            var temporary = ParseOneOfKeywords(Keyword.TEMP, Keyword.TEMPORARY) != Keyword.undefined;
            var unlogged = ParseKeyword(Keyword.UNLOGGED);
            var table = ParseKeyword(Keyword.TABLE);
            var name = ParseObjectName();
            return new SelectInto(name)
            {
                Temporary = temporary,
                Unlogged = unlogged,
                Table = table,
            };
        });

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        var from = ParseInit(ParseKeyword(Keyword.FROM), () => ParseCommaSeparated(ParseTableAndJoins));

        Sequence<LateralView>? lateralViews = null;
        while (true)
        {
            if (ParseKeywordSequence(Keyword.LATERAL, Keyword.VIEW))
            {
                var outer = ParseKeyword(Keyword.OUTER);
                var lateralView = ParseExpr();
                var lateralViewName = ParseObjectName();
                var lateralColAlias = ParseCommaSeparated(() =>
                    ParseOptionalAlias(new[] { Keyword.WHERE, Keyword.GROUP, Keyword.CLUSTER, Keyword.HAVING, Keyword.LATERAL }));

                lateralViews ??= new Sequence<LateralView>();
                lateralViews.Add(new LateralView(lateralView)
                {
                    LateralViewName = lateralViewName,
                    LateralColAlias = lateralColAlias,
                    Outer = outer
                });
            }
            else
            {
                break;
            }
        }

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);

        GroupByExpression? groupBy = null;

        if (ParseKeywordSequence(Keyword.GROUP, Keyword.BY))
        {
            if (ParseKeyword(Keyword.ALL))
            {
                groupBy = new GroupByExpression.All();
            }
            else
            {
                groupBy = new GroupByExpression.Expressions(ParseCommaSeparated(ParseGroupByExpr));
            }
        }

        var clusterBy = ParseInit(ParseKeywordSequence(Keyword.CLUSTER, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var distributeBy = ParseInit(ParseKeywordSequence(Keyword.DISTRIBUTE, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var sortBy = ParseInit(ParseKeywordSequence(Keyword.SORT, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var having = ParseInit(ParseKeyword(Keyword.HAVING), ParseExpr);

        var namedWindows = ParseInit(ParseKeyword(Keyword.WINDOW), () => ParseCommaSeparated(ParseNamedWindow));

        var qualify = ParseInit(ParseKeyword(Keyword.QUALIFY), ParseExpr);

        return new Select(projection)
        {
            Distinct = distinct,
            Top = top,
            Into = into,
            From = from,
            LateralViews = lateralViews,
            Selection = selection,
            GroupBy = groupBy,
            ClusterBy = clusterBy,
            DistributeBy = distributeBy,
            SortBy = sortBy,
            Having = having,
            NamedWindow = namedWindows,
            QualifyBy = qualify
        };
    }

    public Table ParseAsTable()
    {
        var token1 = NextToken();
        var token2 = NextToken();
        var token3 = NextToken();

        string? tableName;

        if (token2 is Period)
        {
            string? schemaName;
            if (token1 is Word schema)
            {
                schemaName = schema.Value;
            }
            else
            {
                throw Expected("Schema name", token1);
            }

            if (token3 is Word table)
            {
                tableName = table.Value;
            }
            else
            {
                throw Expected("Table name", token3);
            }

            return new Table(tableName, schemaName);
        }
        else
        {
            if (token1 is Word table)
            {
                tableName = table.Value;
            }
            else
            {
                throw Expected("Table name", token3);
            }

            return new Table(tableName);
        }
    }

    public Statement ParseSet()
    {
        var modifier = ParseOneOfKeywords(Keyword.SESSION, Keyword.LOCAL, Keyword.HIVEVAR);
        if (modifier == Keyword.HIVEVAR)
        {
            ExpectToken<Colon>();
        }
        else if (ParseKeyword(Keyword.ROLE))
        {
            var contextModifier = modifier switch
            {
                Keyword.LOCAL => ContextModifier.Local,
                Keyword.SESSION => ContextModifier.Session,
                _ => ContextModifier.None
            };

            var roleName = ParseKeyword(Keyword.NONE) ? null : ParseIdentifier();

            return new SetRole(contextModifier, roleName);
        }

        var variable = ParseKeywordSequence(Keyword.TIME, Keyword.ZONE)
            ? new ObjectName("TIMEZONE")
            : ParseObjectName();

        if (variable.ToString().ToUpperInvariant() == "NAMES" && _dialect is MySqlDialect or GenericDialect)
        {
            if (ParseKeyword(Keyword.DEFAULT))
            {
                return new SetNamesDefault();
            }

            var charsetName = ParseLiteralString();
            var collationName = ParseKeyword(Keyword.COLLATE) ? ParseLiteralString() : null;
            return new SetNames(charsetName, collationName);
        }

        if (ConsumeToken<Equal>() || ParseKeyword(Keyword.TO))
        {
            var values = new Sequence<Expression>();
            while (true)
            {
                try
                {
                    var value = ParseExpr();
                    values.Add(value);
                }
                catch (ParserException)
                {
                    ThrowExpected("variable value", PeekToken());
                }

                if (ConsumeToken<Comma>())
                {
                    continue;
                }

                return new SetVariable(
                    modifier == Keyword.LOCAL,
                    modifier == Keyword.HIVEVAR,
                    variable,
                    values);
            }
        }

        switch (variable.ToString())
        {
            case "TIMEZONE":
                {
                    // for some db (e.g. postgresql), SET TIME ZONE <value> is an alias for SET TIMEZONE [TO|=] <value>
                    var expr = ParseExpr();
                    return new SetTimeZone(modifier == Keyword.LOCAL, expr);
                }
            case "CHARACTERISTICS":
                ExpectKeywords(Keyword.AS, Keyword.TRANSACTION);
                return new SetTransaction(ParseTransactionModes(), Session: true);

            case "TRANSACTION" when modifier == Keyword.undefined:
                {
                    if (!ParseKeyword(Keyword.SNAPSHOT))
                    {
                        return new SetTransaction(ParseTransactionModes());
                    }

                    var snapshotId = ParseValue();
                    return new SetTransaction(null, snapshotId);

                }
            default:
                throw Expected("equal sign or TO", PeekToken());
        }
    }

    public Statement ParseShow()
    {
        var extended = ParseKeyword(Keyword.EXTENDED);
        var full = ParseKeyword(Keyword.FULL);

        if (ParseOneOfKeywords(Keyword.COLUMNS, Keyword.FIELDS) != Keyword.undefined)
        {
            return ParseShowColumns(extended, full);
        }

        if (ParseKeyword(Keyword.TABLES))
        {
            return ParseShowTables(extended, full);
        }
        if (ParseKeyword(Keyword.FUNCTIONS))
        {
            return new ShowFunctions(ParseShowStatementFilter());
        }

        if (extended || full)
        {
            throw new ParserException("EXTENDED/FULL are not supported with this type of SHOW query");
        }

        if (ParseKeyword(Keyword.CREATE))
        {
            return ParseShowCreate();
        }
        if (ParseKeyword(Keyword.COLLATION))
        {
            return ParseShowCollation();
        }

        if (ParseKeyword(Keyword.VARIABLES) && _dialect is MySqlDialect or GenericDialect)
        {
            return new ShowVariables(ParseShowStatementFilter());
        }

        return new ShowVariable(ParseIdentifiers());
    }

    public ShowCreate ParseShowCreate()
    {
        var keyword = ExpectOneOfKeywords(
            Keyword.TABLE,
            Keyword.TRIGGER,
            Keyword.FUNCTION,
            Keyword.PROCEDURE,
            Keyword.EVENT,
            Keyword.VIEW);

        var objectType = keyword switch
        {
            Keyword.TABLE => ShowCreateObject.Table,
            Keyword.TRIGGER => ShowCreateObject.Trigger,
            Keyword.FUNCTION => ShowCreateObject.Function,
            Keyword.PROCEDURE => ShowCreateObject.Procedure,
            Keyword.EVENT => ShowCreateObject.Event,
            Keyword.VIEW => ShowCreateObject.View,
            _ => throw new ParserException("Unable to map keyword to ShowCreateObject")
        };

        return new ShowCreate(objectType, ParseObjectName());
    }

    public ShowTables ParseShowTables(bool extended, bool full)
    {
        var dbName = ParseInit(ParseOneOfKeywords(Keyword.FROM, Keyword.IN) != Keyword.undefined, ParseIdentifier);
        var filter = ParseShowStatementFilter();
        return new ShowTables(extended, full, dbName, filter);
    }

    public ShowColumns ParseShowColumns(bool extended, bool full)
    {
        ExpectOneOfKeywords(Keyword.FROM, Keyword.IN);

        var objectName = ParseObjectName();
        var tableName = objectName;
        if (ParseOneOfKeywords(Keyword.FROM, Keyword.IN) != Keyword.undefined)
        {
            var dbName = new Sequence<Ident> { ParseIdentifier() };

            tableName = new ObjectName(dbName.Concat(tableName.Values));
        }

        var filter = ParseShowStatementFilter();

        return new ShowColumns(extended, full, tableName, filter);
    }

    public Statement ParseShowCollation()
    {
        return new ShowCollation(ParseShowStatementFilter());
    }

    public ShowStatementFilter? ParseShowStatementFilter()
    {
        if (ParseKeyword(Keyword.LIKE))
        {
            return new ShowStatementFilter.Like(ParseLiteralString());
        }

        if (ParseKeyword(Keyword.ILIKE))
        {
            return new ShowStatementFilter.ILike(ParseLiteralString());
        }

        if (ParseKeyword(Keyword.WHERE))
        {
            return new ShowStatementFilter.Where(ParseExpr());
        }

        return null;
    }

    public TableWithJoins ParseTableAndJoins()
    {
        var relation = ParseTableFactor();
        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        Sequence<Join>? joins = null;

        while (true)
        {
            Join join;
            if (ParseKeyword(Keyword.CROSS))
            {
                JoinOperator joinOperator;

                if (ParseKeyword(Keyword.JOIN))
                {
                    joinOperator = new JoinOperator.CrossJoin();
                }
                else if (ParseKeyword(Keyword.APPLY))
                {
                    joinOperator = new JoinOperator.CrossApply();
                }
                else
                {
                    throw Expected("JOIN or APPLY after CROSS", PeekToken());
                }

                join = new Join
                {
                    Relation = ParseTableFactor(),
                    JoinOperator = joinOperator
                };
            }
            else if (ParseKeyword(Keyword.OUTER))
            {
                ExpectKeyword(Keyword.APPLY);
                join = new Join
                {
                    Relation = ParseTableFactor(),
                    JoinOperator = new JoinOperator.OuterApply()
                };
            }
            else
            {
                var natural = ParseKeyword(Keyword.NATURAL);
                var peekKeyword = Keyword.undefined;

                if (PeekToken() is Word word)
                {
                    peekKeyword = word.Keyword;
                }

                Func<JoinConstraint, JoinOperator> joinAction;

                if (peekKeyword is Keyword.INNER or Keyword.JOIN)
                {
                    ParseKeyword(Keyword.INNER); // [ INNER ]
                    ExpectKeyword(Keyword.JOIN);
                    joinAction = constraint => new JoinOperator.Inner(constraint);
                }
                else if (peekKeyword is Keyword.LEFT or Keyword.RIGHT)
                {
                    NextToken(); // consume LEFT/RIGHT
                    var isLeft = peekKeyword == Keyword.LEFT;
                    var joinType = ParseOneOfKeywords(Keyword.OUTER, Keyword.SEMI, Keyword.ANTI, Keyword.JOIN);
                    switch (joinType)
                    {
                        case Keyword.OUTER:
                            ExpectKeyword(Keyword.JOIN);
                            joinAction = isLeft
                                ? constraint => new JoinOperator.LeftOuter(constraint)
                                : constraint => new JoinOperator.RightOuter(constraint);
                            break;

                        case Keyword.SEMI:
                            ExpectKeyword(Keyword.JOIN);
                            joinAction = isLeft
                                ? constraint => new JoinOperator.LeftSemi(constraint)
                                : constraint => new JoinOperator.RightSemi(constraint);
                            break;

                        case Keyword.ANTI:
                            ExpectKeyword(Keyword.JOIN);
                            joinAction = isLeft
                                ? constraint => new JoinOperator.LeftAnti(constraint)
                                : constraint => new JoinOperator.RightAnti(constraint);
                            break;

                        case Keyword.JOIN:
                            joinAction = isLeft
                                ? constraint => new JoinOperator.LeftOuter(constraint)
                                : constraint => new JoinOperator.RightOuter(constraint);
                            break;

                        default:
                            throw Expected($"OUTER, SEMI, ANTI, or JOIN after {peekKeyword}");

                    }
                }
                else if (peekKeyword == Keyword.FULL)
                {
                    NextToken();  // consume FULL
                    ParseKeyword(Keyword.OUTER); // [ OUTER ]
                    ExpectKeyword(Keyword.JOIN);
                    joinAction = constraint => new JoinOperator.FullOuter(constraint);
                }
                else if (peekKeyword == Keyword.OUTER)
                {
                    throw Expected("LEFT, RIGHT, or FULL", PeekToken());
                }
                else if (natural)
                {
                    throw Expected("a join type after NATURAL", PeekToken());
                }
                else
                {
                    break;
                }

                var rel = ParseTableFactor();
                var joniConstraint = ParseJoinConstraint(natural);

                join = new Join
                {
                    Relation = rel,
                    JoinOperator = joinAction(joniConstraint)
                };
            }

            joins ??= new Sequence<Join>();
            joins.Add(join);
        }

        return new TableWithJoins(relation) { Joins = joins };
    }

    /// <summary>
    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    /// <exception cref="NotImplementedException"></exception>
    public TableFactor ParseTableFactor()
    {
        if (ParseKeyword(Keyword.LATERAL))
        {
            if (!ConsumeToken<LeftParen>())
            {
                throw Expected("sub-query after LATERAL", PeekToken());
            }

            return ParseDerivedTableFactor(IsLateral.Lateral);
        }

        if (ParseKeyword(Keyword.TABLE))
        {
            var expr = ExpectParens(ParseExpr);

            var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
            return new TableFactor.TableFunction(expr)
            {
                Alias = alias
            };
        }

        if (ConsumeToken<LeftParen>())
        {
            // A left paren introduces either a derived table (i.e., a sub-query)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SetExpr::Select inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // If the recently consumed '(' starts a derived table, the call to
            // `parse_derived_table_factor` below will return success after parsing the
            // sub-query, followed by the closing ')', and the alias of the derived table.
            // In the example above this is case (3).
            var (parsed, result) = MaybeParseChecked(() => ParseDerivedTableFactor(IsLateral.NotLateral));

            if (parsed)
            {
                return result;
            }

            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            var tableAndJoins = ParseTableAndJoins();

            if (tableAndJoins.Joins.SafeAny())
            {
                // (A)
                ExpectRightParen();
                var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
                return new TableFactor.NestedJoin
                {
                    TableWithJoins = tableAndJoins,
                    Alias = alias
                };
            }

            if (tableAndJoins.Relation is TableFactor.NestedJoin)
            {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                ExpectRightParen();
                var alias = ParseOptionalTableAlias(Keywords.ReservedForColumnAlias);
                return new TableFactor.NestedJoin
                {
                    TableWithJoins = tableAndJoins,
                    Alias = alias
                };
            }

            if (_dialect is SnowflakeDialect or GenericDialect)
            {
                // Dialect-specific behavior: Snowflake diverges from the
                // standard and from most of the other implementations by
                // allowing extra parentheses not only around a join (B), but
                // around lone table names (e.g. `FROM (mytable [AS alias])`)
                // and around derived tables (e.g. `FROM ((SELECT ...)
                // [AS alias])`) as well.
                ExpectRightParen();
                var outerAlias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
                if (outerAlias != null)
                {
                    switch (tableAndJoins.Relation)
                    {
                        case TableFactor.Derived
                            or TableFactor.Table
                            or TableFactor.UnNest
                            or TableFactor.TableFunction
                            or TableFactor.Pivot
                            or TableFactor.Unpivot
                            or TableFactor.NestedJoin:

                            if (tableAndJoins.Relation.Alias != null)
                            {
                                throw new ParserException($"Duplicate alias {tableAndJoins.Relation.Alias.Name}");
                            }
                            // Act as if the alias was specified normally next
                            // to the table name: `(mytable) AS alias` -> `(mytable AS alias)`
                            tableAndJoins.Relation.Alias = outerAlias;
                            break;
                    }
                }

                return tableAndJoins.Relation!;
            }

            throw Expected("join table", PeekToken());
        }

        if (_dialect is BigQueryDialect or PostgreSqlDialect or GenericDialect && ParseKeyword(Keyword.UNNEST))
        {
            //var expr = ExpectParens(ParseExpr);
            var expressions = ExpectParens(() => ParseCommaSeparated(ParseExpr));

            var alias = ParseOptionalTableAlias(Keywords.ReservedForColumnAlias);

            var withOffset = ParseKeywordSequence(Keyword.WITH, Keyword.OFFSET);
            var withOffsetAlias = withOffset ? ParseOptionalAlias(Keywords.ReservedForColumnAlias) : null;
            return new TableFactor.UnNest(expressions)
            {
                Alias = alias,
                WithOffset = withOffset,
                WithOffsetAlias = withOffsetAlias
            };
        }

        var name = ParseObjectName();
        // Parse potential version qualifier
        var version = ParseTableVersion();

        // Postgres, MSSQL: table-valued functions:
        var args = ParseInit(ConsumeToken<LeftParen>(), ParseOptionalArgs);

        var optionalAlias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

        //if (ParseKeyword(Keyword.PIVOT))
        //{
        //    return ParsePivotTableFactor(name, optionalAlias);
        //}

        Sequence<Expression>? withHints = null;
        if (ParseKeyword(Keyword.WITH))
        {
            if (ConsumeToken<LeftParen>())
            {
                withHints = ParseCommaSeparated(ParseExpr);
                ExpectRightParen();
            }
            else
            {
                PrevToken();
            }
        }

        TableFactor table = new TableFactor.Table(name)
        {
            Alias = optionalAlias,
            Args = args,
            WithHints = withHints,
            Version = version
        };

        Keyword kwd;
        while ((kwd = ParseOneOfKeywords(Keyword.PIVOT, Keyword.UNPIVOT)) != Keyword.undefined)
        {
            table = kwd switch
            {
                Keyword.PIVOT => ParsePivotTableFactor(table),
                Keyword.UNPIVOT => ParseUnpivotTableFactor(table),
                _ => throw Expected("PIVOT or UNPIVOT", PeekToken())
            };
        }

        return table;
    }

    public TableVersion? ParseTableVersion()
    {
        if (_dialect is BigQueryDialect or GenericDialect && ParseKeywordSequence(Keyword.FOR, Keyword.SYSTEM_TIME, Keyword.AS, Keyword.OF))
        {
            var expression = ParseExpr();
            return new TableVersion.ForSystemTimeAsOf(expression);
        }

        return null;
    }

    public TableFactor ParseDerivedTableFactor(IsLateral lateral)
    {
        var subQuery = ParseQuery();
        ExpectRightParen();
        var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

        return new TableFactor.Derived(subQuery.Query, lateral == IsLateral.Lateral)
        {
            Alias = alias
        };
    }

    private TableFactor ParsePivotTableFactor(TableFactor table, TableAlias? tableAlias = null)
    {
        var pivot = ExpectParens(() =>
        {
            var token = NextToken();

            var functionName = token switch
            {
                Word w => w.Value,
                _ => throw Expected("an aggregate function name", PeekToken())
            };

            var function = ParseFunction(new ObjectName(functionName));
            ExpectKeyword(Keyword.FOR);

            var valueColumn = ParseObjectName().Values;
            ExpectKeyword(Keyword.IN);

            var pivotValues = ExpectParens(() => ParseCommaSeparated(ParseValue));

            return new TableFactor.Pivot(table, function, valueColumn, pivotValues)
            {
                Alias = tableAlias,
            };
        });

        var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
        pivot.PivotAlias = alias;
        return pivot;
    }

    private TableFactor ParseUnpivotTableFactor(TableFactor table)
    {
        var unpivot = ExpectParens(() =>
        {
            var value = ParseIdentifier();
            ExpectKeyword(Keyword.FOR);

            var name = ParseIdentifier();
            ExpectKeyword(Keyword.IN);

            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);

            return new TableFactor.Unpivot(table, value, name, columns);
        });
        var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

        unpivot.PivotAlias = alias;

        return unpivot;
    }

    public JoinConstraint ParseJoinConstraint(bool natural)
    {
        if (natural)
        {
            return new JoinConstraint.Natural();
        }
        if (ParseKeyword(Keyword.ON))
        {
            var constraint = ParseExpr();
            return new JoinConstraint.On(constraint);
        }

        if (ParseKeyword(Keyword.USING))
        {
            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            return new JoinConstraint.Using(columns);
        }

        return new JoinConstraint.None();
    }

    public Statement ParseGrant()
    {
        var (privileges, grantObjects) = ParseGrantRevokePrivilegesObject();

        ExpectKeyword(Keyword.TO);
        var grantees = ParseCommaSeparated(ParseIdentifier);
        var withGrantOptions = ParseKeywordSequence(Keyword.WITH, Keyword.GRANT, Keyword.OPTION);
        Ident? grantedBy = null;

        if (ParseKeywordSequence(Keyword.GRANTED, Keyword.BY))
        {
            grantedBy = ParseIdentifier();
        }

        return new Grant(privileges, grantObjects, grantees, withGrantOptions, grantedBy);
    }

    public (Privileges, GrantObjects) ParseGrantRevokePrivilegesObject()
    {
        Privileges privileges;
        if (ParseKeyword(Keyword.ALL))
        {
            privileges = new Privileges.All(ParseKeyword(Keyword.PRIVILEGES));
        }
        else
        {
            var permissions = ParseCommaSeparated(ParseGrantPermission);
            var actions = permissions.Select(permission =>
            {
                var (keyword, columns) = permission;

                Ast.Action action = keyword switch
                {
                    Keyword.DELETE => new Ast.Action.Delete(),
                    Keyword.INSERT => new Ast.Action.Insert(columns),
                    Keyword.REFERENCES => new Ast.Action.References(columns),
                    Keyword.SELECT => new Ast.Action.Select(columns),
                    Keyword.TRIGGER => new Ast.Action.Trigger(),
                    Keyword.TRUNCATE => new Ast.Action.Truncate(),
                    Keyword.UPDATE => new Ast.Action.Update(columns),
                    Keyword.USAGE => new Ast.Action.Usage(),
                    Keyword.CONNECT => new Ast.Action.Connect(),
                    Keyword.CREATE => new Ast.Action.Create(),
                    Keyword.EXECUTE => new Ast.Action.Execute(),
                    Keyword.TEMPORARY => new Ast.Action.Temporary(),
                    //// This will cover all future added keywords to
                    //// parse_grant_permission and unhandled in this match
                    _ => throw Expected("grant privilege keyword", PeekToken())
                };
                return action;
            }).ToSequence();

            privileges = new Privileges.Actions(actions);
        }

        ExpectKeyword(Keyword.ON);

        GrantObjects? grantObjects;

        if (ParseKeywordSequence(Keyword.ALL, Keyword.TABLES, Keyword.IN, Keyword.SCHEMA))
        {
            grantObjects = new AllTablesInSchema(ParseCommaSeparated(ParseObjectName));
        }
        else if (ParseKeywordSequence(Keyword.ALL, Keyword.SEQUENCES, Keyword.IN, Keyword.SCHEMA))
        {
            grantObjects = new AllSequencesInSchema(ParseCommaSeparated(ParseObjectName));
        }
        else
        {
            var objectType = ParseOneOfKeywords(Keyword.SEQUENCE, Keyword.SCHEMA, Keyword.TABLE);
            var objects = ParseCommaSeparated(ParseObjectName);
            grantObjects = objectType switch
            {
                Keyword.SCHEMA => new Schema(objects),
                Keyword.SEQUENCE => new Sequences(objects),
                Keyword.TABLE => new Tables(objects),
                _ => new Tables(objects)
            };
        }

        return (privileges, grantObjects);
    }

    public (Keyword, Sequence<Ident>?) ParseGrantPermission()
    {
        var keyword = ParseOneOfKeywords(
            Keyword.CONNECT,
            Keyword.CREATE,
            Keyword.DELETE,
            Keyword.EXECUTE,
            Keyword.INSERT,
            Keyword.REFERENCES,
            Keyword.SELECT,
            Keyword.TEMPORARY,
            Keyword.TRIGGER,
            Keyword.TRUNCATE,
            Keyword.UPDATE,
            Keyword.USAGE);

        Sequence<Ident>? columns = null;
        switch (keyword)
        {
            case Keyword.INSERT or Keyword.REFERENCES or Keyword.SELECT or Keyword.UPDATE:
                {
                    var cols = ParseParenthesizedColumnList(IsOptional.Optional, false);
                    if (cols.Any())
                    {
                        columns = cols;
                    }

                    return (keyword, columns);
                }

            case Keyword.undefined:
                throw Expected("a privilege keyword", PeekToken());

            default:
                return (keyword, null);
        }
    }

    public Revoke ParseRevoke()
    {
        var (privileges, grantObjects) = ParseGrantRevokePrivilegesObject();
        ExpectKeyword(Keyword.FROM);
        var grantees = ParseCommaSeparated(ParseIdentifier);
        var grantedBy = ParseKeywordSequence(Keyword.GRANTED, Keyword.BY) ? ParseIdentifier() : null;
        var cascade = ParseKeyword(Keyword.CASCADE);
        var restrict = ParseKeyword(Keyword.RESTRICT);

        if (cascade && restrict)
        {
            throw new ParserException("Cannot specify both CASCADE and RESTRICT in REVOKE");
        }

        return new Revoke(privileges, grantObjects, grantees, cascade, grantedBy);
    }

    internal Statement ParseInsert()
    {
        var orConflict = _dialect is not SQLiteDialect ? SqliteOnConflict.None :
            ParseKeywordSequence(Keyword.OR, Keyword.REPLACE) ? SqliteOnConflict.Replace :
            ParseKeywordSequence(Keyword.OR, Keyword.ROLLBACK) ? SqliteOnConflict.Rollback :
            ParseKeywordSequence(Keyword.OR, Keyword.ABORT) ? SqliteOnConflict.Abort :
            ParseKeywordSequence(Keyword.OR, Keyword.FAIL) ? SqliteOnConflict.Fail :
            ParseKeywordSequence(Keyword.OR, Keyword.IGNORE) ? SqliteOnConflict.Ignore :
            ParseKeywordSequence(Keyword.REPLACE) ? SqliteOnConflict.Replace :
            SqliteOnConflict.None;

        var ignore = _dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.IGNORE);
        var action = ParseOneOfKeywords(Keyword.INTO, Keyword.OVERWRITE);
        var into = action == Keyword.INTO;
        var overwrite = action == Keyword.OVERWRITE;
        var local = ParseKeyword(Keyword.LOCAL);

        if (ParseKeyword(Keyword.DIRECTORY))
        {
            var path = ParseLiteralString();
            var fileFormat = ParseInit(ParseKeywordSequence(Keyword.STORED, Keyword.AS), ParseFileFormat);
            var query = ParseQuery();
            return new Statement.Directory(overwrite, local, path, fileFormat, query);
        }

        var table = ParseKeyword(Keyword.TABLE);
        var tableName = ParseObjectName();
        var isMySql = _dialect is MySqlDialect;
        var columns = ParseParenthesizedColumnList(IsOptional.Optional, isMySql);

        var partitioned = ParseInit(ParseKeyword(Keyword.PARTITION), () =>
        {
            return ExpectParens(() => ParseCommaSeparated(ParseExpr));
        });

        var afterColumns = ParseParenthesizedColumnList(IsOptional.Optional, false);
        var source = ParseQuery();
        var on = ParseOn();

        var returning = ParseInit(ParseKeyword(Keyword.RETURNING), () => ParseCommaSeparated(ParseSelectItem));

        OnInsert? ParseOn()
        {
            if (ParseKeyword(Keyword.ON))
            {
                if (ParseKeyword(Keyword.CONFLICT))
                {
                    ConflictTarget? conflictTarget = null;
                    if (ParseKeywordSequence(Keyword.ON, Keyword.CONSTRAINT))
                    {
                        conflictTarget = new ConflictTarget.OnConstraint(ParseObjectName());
                    }
                    else if (PeekToken() is LeftParen)
                    {
                        conflictTarget = new ConflictTarget.Column(ParseParenthesizedColumnList(IsOptional.Optional, false));
                    }

                    ExpectKeyword(Keyword.DO);
                    OnConflictAction? conflictAction;
                    if (ParseKeyword(Keyword.NOTHING))
                    {
                        conflictAction = new OnConflictAction.DoNothing();
                    }
                    else
                    {
                        ExpectKeyword(Keyword.UPDATE);
                        ExpectKeyword(Keyword.SET);
                        var assignments = ParseCommaSeparated(ParseAssignment);
                        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);
                        conflictAction = new OnConflictAction.DoUpdate(new DoUpdateAction(assignments, selection));
                    }

                    return new OnInsert.Conflict(new OnConflict(conflictAction, conflictTarget));
                }

                ExpectKeyword(Keyword.DUPLICATE);
                ExpectKeyword(Keyword.KEY);
                ExpectKeyword(Keyword.UPDATE);
                return new OnInsert.DuplicateKeyUpdate(ParseCommaSeparated(ParseAssignment));
            }

            return null;
        }

        return new Insert(tableName, source)
        {
            Or = orConflict,
            Ignore = ignore,
            Into = into,
            Overwrite = overwrite,
            Partitioned = partitioned,
            Columns = columns,
            AfterColumns = afterColumns,
            Table = table,
            On = on,
            Returning = returning
        };
    }

    public Statement ParseUpdate()
    {
        var table = ParseTableAndJoins();
        ExpectKeyword(Keyword.SET);
        var assignments = ParseCommaSeparated(ParseAssignment);
        TableWithJoins? from = null;

        if (ParseKeyword(Keyword.FROM) && _dialect
                is GenericDialect
                or PostgreSqlDialect
                or DuckDbDialect
                or BigQueryDialect
                or SnowflakeDialect
                or RedshiftDialect
                or MsSqlDialect)
        {
            from = ParseTableAndJoins();
        }

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);
        var returning = ParseInit(ParseKeyword(Keyword.RETURNING), () => ParseCommaSeparated(ParseSelectItem));

        return new Update(table, assignments, from, selection, returning);
    }

    /// <summary>
    ///  Parse a `var = expr` assignment, used in an UPDATE statement
    /// </summary>
    /// <returns></returns>
    public Assignment ParseAssignment()
    {
        var idents = ParseIdentifiers();
        ExpectToken<Equal>();
        var expr = ParseExpr();
        return new Assignment(idents, expr);
    }

    public FunctionArg ParseFunctionArgs()
    {
        if (!PeekNthTokenIs<RightArrow>(1))
        {
            return new FunctionArg.Unnamed(ParseWildcardExpr());
        }

        var name = ParseIdentifier();
        ExpectToken<RightArrow>();
        var arg = (FunctionArgExpression)ParseWildcardExpr();

        return new FunctionArg.Named(name, arg);
    }

    public Sequence<FunctionArg> ParseOptionalArgs()
    {
        if (ConsumeToken<RightParen>())
        {
            return new Sequence<FunctionArg>();
        }

        var args = ParseCommaSeparated(ParseFunctionArgs);
        ExpectRightParen();
        return args;
    }

    public (Sequence<FunctionArg> Args, Sequence<OrderByExpression>? OrderBy) ParseOptionalArgsWithOrderBy()
    {
        Sequence<OrderByExpression>? orderBy = null;

        if (ConsumeToken<RightParen>())
        {
            return (new Sequence<FunctionArg>(), orderBy);
        }

        // Snowflake permits a subquery to be passed as an argument without
        // an enclosing set of parens if it's the only argument.
        if (_dialect is SnowflakeDialect && ParseOneOfKeywords(Keyword.WITH, Keyword.SELECT) != Keyword.undefined)
        {
            PrevToken();
            var subquery = ParseQuery();
            ExpectToken<RightParen>();

            var subqueryArgs = new Sequence<FunctionArg>
            {
                new FunctionArg.Unnamed(new FunctionArgExpression.FunctionExpression(new Subquery(subquery)))
            };
            return (subqueryArgs, null);
        }

        var args = ParseCommaSeparated(ParseFunctionArgs);

        if (ParseKeywordSequence(Keyword.ORDER, Keyword.BY))
        {
            orderBy = ParseCommaSeparated(ParseOrderByExpr);
        }

        ExpectRightParen();

        return (args, orderBy);
    }

    /// <summary>
    /// Parse a comma-delimited list of projections after SELECT
    /// </summary>
    /// <returns></returns>
    public SelectItem ParseSelectItem()
    {
        var wildcardExpr = ParseWildcardExpr();

        Expression GetExpression(Expression expr)
        {
            if (_dialect.SupportsFilterDuringAggregation() && ParseKeyword(Keyword.FILTER))
            {
                var i = _index - 1;
                if (ConsumeToken<LeftParen>() && ParseKeyword(Keyword.WHERE))
                {
                    var filter = ParseExpr();
                    ExpectRightParen();
                    return new AggregateExpressionWithFilter(expr, filter);
                }

                _index = i;
                return expr;
            }

            return expr;

        }

        if (wildcardExpr is WildcardExpression.Expr e)
        {
            var expr = GetExpression(e.Expression);

            var alias = ParseOptionalAlias(Keywords.ReservedForColumnAlias);

            if (alias != null)
            {
                return new SelectItem.ExpressionWithAlias(expr, alias);
            }

            return new SelectItem.UnnamedExpression(expr);
        }

        if (wildcardExpr is WildcardExpression.QualifiedWildcard qw)
        {
            return new SelectItem.QualifiedWildcard(qw.Name, ParseWildcardAdditionalOptions());
        }

        return new SelectItem.Wildcard(ParseWildcardAdditionalOptions());
    }
    /// <summary>
    /// Parse an [`WildcardAdditionalOptions`](WildcardAdditionalOptions) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    /// </summary>
    /// <returns></returns>
    public WildcardAdditionalOptions ParseWildcardAdditionalOptions()
    {
        ExcludeSelectItem? optExclude = null;
        if (_dialect is GenericDialect or DuckDbDialect or SnowflakeDialect)
        {
            optExclude = ParseOptionalSelectItemExclude();
        }

        ExceptSelectItem? optExcept = null;
        if (_dialect is GenericDialect or BigQueryDialect or ClickHouseDialect)
        {
            optExcept = ParseOptionalSelectItemExcept();
        }

        RenameSelectItem? optRename = null;
        if (_dialect is GenericDialect or SnowflakeDialect)
        {
            optRename = ParseOptionalSelectItemRename();
        }

        ReplaceSelectItem? optReplace = null;
        if (_dialect is GenericDialect or BigQueryDialect or ClickHouseDialect)
        {
            optReplace = ParseOptionalSelectItemReplace();
        }

        return new WildcardAdditionalOptions
        {
            ExcludeOption = optExclude,
            ExceptOption = optExcept,
            RenameOption = optRename,
            ReplaceOption = optReplace
        };
    }
    /// <summary>
    /// Parse an [`Exclude`](ExcludeSelectItem) information for wildcard select items.
    /// </summary>
    /// <returns></returns>
    public ExcludeSelectItem? ParseOptionalSelectItemExclude()
    {
        ExcludeSelectItem? optExclude = null;

        if (!ParseKeyword(Keyword.EXCLUDE))
        {
            return optExclude;
        }

        if (ConsumeToken<LeftParen>())
        {
            var columns = ParseCommaSeparated(ParseIdentifier);
            ExpectRightParen();

            optExclude = new ExcludeSelectItem.Multiple(columns);
        }
        else
        {
            optExclude = new ExcludeSelectItem.Single(ParseIdentifier());
        }

        return optExclude;
    }
    /// <summary>
    /// Parse an [`Except`](ExceptSelectItem) information for wildcard select items.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public ExceptSelectItem? ParseOptionalSelectItemExcept()
    {
        if (!ParseKeyword(Keyword.EXCEPT))
        {
            return null;
        }

        ExceptSelectItem optionalExcept;

        if (PeekToken() is LeftParen)
        {
            var idents = ParseParenthesizedColumnList(IsOptional.Mandatory, false);

            if (!idents.Any())
            {
                throw Expected("at least one column should be parsed by the expect clause", PeekToken());
            }

            optionalExcept = new ExceptSelectItem(idents.First(), idents.Skip(1).ToSequence());
        }
        else
        {
            var ident = ParseIdentifier();

            optionalExcept = new ExceptSelectItem(ident, new Sequence<Ident>());
        }

        return optionalExcept;
    }

    public RenameSelectItem? ParseOptionalSelectItemRename()
    {
        RenameSelectItem? optRename = null;

        if (!ParseKeyword(Keyword.RENAME))
        {
            return optRename;
        }

        if (ConsumeToken<LeftParen>())
        {
            var idents = ParseCommaSeparated(ParseIdentifierWithAlias);
            ExpectRightParen();

            optRename = new RenameSelectItem.Multiple(idents);
        }
        else
        {
            optRename = new RenameSelectItem.Single(ParseIdentifierWithAlias());
        }

        return optRename;
    }

    public ReplaceSelectItem? ParseOptionalSelectItemReplace()
    {
        return ParseInit(ParseKeyword(Keyword.REPLACE), () =>
        {
            if (ConsumeToken<LeftParen>())
            {
                var items = ParseCommaSeparated(ParseReplaceElements);
                ExpectRightParen();
                return new ReplaceSelectItem(items);
            }

            var token = NextToken();
            throw Expected("( after REPLACE", token);
        });
    }

    public ReplaceSelectElement ParseReplaceElements()
    {
        var expr = ParseExpr();
        var asKeyword = ParseKeyword(Keyword.AS);
        var ident = ParseIdentifier();

        return new ReplaceSelectElement(expr, ident, asKeyword);
    }
    /// <summary>
    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    /// </summary>
    /// <returns>Order By Expression</returns>
    public OrderByExpression ParseOrderByExpr()
    {
        var expr = ParseExpr();

        bool? asc = null;

        if (ParseKeyword(Keyword.ASC))
        {
            asc = true;
        }
        else if (ParseKeyword(Keyword.DESC))
        {
            asc = false;
        }

        bool? nullsFirst = null;
        if (ParseKeywordSequence(Keyword.NULLS, Keyword.FIRST))
        {
            nullsFirst = true;
        }
        else if (ParseKeywordSequence(Keyword.NULLS, Keyword.LAST))
        {
            nullsFirst = false;
        }

        return new OrderByExpression(expr, asc, nullsFirst);
    }
    /// <summary>
    /// Parse a TOP clause, MSSQL equivalent of LIMIT,
    /// that follows after `SELECT [DISTINCT]`.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Top ParseTop()
    {
        Expression? quantity;
        if (ConsumeToken<LeftParen>())
        {
            var quantityExp = ParseExpr();
            ExpectRightParen();
            quantity = quantityExp;
        }
        else
        {
            quantity = new LiteralValue(ParseNumberValue());
        }

        var percent = ParseKeyword(Keyword.PERCENT);
        var withTies = ParseKeywordSequence(Keyword.WITH, Keyword.TIES);

        return new Top(quantity, withTies, percent);
    }

    public Expression? ParseLimit()
    {
        return ParseKeyword(Keyword.ALL) ? null : ParseExpr();
    }

    public Ast.Fetch ParseFetch()
    {
        ExpectOneOfKeywords(Keyword.FIRST, Keyword.NEXT);

        var percent = false;
        var quantity = ParseInit<Expression>(ParseOneOfKeywords(Keyword.ROW, Keyword.ROWS) == Keyword.undefined,
            () =>
            {
                var value = new LiteralValue(ParseValue());
                percent = ParseKeyword(Keyword.PERCENT);
                ExpectOneOfKeywords(Keyword.ROW, Keyword.ROWS);

                return value;
            });

        bool withTies;

        if (ParseKeyword(Keyword.ONLY))
        {
            withTies = false;
        }
        else if (ParseKeywordSequence(Keyword.WITH, Keyword.TIES))
        {
            withTies = true;
        }
        else
        {
            throw Expected("one of ONLY or WITH TIES", PeekToken());
        }

        return new Ast.Fetch(quantity, withTies, percent);
    }

    public Values ParseValues(bool allowEmpty)
    {
        var explicitRows = false;

        var rows = ParseCommaSeparated(() =>
        {
            if (ParseKeyword(Keyword.ROW))
            {
                explicitRows = true;
            }

            ExpectLeftParen();

            if (allowEmpty && PeekToken() is RightParen)
            {
                NextToken();
                return new Sequence<Expression>();
            }

            var expressions = ParseCommaSeparated(ParseExpr);
            ExpectRightParen();
            return expressions;
        });

        return new Values(rows, explicitRows);
    }

    public StartTransaction ParseStartTransaction()
    {
        ExpectKeyword(Keyword.TRANSACTION);
        return new StartTransaction(ParseTransactionModes(), false);
    }

    public Statement ParseBegin()
    {
        _ = ParseOneOfKeywords(Keyword.TRANSACTION, Keyword.WORK);
        return new StartTransaction(ParseTransactionModes(), true);
    }

    public Sequence<TransactionMode>? ParseTransactionModes()
    {
        var modes = new Sequence<TransactionMode>();
        var required = false;

        while (true)
        {
            TransactionMode? mode;
            if (ParseKeywordSequence(Keyword.ISOLATION, Keyword.LEVEL))
            {
                var isoLevel =
                    ParseKeywordSequence(Keyword.READ, Keyword.UNCOMMITTED) ? TransactionIsolationLevel.ReadUncommitted :
                    ParseKeywordSequence(Keyword.READ, Keyword.COMMITTED) ? TransactionIsolationLevel.ReadCommitted :
                    ParseKeywordSequence(Keyword.REPEATABLE, Keyword.READ) ? TransactionIsolationLevel.RepeatableRead :
                    ParseKeyword(Keyword.SERIALIZABLE) ? TransactionIsolationLevel.Serializable :
                    throw Expected("isolation level", PeekToken());

                mode = new TransactionMode.IsolationLevel(isoLevel);
            }
            else if (ParseKeywordSequence(Keyword.READ, Keyword.ONLY))
            {
                mode = new TransactionMode.AccessMode(TransactionAccessMode.ReadOnly);
            }
            else if (ParseKeywordSequence(Keyword.READ, Keyword.WRITE))
            {
                mode = new TransactionMode.AccessMode(TransactionAccessMode.ReadWrite);
            }
            else if (required)
            {
                throw Expected("transaction mode", PeekToken());
            }
            else
            {
                break;
            }

            modes.Add(mode);
            required = ConsumeToken<Comma>();
        }

        return modes.Any() ? modes : null;
    }

    public Deallocate ParseDeallocate()
    {
        var keyword = ParseKeyword(Keyword.PREPARE);

        return new Deallocate(ParseIdentifier(), keyword);
    }

    public bool ParseCommitRollbackChain()
    {
        _ = ParseOneOfKeywords(Keyword.TRANSACTION, Keyword.WORK);
        if (ParseKeyword(Keyword.AND))
        {
            var chain = !ParseKeyword(Keyword.NO);
            ExpectKeyword(Keyword.CHAIN);
            return chain;
        }

        return false;
    }

    public Execute ParseExecute()
    {
        var name = ParseIdentifier();
        Sequence<Expression>? parameters = null;
        if (ConsumeToken<LeftParen>())
        {
            parameters = ParseCommaSeparated(ParseExpr);
            ExpectRightParen();
        }

        return new Execute(name, parameters);
    }

    public Prepare ParsePrepare()
    {
        var name = ParseIdentifier();
        var dataTypes = new Sequence<DataType>();
        if (ConsumeToken<LeftParen>())
        {
            dataTypes = ParseCommaSeparated(ParseDataType);
            ExpectRightParen();
        }

        ExpectKeyword(Keyword.AS);
        var statement = ParseStatement();
        return new Prepare(name, dataTypes, statement);
    }

    public Sequence<MergeClause> ParseMergeClauses()
    {
        var clauses = new Sequence<MergeClause>();

        while (true)
        {
            if (PeekToken() is EOF or SemiColon)
            {
                break;
            }

            ExpectKeyword(Keyword.WHEN);
            var isNotMatched = ParseKeyword(Keyword.NOT);
            ExpectKeyword(Keyword.MATCHED);

            var predicate = ParseInit(ParseKeyword(Keyword.AND), ParseExpr);
            ExpectKeyword(Keyword.THEN);

            var keyword = ParseOneOfKeywords(Keyword.UPDATE, Keyword.INSERT, Keyword.DELETE);

            switch (keyword)
            {
                case Keyword.UPDATE:
                    if (isNotMatched)
                    {
                        throw new ParserException("UPDATE in NOT MATCHED merge clause");
                    }
                    ExpectKeyword(Keyword.SET);
                    var assignments = ParseCommaSeparated(ParseAssignment);
                    clauses.Add(new MergeClause.MatchedUpdate(assignments, predicate));
                    break;

                case Keyword.DELETE:
                    if (isNotMatched)
                    {
                        throw new ParserException("DELETE in NOT MATCHED merge clause");
                    }

                    clauses.Add(new MergeClause.MatchedDelete(predicate));
                    break;

                case Keyword.INSERT:
                    if (!isNotMatched)
                    {
                        throw new ParserException("INSERT in MATCHED merge clause");
                    }

                    var isMySql = _dialect is MySqlDialect;
                    var columns = ParseParenthesizedColumnList(IsOptional.Optional, isMySql);
                    ExpectKeyword(Keyword.VALUES);
                    var values = ParseValues(isMySql);
                    clauses.Add(new MergeClause.NotMatched(columns, values, predicate));
                    break;

                default:
                    throw Expected("UPDATE, DELETE or INSERT in merge clause");
            }
        }

        return clauses;
    }

    public Merge ParseMerge()
    {
        var into = ParseKeyword(Keyword.INTO);
        var table = ParseTableFactor();
        ExpectKeyword(Keyword.USING);
        var source = ParseTableFactor();
        ExpectKeyword(Keyword.ON);
        var on = ParseExpr();
        var clauses = ParseMergeClauses();
        return new Merge(into, table, source, on, clauses);
    }

    public Statement ParsePragma()
    {
        var name = ParseObjectName();
        if (ConsumeToken<LeftParen>())
        {
            var value = ParseNumberValue();
            ExpectRightParen();
            return new Pragma(name, value, false);
        }

        if (ConsumeToken<Equal>())
        {
            return new Pragma(name, ParseNumberValue(), true);
        }

        return new Pragma(name, null, false);
    }
    /// <summary>
    /// CREATE [ { TEMPORARY | TEMP } ] SEQUENCE [ IF NOT EXISTS ] sequence_name
    ///
    /// <see href="https://www.postgresql.org/docs/current/sql-createsequence.html"/>
    /// </summary>
    /// <param name="temporary"></param>
    /// <returns></returns>
    public CreateSequence ParseCrateSequence(bool temporary)
    {
        //[ IF NOT EXISTS ]
        var ifNotExists = ParseIfNotExists();
        var name = ParseObjectName();
        //[ AS data_type ]
        var dataType = ParseInit(ParseKeyword(Keyword.AS), ParseDataType);
        var sequenceOptions = ParseCreateSequenceOptions();
        var ownedBy = ParseInit(ParseKeywordSequence(Keyword.OWNED, Keyword.BY), () => ParseKeyword(Keyword.NONE)
            ? new ObjectName(new Ident("NONE"))
            : ParseObjectName());

        return new CreateSequence(name)
        {
            Temporary = temporary,
            IfNotExists = ifNotExists,
            DataType = dataType,
            SequenceOptions = sequenceOptions,
            OwnedBy = ownedBy
        };
    }

    public Sequence<SequenceOptions> ParseCreateSequenceOptions()
    {
        var sequenceOptions = new Sequence<SequenceOptions>();

        //[ INCREMENT [ BY ] increment ]
        if (ParseKeyword(Keyword.INCREMENT))
        {
            var by = ParseKeyword(Keyword.BY);
            sequenceOptions.Add(new SequenceOptions.IncrementBy(new LiteralValue(ParseNumberValue()), by));
        }

        //[ MINVALUE minvalue | NO MINVALUE ]
        if (ParseKeyword(Keyword.MINVALUE))
        {
            var expr = new LiteralValue(ParseNumberValue());
            sequenceOptions.Add(new SequenceOptions.MinValue(new Some(expr)));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MINVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MinValue(new MinMaxValue.None()));
        }
        else
        {
            sequenceOptions.Add(new SequenceOptions.MinValue(new Empty()));
        }

        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if (ParseKeywordSequence(Keyword.MAXVALUE))
        {
            var expr = new LiteralValue(ParseNumberValue());
            sequenceOptions.Add(new SequenceOptions.MaxValue(new Some(expr)));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MAXVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MaxValue(new MinMaxValue.None()));
        }
        else
        {
            sequenceOptions.Add(new SequenceOptions.MaxValue(new Empty()));
        }

        //[ START [ WITH ] start ]
        if (ParseKeywordSequence(Keyword.START))
        {
            var with = ParseKeyword(Keyword.WITH);
            var expr = new LiteralValue(ParseNumberValue());
            sequenceOptions.Add(new SequenceOptions.StartWith(expr, with));
        }

        //[ CACHE cache ]
        if (ParseKeyword(Keyword.CACHE))
        {
            sequenceOptions.Add(new SequenceOptions.Cache(new LiteralValue(ParseNumberValue())));
        }

        // [ [ NO ] CYCLE ]
        if (ParseKeyword(Keyword.NO))
        {
            if (ParseKeyword(Keyword.CYCLE))
            {
                sequenceOptions.Add(new SequenceOptions.Cycle(true));
            }
        }
        else if (ParseKeyword(Keyword.CYCLE))
        {
            sequenceOptions.Add(new SequenceOptions.Cycle(false));
        }

        return sequenceOptions;
    }

    public static void ThrowExpectedToken(Token expected, Token actual)
    {
        ThrowExpected($"{expected}", actual);
    }

    public static void ThrowExpected(string expected, Token actual)
    {
        throw Expected($"{expected}", actual);
    }

    public static ParserException Expected(string message, Token actual)
    {
        return new ParserException($"Expected {message}{Found(actual)}", actual.Location);
    }

    public static ParserException Expected(string message)
    {
        return new ParserException($"Expected {message}");
    }

    public static string Found(Token token)
    {
        var location = token is EOF ? null : $", {token.Location}";
        return $", found {token}{location}";
    }
}