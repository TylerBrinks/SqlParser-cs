using System.Text.RegularExpressions;
using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using System.Globalization;
using System.Text;
using static SqlParser.Ast.ExactNumberInfo;
using static SqlParser.Ast.Statement;
using static SqlParser.Ast.GrantObjects;
using static SqlParser.Ast.Expression;
using static SqlParser.Ast.AlterTableOperation;

using DataType = SqlParser.Ast.DataType;
using Select = SqlParser.Ast.Select;
using Declare = SqlParser.Ast.Declare;
using HiveRowDelimiter = SqlParser.Ast.HiveRowDelimiter;
using Subscript = SqlParser.Ast.Subscript;

namespace SqlParser;

// This record type fills in the outcome from the Rust project's macro that
// intercepts control flow depending on parsing result.  The same flow is
// used in the parser, and the outcome of the lambda matches this record.  
public record MaybeParsed<T>(bool Parsed, T Result);

public partial class Parser
{
    // https://www.postgresql.org/docs/7.0/operators.htm#AEN2026ExpectRightParen
    public const short OrPrecedence = 5;
    public const short AndPrecedence = 10;
    public const short UnaryNotPrecedence = 15;
    public const short PgOtherPrecedence = 16;
    public const short IsPrecedence = 17;
    public const short LikePrecedence = 19;
    public const short BetweenPrecedence = 20;
    public const short PipePrecedence = 21;
    public const short CaretPrecedence = 22;
    public const short AmpersandPrecedence = 23;
    public const short XOrPrecedence = 24;
    public const short MulDivModOpPrecedence = 40;
    public const short AtTimeZonePrecedence = 41;
    public const short PlusMinusPrecedence = 30;

    //public const short MultiplyPrecedence = 40;
    public const short ArrowPrecedence = 50;

    private int _index;
    private Sequence<Token> _tokens = null!;
    private DepthGuard _depthGuard = null!;
    private Dialect _dialect = null!;
    private ParserOptions _options = null!;
    private ParserState _parserState = ParserState.Normal;

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
        _options = options ?? new ParserOptions
        {
            TrailingCommas = dialect.SupportsTrailingCommas
        };
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

    // ReSharper disable once GrammarMistakeInComment
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
            if (next is EOF)
            {
                break;
            }

            if (next is Word { Keyword: Keyword.END } && expectingStatementDelimiter)
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
            Keyword.FLUSH => ParseFlush(),
            Keyword.DESC => ParseExplain(DescribeAlias.Desc),
            Keyword.DESCRIBE => ParseExplain(DescribeAlias.Describe),
            Keyword.EXPLAIN => ParseExplain(DescribeAlias.Explain),
            Keyword.ANALYZE => ParseAnalyze(),
            Keyword.SELECT or Keyword.WITH or Keyword.VALUES => ParseQuery(true),
            Keyword.TRUNCATE => ParseTruncate(),
            Keyword.ATTACH => _dialect is DuckDbDialect ? ParseAttachDuckDbDatabase() : ParseAttachDatabase(),
            Keyword.DETACH when _dialect is DuckDbDialect or GenericDialect => ParseDetachDuckDbDatabase(),
            Keyword.MSCK => ParseMsck(),
            Keyword.CREATE => ParseCreate(),
            Keyword.CACHE => ParseCacheTable(),
            Keyword.DROP => ParseDrop(),
            Keyword.DISCARD => ParseDiscard(),
            Keyword.DECLARE => ParseDeclare(),
            Keyword.FETCH => ParseFetchStatement(),
            Keyword.DELETE => ParseDelete(),
            Keyword.INSERT => ParseInsert(),
            Keyword.REPLACE => ParseReplace(),
            Keyword.UNCACHE => ParseUncacheTable(),
            Keyword.UPDATE => ParseUpdate(),
            Keyword.ALTER => ParseAlter(),
            Keyword.CALL => ParseCall(),
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
            // `END` is a nonstandard but common alias for the standard 
            // `COMMIT TRANSACTION` statement. It is supported by PostgreSQL.
            Keyword.END => ParseEnd(),
            Keyword.SAVEPOINT => new Savepoint(ParseIdentifier()),
            Keyword.RELEASE => ParseRelease(),
            Keyword.COMMIT => new Commit(ParseCommitRollbackChain()),
            Keyword.ROLLBACK => ParseRollback(),
            Keyword.ASSERT => ParseAssert(),
            // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
            // syntax. They are used for Postgres prepared statement.
            Keyword.DEALLOCATE => ParseDeallocate(),
            Keyword.EXECUTE => ParseExecute(),
            Keyword.PREPARE => ParsePrepare(),
            Keyword.MERGE => ParseMerge(),
            Keyword.PRAGMA => ParsePragma(),
            Keyword.UNLOAD => ParseUnload(),
            // `INSTALL` is DuckDb specific https://duckdb.org/docs/extensions/overview
            Keyword.INSTALL when _dialect is DuckDbDialect or GenericDialect => ParseInstall(),
            // `LOAD` is DuckDb specific https://duckdb.org/docs/extensions/overview
            Keyword.LOAD when _dialect is DuckDbDialect or GenericDialect => ParseLoad(),

            _ => throw Expected("a SQL statement", PeekToken())
        };
    }

    public Statement ParseFlush()
    {
        string? channel = null;
        Sequence<ObjectName>? tables = null;
        var readLock = false;
        var export = false;
        FlushLocation? location = null;
        FlushType objectType;

        if (_dialect is not MySqlDialect or GenericDialect)
        {
            throw new ParserException("Unsupported statement FLUSH", PeekToken().Location);
        }

        if (ParseKeyword(Keyword.NO_WRITE_TO_BINLOG))
        {
            location = new FlushLocation.NoWriteToBinlog();
        }
        else if (ParseKeyword(Keyword.LOCAL))
        {
            location = new FlushLocation.Local();
        }

        if (ParseKeywordSequence(Keyword.BINARY, Keyword.LOGS))
        {
            objectType = FlushType.BinaryLogs;
        }
        else if (ParseKeywordSequence(Keyword.ENGINE, Keyword.LOGS))
        {
            objectType = FlushType.EngineLogs;
        }
        else if (ParseKeywordSequence(Keyword.ERROR, Keyword.LOGS))
        {
            objectType = FlushType.ErrorLogs;
        }
        else if (ParseKeywordSequence(Keyword.GENERAL, Keyword.LOGS))
        {
            objectType = FlushType.GeneralLogs;
        }
        else if (ParseKeyword(Keyword.HOSTS))
        {
            objectType = FlushType.Hosts;
        }
        else if (ParseKeyword(Keyword.PRIVILEGES))
        {
            objectType = FlushType.Privileges;

        }
        else if (ParseKeyword(Keyword.OPTIMIZER_COSTS))
        {
            objectType = FlushType.OptimizerCosts;

        }
        else if (ParseKeywordSequence(Keyword.RELAY, Keyword.LOGS))
        {
            if (ParseKeywordSequence(Keyword.FOR, Keyword.CHANNEL))
            {
                channel = ParseObjectName();
            }

            objectType = FlushType.RelayLogs;

        }
        else if (ParseKeywordSequence(Keyword.SLOW, Keyword.LOGS))
        {
            objectType = FlushType.SlowLogs;

        }
        else if (ParseKeyword(Keyword.STATUS))
        {
            objectType = FlushType.Status;

        }
        else if (ParseKeyword(Keyword.USER_RESOURCES))
        {

            objectType = FlushType.UserResources;
        }
        else if (ParseKeyword(Keyword.LOGS))
        {
            objectType = FlushType.Logs;

        }
        else if (ParseKeyword(Keyword.TABLES))
        {
            var loop = true;

            while (loop)
            {
                var next = NextToken();
                switch (next)
                {
                    case Word { Keyword: Keyword.WITH }:
                        readLock = ParseKeywordSequence(Keyword.READ, Keyword.LOCK);
                        break;

                    case Word { Keyword: Keyword.FOR }:
                        export = ParseKeyword(Keyword.EXPORT);
                        break;
                    case Word { Keyword: Keyword.undefined }:
                        PrevToken();
                        tables = ParseCommaSeparated(ParseObjectName);
                        break;

                    default:
                        loop = false;
                        break;
                }
            }

            objectType = FlushType.Tables;
        }
        else
        {
            throw Expected(
                "BINARY LOGS, ENGINE LOGS, ERROR LOGS, GENERAL LOGS, HOSTS, LOGS, PRIVILEGES, OPTIMIZER_COSTS, RELAY LOGS[FOR CHANNEL channel], SLOW LOGS, STATUS, USER_RESOURCES",
                PeekToken());
        }

        return new Flush(objectType, location, channel, readLock, export, tables);
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
        var partitions = ParseInit(ParseKeyword(Keyword.PARTITION),
            () => { return ExpectParens(() => ParseCommaSeparated(ParseExpr)); });

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

    public AttachDuckDbDatabase ParseAttachDuckDbDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var ifNotExists = ParseIfNotExists();
        var path = ParseIdentifier();
        Ident? alias = null;

        if (ParseKeyword(Keyword.AS))
        {
            alias = ParseIdentifier();
        }

        var attachOptions = ParseAttachDuckDbDatabaseOptions();
        return new AttachDuckDbDatabase(ifNotExists, database, path, alias, attachOptions);
    }

    public Sequence<AttachDuckDbDatabaseOption>? ParseAttachDuckDbDatabaseOptions()
    {
        if (!ConsumeToken<LeftParen>())
        {
            return null;
        }

        var options = new Sequence<AttachDuckDbDatabaseOption>();
        while (true)
        {
            if (ParseKeyword(Keyword.READ_ONLY))
            {
                bool? boolean = ParseKeyword(Keyword.TRUE) ? true : ParseKeyword(Keyword.FALSE) ? false : null;
                options.Add(new AttachDuckDbDatabaseOption.ReadOnly(boolean));
            }
            else if (ParseKeyword(Keyword.TYPE))
            {
                var ident = ParseIdentifier();
                options.Add(new AttachDuckDbDatabaseOption.Type(ident));
            }
            else
            {
                throw Expected("expected one of: ), READ_ONLY, TYPE", PeekToken());
            }

            if (ConsumeToken<RightParen>())
            {
                return options;
            }
            else if (ConsumeToken<Comma>())
            {
                continue;
            }

            throw Expected("expected one of: ')', ','", PeekToken());
        }
    }

    public DetachDuckDbDatabase ParseDetachDuckDbDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var ifExists = ParseIfExists();
        var alias = ParseIdentifier();
        return new DetachDuckDbDatabase(ifExists, database, alias);
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

    public Expression ParseWildcardExpr()
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
                                return new QualifiedWildcard(new ObjectName(idParts));

                            default:
                                throw Expected("an identifier or a '*' after '.'");
                        }
                    }

                    break;
                }
            case Multiply:
                return new Wildcard();
        }

        _index = index;
        return ParseExpr();
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
            FunctionArguments args;

            if (ConsumeToken<LeftParen>())
            {
                args = new FunctionArguments.List(ParseFunctionArgumentList());
            }
            else
            {
                args = new FunctionArguments.None();
            }

            return new Function(name)
            {
                Args = args
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

        Expression ParseConvertExpr()
        {
            if (_dialect is MsSqlDialect)
            {
                return ParseMsSqlConvert();
            }

            Expression expr;
            DataType? dataType;
            ObjectName? charset = null;

            if (_dialect.ConvertTypeBeforeValue)
            {
                return ExpectParens(() =>
                {
                    dataType = ParseDataType();
                    ExpectToken<Comma>();
                    expr = ParseExpr();
                    return new Expression.Convert(expr, null, charset, false, new Sequence<Expression>());

                });
            }

            ExpectLeftParen();
            expr = ParseExpr();

            if (ParseKeyword(Keyword.USING))
            {
                charset = ParseObjectName();
                ExpectRightParen();

                return new Expression.Convert(expr, null, charset, false, new Sequence<Expression>());
            }

            ExpectToken<Comma>();

            dataType = ParseDataType();
            if (ParseKeywordSequence(Keyword.CHARACTER, Keyword.SET))
            {
                charset = ParseObjectName();
            }

            ExpectRightParen();

            return new Expression.Convert(expr, dataType, charset, false, new Sequence<Expression>());
        }

        Expression ParseMsSqlConvert()
        {
            return ExpectParens(() =>
            {
                var dataType = ParseDataType();
                ExpectToken<Comma>();
                var expr = ParseExpr();
                var styles = new Sequence<Expression>();

                if (ConsumeToken<Comma>())
                {
                    styles = ParseCommaSeparated(ParseExpr);
                }

                return new Expression.Convert(expr, dataType, null, true, styles);
            });
        }

        CastFormat? ParseOptionalCastFormat()
        {
            if (ParseKeyword(Keyword.FORMAT))
            {
                var value = ParseValue();

                var timeZoneValue = ParseOptionalTimeZone();
                if (timeZoneValue != null)
                {
                    return new CastFormat.ValueAtTimeZone(value, timeZoneValue);
                }

                return new CastFormat.Value(value);
            }

            return null;
        }

        Value? ParseOptionalTimeZone()
        {
            if (ParseKeywordSequence(Keyword.AT, Keyword.TIME, Keyword.ZONE))
            {
                return ParseValue();
            }

            return null;
        }

        Expression ParseCastExpression(CastKind kind)
        {
            return ExpectParens(() =>
            {
                var expr = ParseExpr();
                ExpectKeyword(Keyword.AS);
                var dataType = ParseDataType();
                var format = ParseOptionalCastFormat();

                return new Cast(expr, dataType, kind, format);
            });
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
                DateTimeField field = new DateTimeField.NoDateTime();
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

        Expression ParsePositionExpr(Ident ident)
        {
            var positionExpression = MaybeParse(() =>
            {
                ExpectLeftParen();

                var expr = ParseSubExpression(BetweenPrecedence);

                ExpectKeyword(Keyword.IN);

                var from = ParseExpr();
                ExpectRightParen();

                return new Position(expr, from);
            });
           

            if (positionExpression != null)
            {
                return positionExpression;
            }

            return ParseFunction(new ObjectName(ident));
        }

        Substring ParseSubstringExpr()
        {
            return ExpectParens(() =>
            {
                var expr = ParseExpr();
                Expression? fromExpr = null;
                Expression? toExpr = null;
                var special = false;

                if (_dialect.SupportsSubstringFromForExpression)
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
                else if (_dialect is SnowflakeDialect or BigQueryDialect or GenericDialect && ConsumeToken<Comma>())
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

        Expression.Array ParseLeftArray()
        {
            ExpectToken<LeftBracket>();
            return ParseArrayExpr(true);
        }

        Expression ParseLeftParen()
        {
            Expression expr;
            Expression? lambda;

            var subQuery = TryParseExpressionSubQuery();
            if (subQuery != null)
            {
                expr = subQuery;
            }
            else if ((lambda = TryParseLambda()) != null)
            {
                return lambda;
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

        Expression ParseStruct()
        {
            PrevToken();
            return ParseBigQueryStructLiteral();
        }

        Expression ParseConnectByExpression()
        {
            return new Prior(ParseSubExpression(PlusMinusPrecedence));
        }

        Expression ParseDuckDbMapLiteral()
        {
            ExpectToken<LeftBrace>();
            var fields = ParseCommaSeparated(ParseDuckDbMapField);
            ExpectToken<RightBrace>();
            return new Expression.Map(new Ast.Map(fields));
        }

        MapEntry ParseDuckDbMapField()
        {
            var key = ParseExpr();

            ExpectToken<Colon>();

            var value = ParseExpr();

            return new MapEntry(key, value);
        }

        Expression ParseBigQueryStructLiteral()
        {
            //var (fields, trailingBracket) = ParseStructTypeDef(ParseBigQueryStructFieldDef);
            var (fields, trailingBracket) = ParseStructTypeDef(ParseStructFieldDef);

            if (trailingBracket)
            {
                throw new ParserException("Unmatched > in STRUCT literal", PeekToken().Location);
            }

            var values = ExpectParens(() => ParseCommaSeparated(() => ParseStructFieldExpression(fields.Any())));

            return new Struct(values, fields);
        }

        Expression ParseMultipart(Word word)
        {
            var tkn = PeekToken();
            if (tkn is LeftParen or Period)
            {
                var idParts = new Sequence<Ident> { word.ToIdent() };
                var endsWithWildcard = false;
                while (ConsumeToken<Period>())
                {
                    switch (NextToken())
                    {
                        case Word w:
                            idParts.Add(w.ToIdent());
                            break;

                        case Multiply:
                            if (_dialect is PostgreSqlDialect)
                            {
                                endsWithWildcard = true;
                                break;
                            }
                            else
                            {
                                throw Expected("an identifier after '.'", PeekToken());
                            }

                        case SingleQuotedString s:
                            idParts.Add(new Ident(s.Value, Symbols.SingleQuote));
                            break;

                        default:
                            throw Expected("an identifier or a '*' after '.'", PeekToken());
                    }
                }

                if (endsWithWildcard)
                {
                    return new QualifiedWildcard(new ObjectName(idParts));
                }

                if (ConsumeToken<LeftParen>())
                {
                    if (_dialect is SnowflakeDialect or MsSqlDialect && ConsumeTokens(typeof(Plus), typeof(RightParen)))
                    {
                        if (idParts.Count == 1)
                        {
                            return new OuterJoin(new Identifier(idParts[0]));
                        }

                        return new OuterJoin(new CompoundIdentifier(idParts));
                    }

                    PrevToken();
                    return ParseFunction(new ObjectName(idParts));
                }

                return new CompoundIdentifier(idParts);
            }

            if (tkn is SingleQuotedString or DoubleQuotedString or HexStringLiteral && word.Value.StartsWith("_"))
            {
                return new IntroducedString(word.Value, ParseIntroducedStringValue());
            }

            if (tkn is Arrow && _dialect.SupportsLambdaFunctions)
            {
                ExpectToken<Arrow>();
                return new Lambda(new LambdaFunction(new OneOrManyWithParens<Ident>.One(word.ToIdent()), ParseExpr()));
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

        Function ParseArraySubquery(Word word)
        {
            return ExpectParens(() =>
            {
                var query = ParseQuery();
                return new Function(new ObjectName(word.ToIdent()))
                {
                    Args = new FunctionArguments.Subquery(query)
                };
            });
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
            Word
            {
                Keyword: Keyword.CURRENT_CATALOG or Keyword.CURRENT_USER or Keyword.SESSION_USER or Keyword.USER
            } word
                when _dialect is PostgreSqlDialect or GenericDialect
                => new Function(new ObjectName(word.ToIdent()))
                {
                    Args = new FunctionArguments.None()
                },

            Word
            {
                Keyword: Keyword.CURRENT_TIMESTAMP or Keyword.CURRENT_TIME or Keyword.CURRENT_DATE
                    or Keyword.LOCALTIME or Keyword.LOCALTIMESTAMP
            } word
                => ParseTimeFunctions(new ObjectName(word.ToIdent())),

            Word { Keyword: Keyword.CASE } => ParseCaseExpr(),
            Word { Keyword: Keyword.CONVERT } => ParseConvertExpr(),
            Word { Keyword: Keyword.CAST } => ParseCastExpression(CastKind.Cast),
            Word { Keyword: Keyword.TRY_CAST } => ParseCastExpression(CastKind.TryCast),
            Word { Keyword: Keyword.SAFE_CAST } => ParseCastExpression(CastKind.SafeCast),
            Word { Keyword: Keyword.EXISTS } when SupportDataBricksExists() => ParseExistsExpr(false),
            Word { Keyword: Keyword.EXTRACT } => ParseExtractExpr(),
            Word { Keyword: Keyword.CEIL } => ParseCeilFloorExpr(true),
            Word { Keyword: Keyword.FLOOR } => ParseCeilFloorExpr(false),
            Word { Keyword: Keyword.POSITION } p when PeekToken() is LeftParen => ParsePositionExpr(p.ToIdent()),
            Word { Keyword: Keyword.SUBSTRING } => ParseSubstringExpr(),
            Word { Keyword: Keyword.OVERLAY } => ParseOverlayExpr(),
            Word { Keyword: Keyword.TRIM } => ParseTrimExpr(),
            Word { Keyword: Keyword.INTERVAL } => ParseInterval(),
            // Treat ARRAY[1,2,3] as an array [1,2,3], otherwise try as subquery or a function call
            Word { Keyword: Keyword.ARRAY } when PeekToken() is LeftBracket => ParseLeftArray(),
            Word { Keyword: Keyword.ARRAY } arr when
                PeekToken() is LeftParen &&
                _dialect is not ClickHouseDialect and not DatabricksDialect
                => ParseArraySubquery(arr),
            //Word { Keyword: Keyword.ARRAY_AGG } => ParseArrayAggregateExpression(),
            Word { Keyword: Keyword.NOT } => ParseNot(),
            Word { Keyword: Keyword.MATCH } when _dialect is MySqlDialect or GenericDialect => ParseMatchAgainst(),
            Word { Keyword: Keyword.STRUCT } when _dialect is BigQueryDialect or GenericDialect => ParseStruct(),
            Word { Keyword: Keyword.PRIOR } when _parserState == ParserState.ConnectBy => ParseConnectByExpression(),
            Word { Keyword: Keyword.MAP } when _dialect.SupportMapLiteralSyntax && PeekTokenIs<LeftBrace>() => ParseDuckDbMapLiteral(),
            //  
            // Here `word` is a word, check if it's a part of a multipart
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
                or TripleSingleQuotedString
                or TripleDoubleQuotedString
                or DollarQuotedString
                or SingleQuotedByteStringLiteral
                or DoubleQuotedByteStringLiteral
                or TripleSingleQuotedByteStringLiteral
                or TripleDoubleQuotedByteStringLiteral
                or SingleQuotedRawStringLiteral
                or DoubleQuotedRawStringLiteral
                or TripleSingleQuotedRawStringLiteral
                or TripleDoubleQuotedRawStringLiteral
                or RawStringLiteral
                or NationalStringLiteral
                or HexStringLiteral
                or UnicodeStringLiteral
                => ParseTokenValue(),

            LeftParen => ParseLeftParen(),
            Placeholder or Colon or AtSign => ParseTokenValue(),
            LeftBrace when _dialect.SupportsDictionarySyntax => ParseDuckDbStructLiteral(),

            _ => throw Expected("an expression", token)
        };

        #endregion

        if (ParseKeyword(Keyword.COLLATE))
        {
            return new Collate(expr, ParseObjectName());
        }

        return expr;

        bool SupportDataBricksExists()
        {
            var word = PeekNthToken(1) as Word;

            return _dialect is not DatabricksDialect || word is { Keyword: Keyword.SELECT } ||
                   word is { Keyword: Keyword.WITH };
        }
    }

    public Expression? TryParseExpressionSubQuery()
    {
        var keyword = ParseOneOfKeywords(Keyword.SELECT, Keyword.WITH);
        if (keyword == Keyword.undefined)
        {
            return null;
        }

        PrevToken();
        return new Subquery(ParseQuery());
    }

    public Expression? TryParseLambda()
    {
        if (!_dialect.SupportsLambdaFunctions)
        {
            return null;
        }

        return MaybeParse(() =>
        {
            var parameters = ParseCommaSeparated(ParseIdentifier);
            ExpectToken<RightParen>();
            ExpectToken<Arrow>();
            var expr = ParseExpr();
            return new Lambda(new LambdaFunction(new OneOrManyWithParens<Ident>.Many(parameters), expr));
        });
    }

    public Dictionary ParseDuckDbStructLiteral()
    {
        PrevToken();
        ExpectToken<LeftBrace>();
        var fields = ParseCommaSeparated(ParseDuckDbDictionaryField);
        ExpectToken<RightBrace>();

        return new Dictionary(fields);
    }

    /// <summary>
    /// Parse a field for a DuckDb dictionary
    ///
    /// https://duckdb.org/docs/sql/data_types/struct#creating-structs
    /// </summary>
    /// <returns>DictionaryField</returns>
    public DictionaryField ParseDuckDbDictionaryField()
    {
        var key = ParseIdentifier();

        ExpectToken<Colon>();

        var expression = ParseExpr();

        return new DictionaryField(key, expression);
    }

    /// <summary>
    /// Parse an expression value for a bigquery struct
    /// </summary>
    public (StructField, bool) ParseStructFieldDef()
    {
        var isAnonymous = !(PeekNthToken(0) is Word && PeekNthToken(1) is Word);

        var fieldName = isAnonymous
            ? null
            : ParseIdentifier();

        var (fieldType, trailingBracket) = ParseDataTypeHelper();

        return (new StructField(fieldType, fieldName), trailingBracket);
    }

    public (Sequence<StructField> Fields, bool MatchingTrailingBracket) ParseStructTypeDef(
        Func<(StructField, bool)> elementParser)
    {
        var startToken = PeekToken();
        ExpectKeyword(Keyword.STRUCT);

        if (PeekToken() is not LessThan)
        {
            return (new Sequence<StructField>(), false);
        }

        NextToken();

        var fieldDefinitions = new Sequence<StructField>();
        bool trailingBracket;

        while (true)
        {
            (var field, trailingBracket) = elementParser();

            fieldDefinitions.Add(field);
            if (!ConsumeToken<Comma>())
            {
                break;
            }

            // Angle brackets are balanced, so we only expect the trailing `>>` after
            // we've matched all field types for the current struct.
            // e.g. this is invalid syntax `STRUCT<STRUCT<INT>>>, INT>(NULL)`
            if (trailingBracket)
            {
                throw new ParserException("unmatched > in STRUCT definition", startToken.Location);
            }
        }

        return (fieldDefinitions, ExpectClosingAngleBracket(trailingBracket));
    }

    public Expression ParseStructFieldExpression(bool typedSyntax)
    {
        var expression = ParseExpr();
        if (ParseKeyword(Keyword.AS))
        {
            if (typedSyntax)
            {
                PrevToken();
                throw new ParserException("Typed syntax does not allow AS", PeekToken().Location);
            }

            var fieldName = ParseIdentifier();
            return new Named(expression, fieldName);
        }

        return expression;
    }

    public (DataType, DataType) ParseClickHouseMapDef()
    {
        ExpectKeyword(Keyword.MAP);

        var (key, value) = ExpectParens(() =>
        {
            var keyDataType = ParseDataType();
            ExpectToken<Comma>();
            var valueDataType = ParseDataType();

            return (keyDataType, valueDataType);
        });

        return (key, value);
    }

    public Sequence<StructField> ParseClickHouseTupleDef()
    {
        ExpectKeyword(Keyword.TUPLE);
        var fieldDefinitions = ExpectParens(() =>
        {
            var definitions = new Sequence<StructField>();
            while (true)
            {
                var (def, _) = ParseStructFieldDef();
                definitions.Add(def);
                if (!ConsumeToken<Comma>())
                {
                    break;
                }
            }

            return definitions;
        });

        return fieldDefinitions;
    }

    private bool ExpectClosingAngleBracket(bool trailingBracket)
    {
        if (trailingBracket)
        {
            return false;
        }

        var next = PeekToken();
        switch (next)
        {
            case GreaterThan:
                NextToken();
                return false;
            case ShiftRight:
                NextToken();
                return true;
            default:
                throw Expected(">", next);
        }
    }

    public Expression ParseFunction(ObjectName name)
    {
        ExpectLeftParen();

        if (_dialect is SnowflakeDialect && ParseOneOfKeywords(Keyword.WITH, Keyword.SELECT) != Keyword.undefined)
        {
            var subquery = ParseQuery(true);
            ExpectRightParen();
            return new Function(name)
            {
                Args = new FunctionArguments.Subquery(subquery)
            };
        }

        var args = ParseFunctionArgumentList();
        FunctionArguments? parameters = null;
        // ReSharper disable once GrammarMistakeInComment
        // ClickHouse aggregations support parametric functions like `HISTOGRAM(0.5, 0.6)(x, y)`
        // which (0.5, 0.6) is a parameter to the function.
        if (_dialect is ClickHouseDialect or GenericDialect && ConsumeToken<LeftParen>())
        {
            parameters = new FunctionArguments.List(args);
            args = ParseFunctionArgumentList();
        }

        Sequence<OrderByExpression>? withinGroup = null;

        if (ParseKeywordSequence(Keyword.WITHIN, Keyword.GROUP))
        {
            withinGroup = ExpectParens(() =>
            {
                ExpectKeywords(Keyword.ORDER, Keyword.BY);
                return ParseCommaSeparated(ParseOrderByExpr);
            });
        }

        Expression? filter = null;
        if (_dialect.SupportsFilterDuringAggregation &&
            ParseKeyword(Keyword.FILTER) &&
            ConsumeToken<LeftParen>() &&
            ParseKeyword(Keyword.WHERE))
        {
            var filterExpression = ParseExpr();
            ExpectToken<RightParen>();
            filter = filterExpression;
        }

        NullTreatment? nullTreatment = null;

        if (args.Clauses == null || args.Clauses!.Count == 0 ||
            args.Clauses.All(c => c is not FunctionArgumentClause.IgnoreOrRespectNulls))
        {
            nullTreatment = ParseNullTreatment();
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
            Args = new FunctionArguments.List(args),
            Filter = filter,
            Over = over,
            NullTreatment = nullTreatment,
            WithinGroup = withinGroup,
            Parameters = parameters
        };
    }

    public FunctionArgumentList ParseFunctionArgumentList()
    {
        if (ConsumeToken<RightParen>())
        {
            return FunctionArgumentList.Empty();
        }

        var duplicateTreatment = ParseDuplicateTreatment();
        var args = ParseCommaSeparated(ParseFunctionArgs);
        Sequence<FunctionArgumentClause>? clauses = null;

        if (_dialect.SupportsWindowFunctionNullTreatmentArg)
        {
            var nullTreatment = ParseNullTreatment();
            if (nullTreatment != null)
            {
                clauses ??= [];
                clauses.Add(new FunctionArgumentClause.IgnoreOrRespectNulls(nullTreatment));
            }
        }

        if (ParseKeywordSequence(Keyword.ORDER, Keyword.BY))
        {
            clauses ??= [];
            clauses.Add(new FunctionArgumentClause.OrderBy(ParseCommaSeparated(ParseOrderByExpr)));
        }

        if (ParseKeyword(Keyword.LIMIT))
        {
            clauses ??= [];
            clauses.Add(new FunctionArgumentClause.Limit(ParseExpr()));
        }

        if (_dialect is GenericDialect or BigQueryDialect && ParseKeyword(Keyword.HAVING))
        {
#pragma warning disable CS8509 // The switch expression does not handle all possible values of its input type (it is not exhaustive).
            var kind = ParseOneOfKeywords(Keyword.MIN, Keyword.MAX) switch
            {
                Keyword.MIN => HavingBoundKind.Min,
                Keyword.MAX => HavingBoundKind.Max,
            };
#pragma warning restore CS8509 // The switch expression does not handle all possible values of its input type (it is not exhaustive).

            clauses ??= [];
            clauses.Add(new FunctionArgumentClause.Having(new HavingBound(kind, ParseExpr())));
        }

        if (_dialect is GenericDialect or MySqlDialect && ParseKeyword(Keyword.SEPARATOR))
        {
            clauses ??= [];
            clauses.Add(new FunctionArgumentClause.Separator(ParseValue()));
        }

        var onOverflow = ParseListAggOnOverflow();
        if (onOverflow != null)
        {
            clauses ??= [];
            clauses.Add(new FunctionArgumentClause.OnOverflow(onOverflow));
        }

        ExpectToken<RightParen>();

        return new FunctionArgumentList(args, duplicateTreatment, clauses);
    }

    public ListAggOnOverflow? ParseListAggOnOverflow()
    {
        if (ParseKeywordSequence(Keyword.ON, Keyword.OVERFLOW))
        {
            if (ParseKeyword(Keyword.ERROR))
            {
                return new ListAggOnOverflow.Error();
            }

            ExpectKeyword(Keyword.TRUNCATE);

            var token = PeekToken();

            Expression? filter = null;

            if (token is Word w)
            {
                if (w.Keyword == Keyword.WITH || w.Keyword == Keyword.WITHOUT)
                {
                    filter = null;
                }
            }
            else if (token
                     is SingleQuotedString
                     or EscapedStringLiteral
                     or UnicodeStringLiteral
                     or NationalStringLiteral
                     or HexStringLiteral)
            {
                filter = ParseExpr();
            }
            else
            {
                throw Expected("either filler, WITH, or WITHOUT in LISTAGG", token);
            }

            var withCount = ParseKeyword(Keyword.WITH);
            if (!withCount && !ParseKeywordSequence(Keyword.WITHOUT))
            {
                throw Expected("either WITH or WITHOUT in LISTAGG", PeekToken());
            }

            ExpectKeyword(Keyword.COUNT);
            return new ListAggOnOverflow.Truncate { Filler = filter, WithCount = withCount };
        }

        return null;
    }

    public NullTreatment? ParseNullTreatment()
    {
        var keyword = ParseOneOfKeywords(Keyword.RESPECT, Keyword.IGNORE);
        if (keyword != Keyword.undefined)
        {
            ExpectKeyword(Keyword.NULLS);

            return keyword switch
            {
                Keyword.RESPECT => new NullTreatment.RespectNulls(),
                Keyword.IGNORE => new NullTreatment.IgnoreNulls(),
                _ => null
            };
        }

        return null;
    }

    public DuplicateTreatment? ParseDuplicateTreatment()
    {
        var token = PeekToken();

        var hasAll = ParseKeyword(Keyword.ALL);
        var hasDistinct = ParseKeyword(Keyword.DISTINCT);

        return (hasAll, hasDistinct) switch
        {
            (true, false) => DuplicateTreatment.All,
            (false, true) => DuplicateTreatment.Distinct,
            (false, false) => null,
            (true, true) => throw new ParserException("Cannot specify both ALL and DISTINCT", token.Location)
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

        NamedWindowExpression windowSpec;

        if (ConsumeToken<LeftParen>())
        {
            windowSpec = new NamedWindowExpression.NamedWindowSpec(ParseWindowSpec());
        }
        else if (_dialect.SupportsWindowClauseNamedWindowReference)
        {
            windowSpec = new NamedWindowExpression.NamedWindow(ParseIdentifier());
        }
        else
        {
            throw Expected("(", PeekToken());
        }

        return new NamedWindowDefinition(ident, windowSpec);
    }

    /// <summary>
    /// Parse window spec expression
    /// </summary>
    public WindowSpec ParseWindowSpec()
    {
        Ident? windowName = null;
        if (PeekToken() is Word { Keyword: Keyword.undefined })
        {
            windowName = MaybeParse(ParseIdentifier);
        }

        var partitionBy = ParseInit(ParseKeywordSequence(Keyword.PARTITION, Keyword.BY),
            () => ParseCommaSeparated(ParseExpr));
        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY),
            () => ParseCommaSeparated(ParseOrderByExpr));
        var windowFrame = ParseInit(!ConsumeToken<RightParen>(), () =>
        {
            var windowFrame = ParseWindowFrame();
            ExpectRightParen();
            return windowFrame;
        });

        return new WindowSpec(partitionBy, orderBy, windowFrame, windowName);
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

            attributes.Add(
                new UserDefinedTypeCompositeAttributeDef(attributeName, attributeDataType, attributeCollation));
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

            if (ParseKeyword(Keyword.ROLLUP))
            {
                return CreateGroupExpr(true, true, e => new Rollup(e));
            }

            if (ConsumeTokens(typeof(LeftParen), typeof(RightParen)))
            {
                // PostgreSQL allow to use empty tuple as a group by expression,
                // e.g. `GROUP BY (), name`. Please refer to GROUP BY Clause section in
                return new Expression.Tuple([]);
            }

            return ParseExpr();
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

            return [ParseExpr()];
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
            Keyword.YEAR => new DateTimeField.Year(),
            Keyword.MONTH => new DateTimeField.Month(),
            Keyword.WEEK => ParseWeek(),
            Keyword.DAY => new DateTimeField.Day(),
            Keyword.DAYOFWEEK => new DateTimeField.DayOfWeek(),
            Keyword.DAYOFYEAR => new DateTimeField.DayOfYear(),
            Keyword.DATE => new DateTimeField.Date(),
            Keyword.DATETIME => new DateTimeField.DateTime(),
            Keyword.HOUR => new DateTimeField.Hour(),
            Keyword.MINUTE => new DateTimeField.Minute(),
            Keyword.SECOND => new DateTimeField.Second(),
            Keyword.CENTURY => new DateTimeField.Century(),
            Keyword.DECADE => new DateTimeField.Decade(),
            Keyword.DOY => new DateTimeField.Doy(),
            Keyword.DOW => new DateTimeField.Dow(),
            Keyword.EPOCH => new DateTimeField.Epoch(),
            Keyword.ISODOW => new DateTimeField.Isodow(),
            Keyword.ISOYEAR => new DateTimeField.Isoyear(),
            Keyword.ISOWEEK => new DateTimeField.IsoWeek(),
            Keyword.JULIAN => new DateTimeField.Julian(),
            Keyword.MICROSECOND => new DateTimeField.Microsecond(),
            Keyword.MICROSECONDS => new DateTimeField.Microseconds(),
            Keyword.MILLENIUM => new DateTimeField.Millenium(),
            Keyword.MILLENNIUM => new DateTimeField.Millennium(),
            Keyword.MILLISECOND => new DateTimeField.Millisecond(),
            Keyword.MILLISECONDS => new DateTimeField.Milliseconds(),
            Keyword.NANOSECOND => new DateTimeField.Nanosecond(),
            Keyword.NANOSECONDS => new DateTimeField.Nanoseconds(),
            Keyword.QUARTER => new DateTimeField.Quarter(),
            Keyword.TIME => new DateTimeField.Time(),
            Keyword.TIMEZONE => new DateTimeField.Timezone(),
            Keyword.TIMEZONE_ABBR => new DateTimeField.TimezoneAbbr(),
            Keyword.TIMEZONE_HOUR => new DateTimeField.TimezoneHour(),
            Keyword.TIMEZONE_MINUTE => new DateTimeField.TimezoneMinute(),
            Keyword.TIMEZONE_REGION => new DateTimeField.TimezoneRegion(),
            _ when _dialect is SnowflakeDialect or GenericDialect => ParseCustomDate(),
            _ => throw Expected("date/time field", token)
        };

        DateTimeField ParseWeek()
        {
            Ident? weekday = null;

            if (_dialect is BigQueryDialect or GenericDialect && ConsumeToken<LeftParen>())
            {
                weekday = ParseIdentifier();
                ExpectToken<RightParen>();
            }

            return new DateTimeField.Week(weekday);
        }

        DateTimeField ParseCustomDate()
        {
            PrevToken();
            return new DateTimeField.Custom(ParseIdentifier());
        }
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
        DateTimeField lastField = new DateTimeField.None();

        if (leadingField is DateTimeField.Second)
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
                if (lastField is DateTimeField.Second)
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
        return Extensions.DateTimeFields.Any(kwd => kwd == keyword) ? ParseDateTimeField() : new DateTimeField.None();
    }

    public Subscript ParseSubscriptInner()
    {
        Expression? lowerBound = null;

        if (!ConsumeToken<Colon>())
        {
            lowerBound = ParseExpr();
        }

        if (ConsumeToken<RightBracket>())
        {
            if (lowerBound != null)
            {
                return new Subscript.Index(lowerBound);
            }

            return new Subscript.Slice(lowerBound, null, null);
        }

        if (lowerBound != null)
        {
            ExpectToken<Colon>();
        }

        if (ConsumeToken<RightBracket>())
        {
            return new Subscript.Slice(lowerBound, null, null);
        }

        var upperBound = ParseExpr();

        if (ConsumeToken<RightBracket>())
        {
            return new Subscript.Slice(lowerBound, upperBound, null);
        }

        ExpectToken<Colon>();

        Expression? stride = null;

        if (!ConsumeToken<RightBracket>())
        {
            stride = ParseExpr();
        }

        if (stride != null)
        {
            ExpectToken<RightBracket>();
        }

        return new Subscript.Slice(lowerBound, upperBound, stride);
    }

    public Expression ParseSubscript(Expression expression)
    {
        var subscript = ParseSubscriptInner();
        return new Expression.Subscript(expression, subscript);
    }

    public JsonAccess ParseJsonAccess(Expression expr)
    {
        var path = new Sequence<JsonPathElement>();
        while (true)
        {
            var next = NextToken();

            if (next is Colon && path.Count == 0)
            {
                path.Add(ParseJsonPathObjectKey());
            }
            else if (next is Period && path.Any())
            {
                path.Add(ParseJsonPathObjectKey());

            }
            else if (next is LeftBracket)
            {
                var key = ParseExpr();
                ExpectToken<RightBracket>();

                path.Add(new JsonPathElement.Bracket(key));
            }
            else
            {
                PrevToken();
                break;
            }
        }

        return new JsonAccess(expr, new JsonPath(path));
    }

    public JsonPathElement ParseJsonPathObjectKey()
    {
        var token = NextToken();
        if (token is Word w)
        {
            // path segments in SF dot notation can be unquoted or double-quoted
            var quoted = w.QuoteStyle == Symbols.DoubleQuote;
            return new JsonPathElement.Dot(w.Value, quoted);
        }
        else if (token is DoubleQuotedString d)
        {
            return new JsonPathElement.Dot(d.Value, true);
        }

        throw Expected("Variant object key name", token);
    }
    /// <summary>
    /// Parse the ESCAPE CHAR portion of LIKE, ILIKE, and SIMILAR TO
    /// </summary>
    public string? ParseEscapeChar()
    {
        if (ParseKeyword(Keyword.ESCAPE))
        {
            return ParseLiteralString();
        }

        return null;
    }
    //public Expression ParseArrayIndex(Expression expr)
    //{
    //    var index = ParseExpr();
    //    ExpectToken<RightBracket>();
    //    var indexes = new Sequence<Expression> { index };

    //    while (ConsumeToken<LeftBracket>())
    //    {
    //        var innerIndex = ParseExpr();
    //        ExpectToken<RightBracket>();
    //        indexes.Add(innerIndex);
    //    }

    //    return new ArrayIndex(expr, indexes);
    //}

    public LockTable ParseLockTable()
    {
        var table = ParseIdentifier();
        var alias = ParseOptionalAlias(new[] { Keyword.READ, Keyword.WRITE, Keyword.LOW_PRIORITY });
        LockTableType lockType;

        if (ParseKeyword(Keyword.READ))
        {
            lockType = ParseKeyword(Keyword.LOCAL) ? new LockTableType.Read(true) : new LockTableType.Read(false);
        }
        else if (ParseKeyword(Keyword.WRITE))
        {
            lockType = new LockTableType.Write(false);
        }
        else if (ParseKeywordSequence(Keyword.LOW_PRIORITY, Keyword.WRITE))
        {
            lockType = new LockTableType.Write(true);
        }
        else
        {
            throw Expected("an lock type in LOCK TABLES", PeekToken());
        }

        return new LockTable(table, alias, lockType);
    }

    public Expression ParseMapAccess(Expression expr)
    {
        var key = ParseExpr();
        ConsumeToken<RightBracket>();

        var keys = new Sequence<MapAccessKey>
        {
            new(key, MapAccessSyntax.Bracket)
        };

        while (true)
        {
            var token = PeekToken();

            if (token is LeftBracket)
            {
                NextToken();
                var parsed = ParseExpr();
                ExpectToken<RightBracket>();
                keys.Add(new MapAccessKey(parsed, MapAccessSyntax.Bracket));
            }
            else if (token is Period && _dialect is BigQueryDialect)
            {
                NextToken();
                keys.Add(new MapAccessKey(ParseExpr(), MapAccessSyntax.Period));
            }
            else
            {
                break;
            }
        }

        return new MapAccess(expr, keys);
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
                if (_dialect.SupportsInEmptyList)
                {
                    return new InList(expr, ParseCommaSeparatedEmpty(ParseExpr), negated);
                }

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
    /// Parse a comma-separated list of 1+ SelectItem
    /// </summary>
    /// <returns>List of select items</returns>
    public Sequence<SelectItem> ParseProjection()
    {
        // BigQuery allows trailing commas, but only in project lists
        // e.g. `SELECT 1, 2, FROM t`
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#trailing_commas
        //
        // This pattern could be captured better with RAII type semantics, but it's quite a bit of
        // code to add for just one case, so we'll just do it manually here.
        var oldValue = _options.TrailingCommas;

        _options.TrailingCommas |= _dialect.SupportsProjectionTrailingCommas;

        var result = ParseCommaSeparated(ParseSelectItem);
        _options.TrailingCommas = oldValue;

        return result;
    }
    /// <summary>
    /// Parse a comma-separated list of 0+ items accepted by type T
    /// </summary>
    /// <typeparam name="T">Type of item to parse</typeparam>
    /// <param name="action">Parse action</param>
    /// <returns>List of T instances</returns>
    public Sequence<T> ParseCommaSeparatedEmpty<T>(Func<T> action)
    {
        if (PeekToken() is RightParen)
        {
            return new Sequence<T>();
        }

        if (_options.TrailingCommas && PeekNthToken(0) is Comma && PeekNthToken(1) is RightParen)
        {
            ConsumeToken<Comma>();
            return new Sequence<T>();
        }

        return ParseCommaSeparated(action);
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
        var persistent = _dialect is DuckDbDialect && ParseKeyword(Keyword.PERSISTENT);

        if (ParseKeyword(Keyword.TABLE))
        {
            return new Statement.CreateTable(ParseCreateTable(orReplace, temporary, global, transient));
        }

        if (ParseKeyword(Keyword.MATERIALIZED) || ParseKeyword(Keyword.VIEW))
        {
            PrevToken();
            return ParseCreateView(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.EXTERNAL))
        {
            return new Statement.CreateTable(ParseCreateExternalTable(orReplace));
        }

        if (ParseKeyword(Keyword.FUNCTION))
        {
            return ParseCreateFunction(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.MACRO))
        {
            return ParseCreateMacro(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.SECRET))
        {
            return ParseCreateSecret(orReplace, temporary, persistent);
        }

        if (orReplace)
        {
            ThrowExpected("[EXTERNAL] TABLE or [MATERIALIZED] VIEW or FUNCTION after CREATE OR REPLACE", PeekToken());
        }

        if (ParseKeyword(Keyword.EXTENSION))
        {
            return ParseCreateExtension();
        }

        if (ParseKeyword(Keyword.INDEX))
        {
            return new Statement.CreateIndex(ParseCreateIndex(false));
        }

        if (ParseKeywordSequence(Keyword.UNIQUE, Keyword.INDEX))
        {
            return new Statement.CreateIndex(ParseCreateIndex(true));
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

    public Statement ParseCreateSecret(bool orReplace, bool temporary, bool persistent)
    {
        var ifNotExists = ParseIfNotExists();
        Ident? storageSpecifier = null;
        Ident? name = null;

        if (!PeekTokenIs<LeftParen>())
        {
            if (ParseKeyword(Keyword.IN))
            {
                storageSpecifier = ParseIdentifier();
            }
            else
            {
                name = ParseIdentifier();
            }

            if (storageSpecifier == null && !PeekTokenIs<LeftParen>() && ParseKeyword(Keyword.IN))
            {
                storageSpecifier = ParseIdentifier();
            }
        }

        ExpectToken<LeftParen>();
        ExpectKeyword(Keyword.TYPE);
        var secretType = ParseIdentifier();
        var options = new Sequence<SecretOption>();

        if (ConsumeToken<Comma>())
        {
            options.AddRange(ParseCommaSeparated(() => new SecretOption(ParseIdentifier(), ParseIdentifier())));
        }

        ExpectToken<RightParen>();

        bool? temp = (temporary, persistent) switch
        {
            (true, false) => true,
            (false, true) => false,
            (false, false) => null,
            _ => throw Expected("TEMPORARY or PERSISTENT", PeekToken())
        };

        return new CreateSecret(orReplace, temp, ifNotExists, name, storageSpecifier, secretType, options);
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
        ExpectKeyword(Keyword.TABLE);
        var ifExists = ParseKeywordSequence(Keyword.IF, Keyword.EXISTS);
        var tableName = ParseObjectName();

        return new UNCache(tableName, ifExists);
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
        // ReSharper disable once GrammarMistakeInComment
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

    public Statement ParseCreateFunction(bool orReplace, bool temporary)
    {
        if (_dialect is HiveDialect)
        {
            return ParseHiveCreateFunction();
        }

        if (_dialect is PostgreSqlDialect or GenericDialect)
        {
            return ParsePostgresCreateFunction();
        }

        if (_dialect is DuckDbDialect)
        {
            return ParseCreateMacro(orReplace, temporary);
        }

        if (_dialect is BigQueryDialect)
        {
            return ParseBigqueryCreateFunction();
        }

        PrevToken();
        throw Expected("an object type after CREATE", PeekToken());

        Statement ParseHiveCreateFunction()
        {
            var name = ParseObjectName();
            ExpectKeyword(Keyword.AS);

            var @as = ParseCreateFunctionBodyString();
            var @using = ParseOptionalCreateFunctionUsing();

            return new CreateFunction(name)
            {
                OrReplace = orReplace,
                Temporary = temporary,
                FunctionBody = new CreateFunctionBody.AsBeforeOptions(@as),
                Using = @using
            };
        }

        Statement ParsePostgresCreateFunction()
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

            CreateFunctionBody? functionBody = null;
            Ident? language = null;
            FunctionBehavior? behavior = null;
            FunctionCalledOnNull? calledOnNull = null;
            FunctionParallel? parallel = null;

            while (true)
            {
                if (ParseKeyword(Keyword.AS))
                {
                    EnsureNotSet(functionBody, "AS");

                    functionBody = new CreateFunctionBody.AsBeforeOptions(ParseCreateFunctionBodyString());
                }
                else if (ParseKeyword(Keyword.LANGUAGE))
                {
                    EnsureNotSet(language, "LANGUAGE");
                    language = ParseIdentifier();
                }
                else if (ParseKeyword(Keyword.IMMUTABLE))
                {
                    EnsureNotSet(behavior, "IMMUTABLE | STABLE | VOLATILE");
                    behavior = FunctionBehavior.Immutable;
                }
                else if (ParseKeyword(Keyword.STABLE))
                {
                    EnsureNotSet(behavior, "IMMUTABLE | STABLE | VOLATILE");
                    behavior = FunctionBehavior.Stable;
                }
                else if (ParseKeyword(Keyword.VOLATILE))
                {
                    EnsureNotSet(behavior, "IMMUTABLE | STABLE | VOLATILE");
                    behavior = FunctionBehavior.Volatile;
                }
                else if (ParseKeywordSequence(Keyword.CALLED, Keyword.ON, Keyword.NULL, Keyword.INPUT))
                {
                    EnsureNotSet(calledOnNull, "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT");
                    calledOnNull = FunctionCalledOnNull.CalledOnNullInput;
                }
                else if (ParseKeywordSequence(Keyword.RETURNS, Keyword.NULL, Keyword.ON, Keyword.NULL, Keyword.INPUT))
                {
                    EnsureNotSet(calledOnNull, "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT");
                    calledOnNull = FunctionCalledOnNull.ReturnsNullOnNullInput;
                }
                else if (ParseKeyword(Keyword.STRICT))
                {
                    EnsureNotSet(calledOnNull, "CALLED ON NULL INPUT | RETURNS NULL ON NULL INPUT | STRICT");
                    calledOnNull = FunctionCalledOnNull.Strict;
                }
                else if (ParseKeyword(Keyword.PARALLEL))
                {
                    EnsureNotSet(parallel, "PARALLEL { UNSAFE | RESTRICTED | SAFE }");
                    if (ParseKeyword(Keyword.UNSAFE))
                    {
                        parallel = FunctionParallel.Unsafe;
                    }
                    else if (ParseKeyword(Keyword.RESTRICTED))
                    {
                        parallel = FunctionParallel.Restricted;

                    }
                    else if (ParseKeyword(Keyword.SAFE))
                    {
                        parallel = FunctionParallel.Safe;
                    }
                    else
                    {
                        throw Expected("one of UNSAFE | RESTRICTED | SAFE", PeekToken());
                    }
                }
                else if (ParseKeyword(Keyword.RETURN))
                {
                    EnsureNotSet(functionBody, "RETURN");
                    functionBody = new CreateFunctionBody.Return(ParseExpr());
                }
                else
                {
                    break;
                }
            }

            return new CreateFunction(name)
            {
                OrReplace = orReplace,
                Temporary = temporary,
                Args = args,
                ReturnType = returnType,
                Behavior = behavior,
                CalledOnNull = calledOnNull,
                Parallel = parallel,
                Language = language,
                FunctionBody = functionBody,
            };
        }

        Statement ParseBigqueryCreateFunction()
        {
            var ifNotExists = ParseIfNotExists();
            var name = ParseObjectName();

            var args = ExpectParens(() =>
            {
                return ParseCommaSeparated(() =>
                {
                    var name = ParseIdentifier();
                    var dataType = ParseDataType();
                    return new OperateFunctionArg(ArgMode.None)
                    {
                        Name = name,
                        DataType = dataType
                    };
                });
            });

            var returnType = ParseKeyword(Keyword.RETURNS)
                ? ParseDataType()
                : null;

            var determinismSpecifier = ParseKeyword(Keyword.DETERMINISTIC)
                ? FunctionDeterminismSpecifier.Deterministic
                : FunctionDeterminismSpecifier.NotDeterministic;

            var language = ParseKeyword(Keyword.LANGUAGE)
                ? ParseIdentifier()
                : null;

            var remoteConnection = ParseKeywordSequence(Keyword.REMOTE, Keyword.WITH, Keyword.CONNECTION)
                ? ParseObjectName()
                : null;

            // `OPTIONS` may come before of after the function body but
            // may be specified at most once.
            var options = MaybeParseOptions(Keyword.OPTIONS);

            CreateFunctionBody? functionBody = null;

            if (remoteConnection == null)
            {
                ExpectKeyword(Keyword.AS);
                var expr = ParseExpr();
                if (options is null)
                {
                    options = MaybeParseOptions(Keyword.OPTIONS);
                    functionBody = new CreateFunctionBody.AsBeforeOptions(expr);
                }
                else
                {
                    functionBody = new CreateFunctionBody.AsAfterOptions(expr);
                }
            }

            return new CreateFunction(name)
            {
                OrReplace = orReplace,
                Temporary = temporary,
                IfNotExists = ifNotExists,
                Args = args,
                ReturnType = returnType,
                FunctionBody = functionBody,
                Language = language,
                DeterminismSpecifier = determinismSpecifier,
                Options = options,
                RemoteConnection = remoteConnection,
            };
        }

        void EnsureNotSet(object? field, string fieldName)
        {
            if (field is not null)
            {
                throw new ParserException($"{fieldName} specified more than once");
            }
        }
    }

    public Sequence<SqlOption>? MaybeParseOptions(Keyword keyword)
    {
        if (PeekToken() is Word w && w.Keyword == keyword)
        {
            return ParseOptions(keyword);
        }

        return null;
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

    public Expression ParseCreateFunctionBodyString()
    {
        var next = PeekToken();
        if (next is DollarQuotedString dq && _dialect is PostgreSqlDialect or GenericDialect)
        {
            NextToken();
            return new LiteralValue(new Value.DollarQuotedString(new DollarQuotedStringValue(dq.Value)));
        }

        return new LiteralValue(new Value.SingleQuotedString(ParseLiteralString()));
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

        if (ConsumeToken<Tokens.Assignment>() || ConsumeToken<RightArrow>())
        {
            defaultExpression = ParseExpr();
        }

        return new MacroArg(name, defaultExpression);
    }

    public Ast.CreateTable ParseCreateExternalTable(bool orReplace)
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

        return new Ast.CreateTable(tableName, columns)
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

        var ifNotExists = _dialect is BigQueryDialect or SQLiteDialect or GenericDialect && ParseIfNotExists();

        var name = ParseObjectName(_dialect is BigQueryDialect);
        var columns = ParseViewColumns();
        CreateTableOptions options = new CreateTableOptions.None();
        var withOptions = ParseOptions(Keyword.WITH);

        if (withOptions.SafeAny())
        {
            options = new CreateTableOptions.With(withOptions);
        }

        var clusterBy = ParseInit(ParseKeyword(Keyword.CLUSTER), () =>
        {
            ExpectKeyword(Keyword.BY);
            return ParseParenthesizedColumnList(IsOptional.Optional, false);
        });

        if (_dialect is BigQueryDialect or GenericDialect)
        {
            if (PeekToken() is Word { Keyword: Keyword.OPTIONS })
            {
                var opts = ParseOptions(Keyword.OPTIONS);
                if (opts.SafeAny())
                {
                    options = new CreateTableOptions.Options(opts);
                }
            }
        }

        var to = (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.TO))
            ? ParseObjectName()
            : null;

        string? comment = null;

        if (_dialect is SnowflakeDialect or GenericDialect && ParseKeyword(Keyword.COMMENT))
        {
            ExpectToken<Equal>();
            var next = NextToken();
            if (next is SingleQuotedString s)
            {
                comment = s.Value;
            }
            else
            {
                throw Expected("string literal", next);
            }
        }

        ExpectKeyword(Keyword.AS);
        var query = ParseQuery();

        var withNoBinding = _dialect is RedshiftDialect or GenericDialect &&
                            ParseKeywordSequence(Keyword.WITH, Keyword.NO, Keyword.SCHEMA, Keyword.BINDING);

        return new CreateView(name, query)
        {
            Columns = columns,
            ClusterBy = clusterBy,
            Materialized = materialized,
            OrReplace = orReplace,
            Comment = comment,
            WithNoSchemaBinding = withNoBinding,
            IfNotExists = ifNotExists,
            Temporary = temporary,
            Options = options,
            To = to
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

        var temporary = _dialect is MySqlDialect or DuckDbDialect or GenericDialect &&
                        ParseKeyword(Keyword.TEMPORARY);

        var persistent = _dialect is DuckDbDialect && ParseKeyword(Keyword.PERSISTENT);

        ObjectType? objectType = null;
        if (ParseKeyword(Keyword.TABLE))
        {
            objectType = ObjectType.Table;
        }
        else if (ParseKeyword(Keyword.VIEW))
        {
            objectType = ObjectType.View;
        }
        else if (ParseKeyword(Keyword.INDEX))
        {
            objectType = ObjectType.Index;
        }
        if (ParseKeyword(Keyword.ROLE))
        {
            objectType = ObjectType.Role;
        }
        else if (ParseKeyword(Keyword.SCHEMA))
        {
            objectType = ObjectType.Schema;
        }
        else if (ParseKeyword(Keyword.SEQUENCE))
        {
            objectType = ObjectType.Sequence;
        }
        else if (ParseKeyword(Keyword.STAGE))
        {
            objectType = ObjectType.Stage;
        }
        else if (ParseKeyword(Keyword.FUNCTION))
        {
            return ParseDropFunction();
        }
        else if (ParseKeyword(Keyword.PROCEDURE))
        {
            return ParseDropProcedure();
        }
        else if (ParseKeyword(Keyword.SECRET))
        {
            return ParseDropSecret(temporary, persistent);
        }

        if (objectType == null)
        {
            throw Expected("TABLE, VIEW, INDEX, ROLE, SCHEMA, FUNCTION PROCEDURE, STAGE, or SEQUENCE after DROP", PeekToken());
        }

        // Many dialects support the non-standard `IF EXISTS` clause and allow
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
            ObjectType = objectType.Value,
            IfExists = ifExists,
            Cascade = cascade,
            Restrict = restrict,
            Purge = purge,
            Temporary = temporary
        };
    }

    private DropProcedure ParseDropProcedure()
    {
        var ifExists = ParseIfExists();
        var procDesc = ParseCommaSeparated(ParseDropFunctionDescription);
        var keyword = ParseOneOfKeywords(Keyword.CASCADE, Keyword.RESTRICT);

        ReferentialAction? option = keyword switch
        {
            Keyword.CASCADE => ReferentialAction.Cascade,
            Keyword.RESTRICT => ReferentialAction.Restrict,
            _ => null
        };

        return new DropProcedure(ifExists, procDesc, option);
    }

    public DropFunctionDesc ParseDropFunctionDescription()
    {
        var name = ParseObjectName();
        Sequence<OperateFunctionArg>? args = null;

        if (ConsumeToken<LeftParen>())
        {
            if (!ConsumeToken<RightParen>())
            {
                args = ParseCommaSeparated(ParseFunctionArg);
                ExpectToken<RightParen>();
            }
        }

        return new DropFunctionDesc(name, args);
    }

    private DropSecret ParseDropSecret(bool temporary, bool persistent)
    {
        var ifExists = ParseIfExists();
        var name = ParseIdentifier();
        Ident? storageSpecifier = null;
        if (ParseKeyword(Keyword.FROM))
        {
            storageSpecifier = ParseIdentifier();
        }

        bool? temp = (temporary, persistent) switch
        {
            (true, false) => true,
            (false, true) => false,
            (false, false) => null,
            _ => throw Expected("TEMPORARY or PERSISTENT", PeekToken())
        };

        return new DropSecret(ifExists, temp, name, storageSpecifier);
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
    public Statement.Declare ParseDeclare()
    {
        if (_dialect is BigQueryDialect)
        {
            return ParseBigQueryDeclare();
        }

        if (_dialect is SnowflakeDialect)
        {
            return ParseSnowflakeDeclare();
        }

        if (_dialect is MsSqlDialect)
        {
            return ParseMsSqlDeclare();
        }

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
        var declareType = DeclareType.Cursor;

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

        return new Statement.Declare([
            new([name], null, null, declareType)
            {
                Binary = binary,
                Sensitive = sensitive,
                Scroll = scroll,
                Hold = hold,
                ForQuery = query
            }
        ]);
    }

    public Statement.Declare ParseBigQueryDeclare()
    {
        var names = ParseCommaSeparated(ParseIdentifier);

        var token = PeekToken();
        DataType? dataType = token switch
        {
            Word { Keyword: Keyword.DEFAULT } => null,
            _ => ParseDataType()
        };

        Expression? expression = null;

        if (dataType != null)
        {
            if (ParseKeyword(Keyword.DEFAULT))
            {
                expression = ParseExpr();
            }
        }
        else
        {
            // If no variable type - default expression must be specified, per BQ docs.
            // i.e `DECLARE foo;` is invalid.
            ExpectKeyword(Keyword.DEFAULT);
            expression = ParseExpr();
        }

        DeclareAssignment? declaration = null;
        if (expression != null)
        {
            declaration = new DeclareAssignment.Default(expression);
        }

        return new Statement.Declare([new Declare(names, dataType, declaration, null)]);
    }

    public Statement.Declare ParseMsSqlDeclare()
    {
        var statements = new Sequence<Declare>();

        while (true)
        {
            var name = ParseIdentifier();
            if (!name.Value.StartsWith(Symbols.At))
            {
                throw Expected("Invalid MsSql variable declaration", PeekToken());
            }

            var token = PeekToken();
            DeclareType? declareType = null;
            DataType? dataType = null;

            if (token is Word w)
            {
                if (w.Keyword == Keyword.CURSOR)
                {
                    NextToken();
                    declareType = DeclareType.Cursor;
                }
                else if (w.Keyword == Keyword.AS)
                {
                    NextToken();
                    dataType = ParseDataType();
                }
                else
                {
                    dataType = ParseDataType();
                }
            }
            else
            {
                dataType = ParseDataType();
            }

            var assignment = ParseMsSqlVariableDeclarationExpression();

            statements.Add(new Declare([name], dataType, assignment, declareType));

            if (NextToken() is not Comma)
            {
                break;
            }
        }

        return new Statement.Declare(statements);
    }

    public Sequence<UnionField> ParseUnionTypeDef()
    {
        ExpectKeyword(Keyword.UNION);

        return ExpectParens(() =>
        {
            return ParseCommaSeparated(() =>
            {
                var identifier = ParseIdentifier();
                var fieldType = ParseDataType();
                return new UnionField(identifier, fieldType);
            });
        });
    }

    public DeclareAssignment? ParseMsSqlVariableDeclarationExpression()
    {
        if (PeekToken() is Equal)
        {
            NextToken();
            return new DeclareAssignment.MsSqlAssignment(ParseExpr());
        }

        return null;
    }

    public Statement.Declare ParseSnowflakeDeclare()
    {
        var statements = new Sequence<Declare>();

        while (true)
        {
            var name = ParseIdentifier();

            DeclareType? declareType = null;
            Query? forQuery = null;
            DeclareAssignment? assignedExpression = null;
            DataType? dataType = null;

            if (ParseKeyword(Keyword.CURSOR))
            {
                declareType = DeclareType.Cursor;

                ExpectKeyword(Keyword.FOR);

                switch (PeekToken())
                {
                    case Word { Keyword: Keyword.SELECT }:
                        forQuery = ParseQuery();
                        break;

                    default:
                        assignedExpression = new DeclareAssignment.For(ParseExpr());
                        break;
                }
            }
            else if (ParseKeyword(Keyword.RESULTSET))
            {
                if (!PeekTokenIs<SemiColon>())
                {
                    assignedExpression = ParseSnowflakeVariableDeclarationExpression();
                }

                declareType = DeclareType.ResultSet;
            }
            else if (ParseKeyword(Keyword.EXCEPTION))
            {
                if (PeekTokenIs<LeftParen>())
                {
                    assignedExpression = new DeclareAssignment.DeclareExpression((ParseExpr()));
                }
                declareType = DeclareType.Exception;
            }
            else
            {
                // Without an explicit keyword, the only valid option is variable declaration.
                assignedExpression = ParseSnowflakeVariableDeclarationExpression();
                if (assignedExpression == null)
                {
                    if (PeekTokenIs<Word>())
                    {
                        dataType = ParseDataType();

                        assignedExpression = ParseSnowflakeVariableDeclarationExpression();
                    }
                }
            }

            var statement = new Declare([name], dataType, assignedExpression, declareType)
            {
                ForQuery = forQuery
            };

            statements.Add(statement);
            if (ConsumeToken<SemiColon>())
            {
                var token = PeekToken();
                if (token is Word w)
                {
                    if (System.Array.IndexOf(Keywords.All, w.Value.ToUpperInvariant()) > -1)
                    {
                        // Not a keyword -start of a new declaration.
                        continue;
                    }

                    // Put back the semicolon, this is the end of the DECLARE statement.
                    PrevToken();
                }
            }

            break;
        }

        return new Statement.Declare(statements);
    }

    public DeclareAssignment? ParseSnowflakeVariableDeclarationExpression()
    {
        return PeekToken() switch
        {
            Word { Keyword: Keyword.DEFAULT } => ParseDefault(),
            Tokens.Assignment => ParseDeclareAssignment(),
            _ => null
        };

        DeclareAssignment ParseDefault()
        {
            NextToken();
            return new DeclareAssignment.Default(ParseExpr());
        }

        DeclareAssignment ParseDeclareAssignment()
        {
            NextToken();
            return new DeclareAssignment.Assignment(ParseExpr());
        }
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

    public Statement ParseCreateExtension()
    {
        var ifNot = ParseIfExists();
        var name = ParseIdentifier();

        Ident? schema = null;
        Ident? version = null;
        var cascade = false;

        if (ParseKeyword(Keyword.WITH))
        {
            if (ParseKeyword(Keyword.SCHEMA))
            {
                schema = ParseIdentifier();
            }

            if (ParseKeyword(Keyword.VERSION))
            {
                version = ParseIdentifier();
            }

            cascade = ParseKeyword(Keyword.CASCADE);
        }

        return new CreateExtension(name, ifNot, cascade, schema, version);
    }

    public Ast.CreateIndex ParseCreateIndex(bool unique)
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

        return new Ast.CreateIndex(indexName, tableName)
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
            switch (ParseOneOfKeywords(Keyword.ROW, Keyword.STORED, Keyword.LOCATION, Keyword.WITH))
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

                case Keyword.WITH:
                    PrevToken();
                    var properties = ParseOptionsWithKeywords(Keyword.WITH, Keyword.SERDEPROPERTIES);
                    if (properties.SafeAny())
                    {
                        hiveFormat ??= new HiveFormat();
                        hiveFormat.SerdeProperties = properties;
                    }
                    else
                    {
                        loop = false;
                    }

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

        Sequence<HiveRowDelimiter>? rowDelimiters = null;

        var loop = true;
        while (loop)
        {
            var keyword = ParseOneOfKeywords(Keyword.FIELDS, Keyword.COLLECTION, Keyword.MAP, Keyword.LINES, Keyword.NULL);

            switch (keyword)
            {
                case Keyword.FIELDS:
                    if (ParseKeywordSequence(Keyword.TERMINATED, Keyword.BY))
                    {
                        rowDelimiters = new Sequence<HiveRowDelimiter>
                        {
                            new (HiveDelimiter.FieldsTerminatedBy, ParseIdentifier())
                        };

                        if (ParseKeywordSequence(Keyword.ESCAPED, Keyword.BY))
                        {
                            rowDelimiters.Add(new HiveRowDelimiter(HiveDelimiter.FieldsEscapedBy, ParseIdentifier()));
                        }
                    }
                    else
                    {
                        loop = false;
                    }
                    break;
                case Keyword.COLLECTION:
                    if (ParseKeywordSequence(Keyword.ITEMS, Keyword.TERMINATED, Keyword.BY))
                    {
                        rowDelimiters ??= [];
                        rowDelimiters.Add(new HiveRowDelimiter(HiveDelimiter.CollectionItemsTerminatedBy, ParseIdentifier()));
                    }
                    else
                    {
                        loop = false;
                    }
                    break;
                case Keyword.MAP:
                    if (ParseKeywordSequence(Keyword.KEYS, Keyword.TERMINATED, Keyword.BY))
                    {
                        rowDelimiters ??= [];
                        rowDelimiters.Add(new HiveRowDelimiter(HiveDelimiter.MapKeysTerminatedBy, ParseIdentifier()));
                    }
                    else
                    {
                        loop = false;
                    }
                    break;
                case Keyword.LINES:
                    if (ParseKeywordSequence(Keyword.TERMINATED, Keyword.BY))
                    {
                        rowDelimiters ??= [];
                        rowDelimiters.Add(new HiveRowDelimiter(HiveDelimiter.LinesTerminatedBy, ParseIdentifier()));
                    }
                    else
                    {
                        loop = false;
                    }
                    break;
                case Keyword.NULL:
                    if (ParseKeywordSequence(Keyword.DEFINED, Keyword.AS))
                    {
                        rowDelimiters ??= [];
                        rowDelimiters.Add(new HiveRowDelimiter(HiveDelimiter.NullDefinedAs, ParseIdentifier()));
                    }
                    else
                    {
                        loop = false;
                    }
                    break;

                default:
                    loop = false;
                    break;
            }
        }

        return new HiveRowFormat.Delimited(rowDelimiters);
    }

    public Ast.CreateTable ParseCreateTable(bool orReplace, bool temporary, bool? global, bool transient)
    {
        var allowUnquotedHyphen = _dialect is BigQueryDialect;
        var ifNotExists = ParseIfNotExists();
        var tableName = ParseObjectName(allowUnquotedHyphen);

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

        var like = ParseInit<ObjectName?>(ParseKeyword(Keyword.LIKE) || ParseKeyword(Keyword.ILIKE), () => ParseObjectName(allowUnquotedHyphen));

        var clone = ParseInit<ObjectName?>(ParseKeyword(Keyword.CLONE), () => ParseObjectName(allowUnquotedHyphen));

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
                var name = w.Value;

                Sequence<Ident>? parameters = null;
                var peeked = PeekToken();
                if (peeked is LeftParen)
                {
                    parameters = ExpectParens(() => ParseCommaSeparated(ParseIdentifier));
                }
                return new TableEngine(name, parameters);
            }

            throw Expected("identifier", token);
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

        Expression? primaryKey = null;

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY))
        {
            primaryKey = ParseExpr();
        }

        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () =>
        {
            OneOrManyWithParens<Expression> orderExpression;

            if (ConsumeToken<LeftParen>())
            {
                Sequence<Expression>? cols = null;

                if (PeekToken() is not RightParen)
                {
                    cols = ParseCommaSeparated(ParseExpr);
                }

                ExpectRightParen();
                orderExpression = new OneOrManyWithParens<Expression>.Many(cols);
            }
            else
            {
                orderExpression = new OneOrManyWithParens<Expression>.One(ParseExpr());
            }

            return orderExpression;

        });

        var createTableConfig = ParseOptionalCreateTableConfig();

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

        var comment = ParseInit(ParseKeyword(Keyword.COMMENT), () =>
        {
            ConsumeToken<Equal>();
            var next = NextToken();
            if (next is SingleQuotedString str)
            {
                return new CommentDef.WithoutEq(str.Value);
            }

            throw Expected("Comment", PeekToken());
        });

        // Parse optional `AS ( query )`
        var query = ParseInit<Query>(ParseKeyword(Keyword.AS), () => ParseQuery());

        return new Ast.CreateTable(tableName, columns)
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
            PrimaryKey = primaryKey,
            OrderBy = orderBy,
            DefaultCharset = defaultCharset,
            Collation = collation,
            OnCommit = onCommit,
            OnCluster = onCluster,
            Strict = strict,
            AutoIncrementOffset = autoIncrementOffset,
            PartitionBy = createTableConfig.PartitionBy,
            ClusterBy = createTableConfig.ClusterBy,
            Options = createTableConfig.Options
        };
    }

    public CreateTableConfiguration ParseOptionalCreateTableConfig()
    {
        Expression? partitionBy = null;
        WrappedCollection<Ident>? clusterBy = null;
        Sequence<SqlOption>? options = null;

        if (_dialect is BigQueryDialect or PostgreSqlDialect && ParseKeywordSequence(Keyword.PARTITION, Keyword.BY))
        {
            partitionBy = ParseExpr();
        }

        if (_dialect is BigQueryDialect or PostgreSqlDialect)
        {
            if (ParseKeywordSequence(Keyword.CLUSTER, Keyword.BY))
            {
                clusterBy = new WrappedCollection<Ident>.NoWrapping(ParseCommaSeparated(ParseIdentifier));
            }

            if (PeekToken() is Word { Keyword: Keyword.OPTIONS })
            {
                options = ParseOptions(Keyword.OPTIONS);
            }
        }

        return new CreateTableConfiguration(partitionBy, clusterBy, options);
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
            if (ParseAnyOptionalTableConstraints(constraint => constraints.Add(constraint))) {
                // work has been done already
            }
            else if ((PeekToken() is Word) || (PeekToken() is SingleQuotedString)) {
                columns.Add(ParseColumnDef());
            }
            else {
                ThrowExpected("column name or constraint definition", PeekToken());
            }

            var commaFound = ConsumeToken<Comma>();

            var rightParen = PeekToken() is RightParen;

            if (!commaFound && !rightParen)
            {
                ThrowExpected("',' or ')' after column definition", PeekToken());
            }

            if (rightParen && (!commaFound || _options.TrailingCommas))
            {
                ConsumeToken<RightParen>();
                break;
            }
        }

        return (columns, constraints);
    }

    public ColumnDef ParseColumnDef()
    {
        var name = ParseIdentifier();
        var dataType = IsUnspecified() ? new DataType.Unspecified() : ParseDataType();
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
            else if ((opt = ParseOptionalColumnOption()) is not null)
            {
                options ??= new Sequence<ColumnOptionDef>();
                options.Add(new ColumnOptionDef(opt));
            }
            else if (_dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.COLLATE))
            {
                collation = ParseObjectName();
            }
            else
            {
                break;
            }
        }

        return new ColumnDef(name, dataType, collation, options);

        bool IsUnspecified()
        {
            if (_dialect is not SQLiteDialect)
            {
                return false;
            }

            if (PeekToken() is Word w)
            {
                return w.Keyword
                    is Keyword.CONSTRAINT
                    or Keyword.PRIMARY
                    or Keyword.NOT
                    or Keyword.UNIQUE
                    or Keyword.CHECK
                    or Keyword.DEFAULT
                    or Keyword.COLLATE
                    or Keyword.REFERENCES
                    or Keyword.GENERATED
                    or Keyword.AS;
            }

            return true;
        }
    }

    public Keyword? ParseColumnConflictClause () {
        if (!ParseKeywordSequence(Keyword.ON, Keyword.CONFLICT)) return null;
        return ParseOneOfKeywords(Keyword.ROLLBACK, Keyword.ABORT, Keyword.FAIL, Keyword.IGNORE, Keyword.REPLACE);
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

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.MATERIALIZED))
        {
            return new ColumnOption.Materialized(ParseExpr());
        }

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.ALIAS))
        {
            return new ColumnOption.Alias(ParseExpr());
        }

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.EPHEMERAL))
        {
            var next = PeekToken();

            if (next is Comma or RightParen)
            {
                return new ColumnOption.Ephemeral();
            }

            return new ColumnOption.Ephemeral(ParseExpr());

        }

        if (ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY))
        {
            var order = _dialect is SQLiteDialect ? ParseOneOfKeywords([Keyword.ASC, Keyword.DESC]) : Keyword.undefined;
            var conflict = _dialect is SQLiteDialect ? ParseColumnConflictClause() : Keyword.undefined;
            var autoincrement = _dialect is SQLiteDialect && ParseKeyword(Keyword.AUTOINCREMENT);
            var characteristics = ParseConstraintCharacteristics();
            return new ColumnOption.Unique(true) {
                Characteristics = characteristics,
                Order = order != Keyword.undefined ? order : null,
                Conflict = conflict != Keyword.undefined ? conflict : null,
                Autoincrement = autoincrement
            };
        }

        if (ParseKeyword(Keyword.UNIQUE))
        {
            var conflict = _dialect is SQLiteDialect ? ParseColumnConflictClause() : null;
            var characteristics = ParseConstraintCharacteristics();
            return new ColumnOption.Unique(false)
            {
                Characteristics = characteristics,
                Conflict = conflict
            };
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

            var characteristics = ParseConstraintCharacteristics();

            return new ColumnOption.ForeignKey(
                foreignTable,
                referredColumns.Any() ? referredColumns : null,
                onDelete,
                onUpdate)
            {
                Characteristics = characteristics
            };
        }

        if (ParseKeyword(Keyword.CHECK))
        {
            var expr = ExpectParens(ParseExpr);

            return new ColumnOption.Check(expr);
        }

        if (_dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.AUTO_INCREMENT))
        {
            // Support AUTO_INCREMENT for MySQL
            return new ColumnOption.DialectSpecific(new[] { new Word("AUTO_INCREMENT") });
        }

        if (_dialect is SQLiteDialect or GenericDialect && ParseKeyword(Keyword.AUTOINCREMENT))
        {
            // Support AUTOINCREMENT for SQLite
            return new ColumnOption.DialectSpecific(new[] { new Word("AUTOINCREMENT") });
        }

        if ( _dialect is MySqlDialect or GenericDialect && ParseKeywordSequence(Keyword.ON, Keyword.UPDATE))
        {
            return new ColumnOption.OnUpdate(ParseExpr());
        }

        if (ParseKeyword(Keyword.GENERATED))
        {
            return ParseOptionalColumnOptionGenerated();
        }

        if (_dialect is BigQueryDialect or GenericDialect && ParseKeyword(Keyword.OPTIONS))
        {
            PrevToken();
            return new ColumnOption.Options(ParseOptions(Keyword.OPTIONS));
        }

        if (_dialect is MySqlDialect or SQLiteDialect or DuckDbDialect or GenericDialect && ParseKeyword(Keyword.AS))
        {
            return ParseOptionalColumnOptionAs();
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
            catch (ParserException)
            {
            }

            return new ColumnOption.Generated(GeneratedAs.Always, true, sequenceOptions);
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
            catch (ParserException)
            {
            }

            return new ColumnOption.Generated(GeneratedAs.ByDefault, true, sequenceOptions);
        }

        if (ParseKeywordSequence(Keyword.ALWAYS, Keyword.AS))
        {
            try
            {
                var expr = ExpectParens(ParseExpr);
                GeneratedAs genAs;
                GeneratedExpressionMode? expressionMode = null;

                if (ParseKeyword(Keyword.STORED))
                {
                    genAs = GeneratedAs.ExpStored;
                    expressionMode = GeneratedExpressionMode.Sorted;
                }
                else if (_dialect is PostgreSqlDialect)
                {
                    throw Expected("SORTED", PeekToken());
                }
                else if (ParseKeyword(Keyword.VIRTUAL))
                {
                    genAs = GeneratedAs.Always;
                    expressionMode = GeneratedExpressionMode.Virtual;
                }
                else
                {
                    genAs = GeneratedAs.Always;
                }

                return new ColumnOption.Generated(genAs, true, GenerationExpr: expr,
                    GenerationExpressionMode: expressionMode);
            }
            catch (ParserException)
            {
                return null;
            }
        }

        return null;
    }

    public ColumnOption ParseOptionalColumnOptionAs()
    {
        var expr = ExpectParens(ParseExpr);

        GeneratedAs genAs;
        GeneratedExpressionMode? expressionMode = null;

        if (ParseKeyword(Keyword.STORED))
        {
            genAs = GeneratedAs.ExpStored;
            expressionMode = GeneratedExpressionMode.Sorted;
        }
        else if (ParseKeyword(Keyword.VIRTUAL))
        {
            genAs = GeneratedAs.Always;
            expressionMode = GeneratedExpressionMode.Virtual;
        }
        else
        {
            genAs = GeneratedAs.Always;
        }

        return new ColumnOption.Generated(genAs, false, null, expr, expressionMode);
    }

    public ConstraintCharacteristics? ParseConstraintCharacteristics()
    {
        var cc = new ConstraintCharacteristics();

        while (true)
        {
            if (cc.Deferrable == null && ParseKeywordSequence(Keyword.NOT, Keyword.DEFERRABLE))
            {
                cc.Deferrable = false;
            }
            else if (cc.Deferrable == null && ParseKeyword(Keyword.DEFERRABLE))
            {
                cc.Deferrable = true;
            }
            else if (cc.Initially == null && ParseKeyword(Keyword.INITIALLY))
            {
                if (ParseKeyword(Keyword.DEFERRED))
                {
                    cc.Initially = DeferrableInitial.Deferred;
                }
                else if (ParseKeyword(Keyword.IMMEDIATE))
                {
                    cc.Initially = DeferrableInitial.Immediate;
                }
                else
                {
                    throw Expected("one of DEFERRED or IMMEDIATE", PeekToken());
                }
            }
            else if (cc.Enforced == null && ParseKeyword(Keyword.ENFORCED))
            {
                cc.Enforced = true;
            }
            else if (cc.Enforced == null && ParseKeywordSequence(Keyword.NOT, Keyword.ENFORCED))
            {
                cc.Enforced = false;
            }
            else
            {
                break;
            }
        }

        if (cc.Deferrable != null || cc.Initially != null || cc.Enforced != null)
        {
            return cc;
        }
        else
        {
            return null;
        }
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

    bool ParseAnyOptionalTableConstraints(Action<TableConstraint> action) {
        bool any = false;
        while (true) {
            var constraint = ParseOptionalTableConstraint(any);
            if (constraint == null) return any;
            action(constraint);
            any = true;
        }
    }

    public TableConstraint? ParseOptionalTableConstraint(bool isSubsequentConstraint = false)
    {
        var name = isSubsequentConstraint ? null : ParseInit(ParseKeyword(Keyword.CONSTRAINT), ParseIdentifier);

        var token = NextToken();

        return token switch
        {
            Word { Keyword: Keyword.PRIMARY or Keyword.UNIQUE } w => ParsePrimary(w),
            Word { Keyword: Keyword.FOREIGN } => ParseForeign(),
            Word { Keyword: Keyword.CHECK } => ParseCheck(),
            Word { Keyword: Keyword.INDEX or Keyword.KEY } w when _dialect is GenericDialect or MySqlDialect =>
                ParseIndex(w),
            Word { Keyword: Keyword.FULLTEXT or Keyword.SPATIAL } w when _dialect is GenericDialect or MySqlDialect =>
                ParseText(w),
            _ => ParseDefault()
        };

        TableConstraint ParsePrimary(Word word)
        {
            var isPrimary = word.Keyword == Keyword.PRIMARY;
            ParseKeyword(Keyword.KEY);

            // Optional constraint name
            var identName = MaybeParse(ParseIdentifier) ?? name;

            var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
            var characteristics = ParseConstraintCharacteristics();

            return new TableConstraint.Unique(columns)
            {
                Name = identName,
                IsPrimaryKey = isPrimary,
                Characteristics = characteristics
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

            var characteristics = ParseConstraintCharacteristics();

            return new TableConstraint.ForeignKey(foreignTable, columns)
            {
                Name = name,
                ReferredColumns = referredColumns,
                OnDelete = onDelete,
                OnUpdate = onUpdate,
                Characteristics = characteristics
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

    public Sequence<SqlOption> ParseOptionsWithKeywords(params Keyword[] keywords)
    {
        if (ParseKeywordSequence(keywords))
        {
            return ExpectParens(() => ParseCommaSeparated(ParseSqlOption));
        }

        return [];
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
        var value = ParseExpr();
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

                    HiveSetLocation? location = null;

                    if (ParseKeyword(Keyword.LOCATION))
                    {
                        location = new HiveSetLocation(false, ParseIdentifier());
                    }
                    else if (ParseKeywordSequence(Keyword.SET, Keyword.LOCATION))
                    {
                        location = new HiveSetLocation(true, ParseIdentifier());
                    }

                    return new AlterTable(tableName, ifExists, only, operations, location);
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

    public Sequence<ViewColumnDef>? ParseViewColumns()
    {
        if (ConsumeToken<LeftParen>())
        {
            if (PeekToken() is RightParen)
            {
                NextToken();
                return null;
            }

            var cols = ParseCommaSeparated(ParseViewColumn);
            ExpectRightParen();

            return cols;
        }

        return null;
    }

    public ViewColumnDef ParseViewColumn()
    {
        var name = ParseIdentifier();
        Sequence<SqlOption>? options = null;

        if (_dialect is BigQueryDialect or GenericDialect && ParseKeyword(Keyword.OPTIONS))
        {
            PrevToken();
            options = ParseOptions(Keyword.OPTIONS);
        }

        DataType? dataType = null;
        if (_dialect is ClickHouseDialect)
        {
            dataType = ParseDataType();
        }

        return new ViewColumnDef(name, dataType, options);
    }

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
                var columnPosition = ParseColumnPosition();
                return new AddColumn(columnKeyword, ine, columnDef, columnPosition);
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
        else if (ParseKeyword(Keyword.DISABLE))
        {
            if (ParseKeywordSequence(Keyword.ROW, Keyword.LEVEL, Keyword.SECURITY))
            {
                operation = new DisableRowLevelSecurity();
            }
            else if (ParseKeyword(Keyword.RULE))
            {
                var name = ParseIdentifier();
                operation = new DisableRule(name);
            }
            else if (ParseKeyword(Keyword.TRIGGER))
            {
                var name = ParseIdentifier();
                operation = new DisableTrigger(name);
            }
            else
            {
                throw Expected("ROW LEVEL SECURITY, RULE, or TRIGGER after DISABLE", PeekToken());
            }
        }
        else if (ParseKeyword(Keyword.ENABLE))
        {
            if (ParseKeywordSequence(Keyword.ALWAYS, Keyword.RULE))
            {
                var name = ParseIdentifier();
                operation = new EnableAlwaysRule(name);
            }
            else if (ParseKeywordSequence(Keyword.ALWAYS, Keyword.TRIGGER))
            {
                var name = ParseIdentifier();
                operation = new EnableAlwaysTrigger(name);
            }
            else if (ParseKeywordSequence(Keyword.ROW, Keyword.LEVEL, Keyword.SECURITY))
            {
                operation = new EnableRowLevelSecurity();
            }
            else if (ParseKeywordSequence(Keyword.REPLICA, Keyword.RULE))
            {
                var name = ParseIdentifier();
                operation = new EnableReplicaRule(name);
            }
            else if (ParseKeywordSequence(Keyword.REPLICA, Keyword.TRIGGER))
            {
                var name = ParseIdentifier();
                operation = new EnableReplicaTrigger(name);
            }
            else if (ParseKeyword(Keyword.RULE))
            {
                var name = ParseIdentifier();
                operation = new EnableRule(name);
            }
            else if (ParseKeyword(Keyword.TRIGGER))
            {
                var name = ParseIdentifier();
                operation = new EnableTrigger(name);
            }
            else
            {
                throw Expected("ALWAYS, REPLICA, ROW LEVEL SECURITY, RULE, or TRIGGER after ENABLE", PeekToken());
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
                var cascade = ParseKeyword(Keyword.CASCADE);
                operation = new DropConstraint(name, ifExists, cascade);
            }
            else if (_dialect is MySqlDialect or GenericDialect && ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY))
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

            var columnPosition = ParseColumnPosition();
            operation = new ChangeColumn(oldName, newName, dataType, options, columnPosition);
        }
        else if (ParseKeyword(Keyword.MODIFY))
        {
            _ = ParseKeyword(Keyword.COLUMN);

            var columnName = ParseIdentifier();
            var dataType = ParseDataType();
            var options = new Sequence<ColumnOption>();
            while (ParseOptionalColumnOption() is { } option)
            {
                options.Add(option);
            }

            var position = ParseColumnPosition();
            return new ModifyColumn(columnName, dataType, options, position);
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
            else if (ParseKeywordSequence(Keyword.ADD, Keyword.GENERATED))
            {
                GeneratedAs? genAs = ParseKeyword(Keyword.ALWAYS) ? GeneratedAs.Always :
                    ParseKeywordSequence(Keyword.BY, Keyword.DEFAULT) ? GeneratedAs.ByDefault :
                    null;

                ExpectKeywords(Keyword.AS, Keyword.IDENTITY);

                Sequence<SequenceOptions>? options = null;

                if (PeekToken() is LeftParen)
                {
                    options = ExpectParens(ParseCreateSequenceOptions);
                }

                op = new AlterColumnOperation.AddGenerated(genAs, options);
            }
            else
            {
                if (_dialect is PostgreSqlDialect)
                {
                    throw Expected("SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE, or ADD GENERATED after ALTER COLUMN", PeekToken());
                }
                else
                {
                    throw Expected("SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN", PeekToken());
                }
            }

            operation = new AlterColumn(columnName, op);
        }
        else if (ParseKeyword(Keyword.SWAP))
        {
            ExpectKeyword(Keyword.WITH);
            operation = new SwapWith(ParseObjectName());
        }
        else if (_dialect is PostgreSqlDialect or GenericDialect && ParseKeywordSequence(Keyword.OWNER, Keyword.TO))
        {
            var keyword = ParseOneOfKeywords(Keyword.CURRENT_USER, Keyword.CURRENT_ROLE, Keyword.SESSION_USER);
            Owner newOwner = keyword switch
            {
                Keyword.CURRENT_USER => new Owner.CurrentUser(),
                Keyword.CURRENT_ROLE => new Owner.CurrentRole(),
                Keyword.SESSION_USER => new Owner.SessionUser(),
                Keyword.undefined => new Owner.Identity(ParseIdentifier()),
                _ => throw new ParserException("Unable to parse alter table operation")
            };

            return new OwnerTo(newOwner);
        }
        else
        {
            throw Expected("ADD, RENAME, PARTITION, SWAP or DROP after ALTER TABLE", PeekToken());
        }

        return operation;
    }

    private MySqlColumnPosition? ParseColumnPosition()
    {
        if (_dialect is MySqlDialect or GenericDialect)
        {
            if (ParseKeyword(Keyword.FIRST))
            {
                return new MySqlColumnPosition.First();
            }
            else if (ParseKeyword(Keyword.AFTER))
            {
                return new MySqlColumnPosition.After(ParseIdentifier());
            }

            return null;
        }

        return null;
    }

    public Statement ParseCall()
    {
        var name = ParseObjectName();

        if (PeekToken() is not LeftParen)
        {
            return new Call(new Function(name));
        }

        var fnExpression = ParseFunction(name);
        if (fnExpression is Function fn)
        {
            return new Call(fn);
        }

        throw Expected("a simple procedure call", PeekToken());
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

                    //// Case when Snowflake Semi-structured data like key:value
                    //Keyword.undefined or Keyword.LOCATION or Keyword.TYPE or Keyword.DATE or Keyword.START or Keyword.END
                    //    when _dialect is SnowflakeDialect or GenericDialect
                    //        => new Value.UnQuotedString(w.Value),

                    _ => throw Expected("a concrete value", PeekToken())
                };

            case Number n:
                return ParseNumeric(n);

            case SingleQuotedString s:
                return new Value.SingleQuotedString(s.Value);

            case DoubleQuotedString s:
                return new Value.DoubleQuotedString(s.Value);

            case SingleQuotedByteStringLiteral s:
                return new Value.SingleQuotedByteStringLiteral(s.Value);

            case DoubleQuotedByteStringLiteral s:
                return new Value.DoubleQuotedByteStringLiteral(s.Value);

            case TripleSingleQuotedString s:
                return new Value.TripleSingleQuotedString(s.Value);

            case TripleDoubleQuotedString s:
                return new Value.TripleDoubleQuotedString(s.Value);

            case DollarQuotedString s:
                return new Value.DollarQuotedString(new DollarQuotedStringValue(s.Value, s.Tag));

            case TripleSingleQuotedByteStringLiteral s:
                return new Value.TripleSingleQuotedByteStringLiteral(s.Value);

            case TripleDoubleQuotedByteStringLiteral s:
                return new Value.TripleDoubleQuotedByteStringLiteral(s.Value);

            case SingleQuotedRawStringLiteral s:
                return new Value.SingleQuotedRawStringLiteral(s.Value);

            case DoubleQuotedRawStringLiteral s:
                return new Value.DoubleQuotedRawStringLiteral(s.Value);

            case TripleSingleQuotedRawStringLiteral s:
                return new Value.TripleSingleQuotedRawStringLiteral(s.Value);

            case TripleDoubleQuotedRawStringLiteral s:
                return new Value.TripleDoubleQuotedRawStringLiteral(s.Value);

            case NationalStringLiteral n:
                return new Value.NationalStringLiteral(n.Value);

            case EscapedStringLiteral e:
                return new Value.EscapedStringLiteral(e.Value);

            case UnicodeStringLiteral u:
                return new Value.UnicodeStringLiteral(u.Value);

            case HexStringLiteral h:
                return new Value.HexStringLiteral(h.Value);

            case Placeholder p:
                return new Value.Placeholder(p.Value);

            case Colon c:
                return ParsePlaceholder(c);

            case AtSign a:
                return ParsePlaceholder(a);

            default:
                throw Expected("a value", PeekToken());
        }

        Value ParsePlaceholder(Token tok)
        {
            // Not calling self.parse_identifier()? because only in placeholder we want to check numbers
            // as identifiers. This because snowflake allows numbers as placeholders
            var nextToken = NextToken();
            var ident = nextToken switch
            {
                Word w => w.ToIdent(),
                Number n => new Ident(n.Value),
                _ => throw Expected("placeholder", nextToken)
            };

            var placeholder = tok + ident.Value;
            return new Value.Placeholder(placeholder);
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
    /// Parse a SQL data type (in the context of a CREATE TABLE statement for example)
    /// </summary>
    /// <returns></returns>
    public (DataType, bool) ParseDataTypeHelper()
    {
        var token = NextToken();
        var trailingBracket = false;

        var data = token switch
        {
            Word { Keyword: Keyword.BOOLEAN } => new DataType.Boolean(),
            Word { Keyword: Keyword.BOOL } => new DataType.Bool(),
            Word { Keyword: Keyword.FLOAT } => new DataType.Float(ParseOptionalPrecision()),
            Word { Keyword: Keyword.REAL } => new DataType.Real(),
            Word { Keyword: Keyword.FLOAT4 } => new DataType.Float4(),
            Word { Keyword: Keyword.FLOAT32 } => new DataType.Float32(),
            Word { Keyword: Keyword.FLOAT64 } => new DataType.Float64(),
            Word { Keyword: Keyword.FLOAT8 } => new DataType.Float8(),
            Word { Keyword: Keyword.DOUBLE } => ParseDouble(),
            Word { Keyword: Keyword.TINYINT } => ParseTinyInt(),
            Word { Keyword: Keyword.INT2 } => ParseInt2(),
            Word { Keyword: Keyword.SMALLINT } => ParseSmallInt(),
            Word { Keyword: Keyword.MEDIUMINT } => ParseMediumInt(),
            Word { Keyword: Keyword.INT } => ParseInt(),
            Word { Keyword: Keyword.INT4 } => ParseInt4(),
            Word { Keyword: Keyword.INT8 } => ParseInt8(),
            Word { Keyword: Keyword.INT16 } => new DataType.Int16(),
            Word { Keyword: Keyword.INT32 } => new DataType.Int32(),
            Word { Keyword: Keyword.INT64 } => new DataType.Int64(),
            Word { Keyword: Keyword.INT128 } => new DataType.Int128(),
            Word { Keyword: Keyword.INT256 } => new DataType.Int256(),

            Word { Keyword: Keyword.INTEGER } => ParseInteger(),
            Word { Keyword: Keyword.BIGINT } => ParseBigInt(),

            Word { Keyword: Keyword.UINT8 } => new DataType.UInt8(),
            Word { Keyword: Keyword.UINT16 } => new DataType.UInt16(),
            Word { Keyword: Keyword.UINT32 } => new DataType.UInt32(),
            Word { Keyword: Keyword.UINT64 } => new DataType.UInt64(),
            Word { Keyword: Keyword.UINT128 } => new DataType.UInt128(),
            Word { Keyword: Keyword.UINT256 } => new DataType.UInt256(),

            Word { Keyword: Keyword.VARCHAR } => new DataType.Varchar(ParseOptionalCharacterLength()),
            Word { Keyword: Keyword.NVARCHAR } => new DataType.Nvarchar(ParseOptionalCharacterLength()),
            Word { Keyword: Keyword.CHARACTER } => ParseCharacter(),
            Word { Keyword: Keyword.CHAR } => ParseChar(),
            Word { Keyword: Keyword.CLOB } => new DataType.Clob(ParseOptionalPrecision()),
            Word { Keyword: Keyword.BINARY } => new DataType.Binary(ParseOptionalPrecision()),
            Word { Keyword: Keyword.VARBINARY } => new DataType.Varbinary(ParseOptionalPrecision()),
            Word { Keyword: Keyword.BLOB } => new DataType.Blob(ParseOptionalPrecision()),
            Word { Keyword: Keyword.BYTES } => new DataType.Bytes(ParseOptionalPrecision()),
            Word { Keyword: Keyword.UUID } => new DataType.Uuid(),

            Word { Keyword: Keyword.DATE } => new DataType.Date(),
            Word { Keyword: Keyword.DATE32 } => new DataType.Date32(),

            Word { Keyword: Keyword.DATETIME } => new DataType.Datetime(ParseOptionalPrecision()),
            Word { Keyword: Keyword.DATETIME64 } => ParseDate64(),

            Word { Keyword: Keyword.TIMESTAMP } => ParseTimestamp(),
            Word { Keyword: Keyword.TIMESTAMPTZ } => new DataType.Timestamp(TimezoneInfo.Tz, ParseOptionalPrecision()),

            Word { Keyword: Keyword.TIME } => ParseTime(),
            Word { Keyword: Keyword.TIMETZ } => new DataType.Time(TimezoneInfo.Tz, ParseOptionalPrecision()),
            // Interval types can be followed by a complicated interval
            // qualifier that we don't currently support. See
            // parse_interval for a taste.
            Word { Keyword: Keyword.INTERVAL } => new DataType.Interval(),
            Word { Keyword: Keyword.JSON } => new DataType.Json(),
            Word { Keyword: Keyword.JSONB } => new DataType.JsonB(),
            Word { Keyword: Keyword.REGCLASS } => new DataType.Regclass(),
            Word { Keyword: Keyword.STRING } => new DataType.StringType(),
            Word { Keyword: Keyword.FIXEDSTRING } => ParseFixedString(),

            Word { Keyword: Keyword.TEXT } => new DataType.Text(),
            Word { Keyword: Keyword.BYTEA } => new DataType.Bytea(),
            Word { Keyword: Keyword.NUMERIC } => new DataType.Numeric(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.DECIMAL } => new DataType.Decimal(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.DEC } => new DataType.Dec(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.BIGNUMERIC } => new DataType.BigNumeric(ParseExactNumberOptionalPrecisionScale()),
            Word { Keyword: Keyword.ENUM } => new DataType.Enum(ParseStringValue()),
            Word { Keyword: Keyword.SET } => new DataType.Set(ParseStringValue()),
            Word { Keyword: Keyword.ARRAY } => ParseArray(),
            Word { Keyword: Keyword.STRUCT } when _dialect is BigQueryDialect or GenericDialect => ParseStruct(),
            Word { Keyword: Keyword.UNION } when _dialect is DuckDbDialect or GenericDialect => ParseUnion(),
            Word { Keyword: Keyword.NULLABLE } when _dialect is ClickHouseDialect or GenericDialect =>
                ParseSubtype(child => new DataType.Nullable(child)),
            Word { Keyword: Keyword.LOWCARDINALITY } when _dialect is ClickHouseDialect or GenericDialect =>
                ParseSubtype(child => new DataType.LowCardinality(child)),

            Word { Keyword: Keyword.MAP } when _dialect is ClickHouseDialect or GenericDialect => ParseMap(),
            Word { Keyword: Keyword.NESTED } when _dialect is ClickHouseDialect or GenericDialect => ExpectParens(() =>
                new DataType.Nested(ParseCommaSeparated(ParseColumnDef))),

            Word { Keyword: Keyword.TUPLE } when _dialect is ClickHouseDialect or GenericDialect => ParseClickhouseTuple(),
            _ => ParseUnmatched()
        };

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword.ARRAY syntax from above
        while (ConsumeToken<LeftBracket>())
        {
            long? size = null;

            if (_dialect is GenericDialect or DuckDbDialect or PostgreSqlDialect)
            {
                size = (long?)MaybeParseNullable(ParseLiteralUnit);
            }

            ExpectToken<RightBracket>();
            data = new DataType.Array(new ArrayElementTypeDef.SquareBracket(data, size));
        }

        return (data, trailingBracket);

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
            switch (_dialect)
            {
                case SnowflakeDialect:
                    return new DataType.Array(new ArrayElementTypeDef.None());

                case ClickHouseDialect:
                    return ParseSubtype(p => new DataType.Array(new ArrayElementTypeDef.Parenthesis(p)));
            }

            ExpectToken<LessThan>();
            var (insideType, trailing) = ParseDataTypeHelper();
            trailingBracket = ExpectClosingAngleBracket(trailing);
            return new DataType.Array(new ArrayElementTypeDef.AngleBracket(insideType));
        }

        DataType ParseStruct()
        {
            PrevToken();
            (var fieldDefinitions, trailingBracket) = ParseStructTypeDef(ParseStructFieldDef);
            return new DataType.Struct(fieldDefinitions);
        }

        DataType ParseUnion()
        {
            PrevToken();
            return new DataType.Union(ParseUnionTypeDef());
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

        DataType ParseDate64()
        {
            PrevToken();
            var (precision, timezone) = ParseDateTime64();
            return new DataType.Datetime64(precision, timezone);
        }

        DataType ParseFixedString()
        {
            return ExpectParens(() => new DataType.FixedString(ParseLiteralUnit()));
        }

        DataType ParseSubtype(Func<DataType, DataType> parentType)
        {
            return ExpectParens(() =>
            {
                var insideType = ParseDataType();
                return parentType(insideType);
            });
        }

        DataType ParseMap()
        {
            PrevToken();
            var (key, value) = ParseClickHouseMapDef();
            return new DataType.Map(key, value);
        }

        DataType ParseClickhouseTuple()
        {
            PrevToken();
            return new DataType.Tuple(ParseClickHouseTupleDef());
        }
        #endregion
    }

    private (ulong, string?) ParseDateTime64()
    {
        ExpectKeyword(Keyword.DATETIME64);

        return ExpectParens(() =>
        {
            var precision = ParseLiteralUnit();
            string? timeZone = null;

            if (ConsumeToken<Comma>())
            {
                timeZone = ParseLiteralString();
            }

            return (precision, timeZone);
        });
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
            // Support for MySql dialect double-quoted string, `AS "HOUR"` for example
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

    public ObjectName ParseObjectName(bool inTableClause)
    {
        return ParseObjectNameWithClause(inTableClause);
    }
    /// <summary>
    ///  Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `schema."table"
    /// </summary>
    /// <returns></returns>
    public ObjectName ParseObjectNameWithClause(bool inTableClause)
    {
        var idents = new Sequence<Ident>();
        while (true)
        {
            idents.Add(ParseIdentifierWithClause(inTableClause));
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

    public Ident ParseIdentifierWithClause(bool inTableClause)
    {
        var token = NextToken();
        return token switch
        {
            Word word => ParseIdent(word),
            SingleQuotedString s => new Ident(s.Value, Symbols.SingleQuote),
            DoubleQuotedString s => new Ident(s.Value, Symbols.DoubleQuote),
            _ => throw Expected("identifier", token)
        };

        Ident ParseIdent(Word word)
        {
            var parsed = word.ToIdent();

            // On BigQuery, hyphens are permitted in unquoted identifiers inside a FROM or
            // TABLE clause [0].
            //
            // The first segment must be an ordinary unquoted identifier, e.g. it must not start
            // with a digit. Subsequent segments are either must either be valid identifiers or
            // integers, e.g. foo-123 is allowed, but foo-123a is not.
            //
            // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical

            if (_dialect is BigQueryDialect && word.QuoteStyle == null && inTableClause)
            {
                var requireWhitespace = false;
                var text = new StringBuilder(word.Value);
                // var peek = PeekTokenNoSkip();

                while (PeekTokenNoSkip() is Minus)
                {
                    NextToken();
                    text.Append('-');

                    var nextToken = NextTokenNoSkip();
                    if (nextToken is Word w)
                    {
                        if (w.QuoteStyle == null)
                        {
                            text.Append(w.Value);
                        }
                    }
                    else if (nextToken is Number n)
                    {
                        if (n.Value.All(char.IsDigit))
                        {
                            text.Append(n.Value);
                            requireWhitespace = true;
                        }
                    }
                    else
                    {
                        if (nextToken != null)
                        {
                            throw Expected("continuation of hyphenated identifier", nextToken);
                        }

                        throw Expected("continuation of hyphenated identifier");
                    }
                }

                if (requireWhitespace)
                {
                    var next = NextToken();
                    if (token is EOF or Whitespace)
                    {
                        throw Expected("whitespace following hyphenated identifier", next);
                    }
                }

                parsed = parsed with { Value = text.ToString() };
            }

            return parsed;
        }
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
        if (ParseKeyword(Keyword.MAX))
        {
            return new CharacterLength.Max();
        }

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

        return new CharacterLength.IntegerLength(length, unit);
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

        return new None();
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
        bool withFromKeyword = true;

        if (!ParseKeyword(Keyword.FROM))
        {
            if (_dialect is BigQueryDialect or GenericDialect)
            {
                withFromKeyword = false;
            }
            else
            {
                tables = ParseCommaSeparated(ParseObjectName);
                ExpectKeyword(Keyword.FROM);
                withFromKeyword = true;
            }
        }

        var from = ParseCommaSeparated(ParseTableAndJoins);
        var @using = ParseInit(ParseKeyword(Keyword.USING), ParseTableFactor);

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);
        var returning = ParseInit(ParseKeyword(Keyword.RETURNING), () => ParseCommaSeparated(ParseSelectItem));
        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));
        var limit = ParseInit(ParseKeyword(Keyword.LIMIT), ParseLimit);

        FromTable fromTable = withFromKeyword ? new FromTable.WithFromKeyword(from) : new FromTable.WithoutKeyword(from);

        return new Delete(new DeleteOperation(tables, fromTable, orderBy, @using, selection, returning, limit));
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
    public Statement ParseExplain(DescribeAlias describeAlias)
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
                Format = format
            },
            _ => ParseDescribeFormat()
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

        ExplainTable ParseDescribeFormat()
        {
            HiveDescribeFormat? hiveFormat = null;
            var kwd = ParseOneOfKeywords(Keyword.EXTENDED, Keyword.FORMATTED);
            var hasTableKeyword = ParseKeyword(Keyword.TABLE);
            var tableName = ParseObjectName();

            switch (kwd)
            {
                case Keyword.EXTENDED:
                    hiveFormat = HiveDescribeFormat.Extended;
                    break;

                case Keyword.FORMATTED:
                    hiveFormat = HiveDescribeFormat.Formatted;
                    break;
            }

            return new ExplainTable(describeAlias, tableName, hiveFormat, hasTableKeyword);
        }
    }

    public CommonTableExpression ParseCommonTableExpression()
    {
        var name = ParseIdentifier();

        CommonTableExpression? cte;

        if (ParseKeyword(Keyword.AS))
        {
            CteAsMaterialized? isMaterialized = null;
            if (_dialect is PostgreSqlDialect)
            {
                if (ParseKeyword(Keyword.MATERIALIZED))
                {
                    isMaterialized = CteAsMaterialized.Materialized;
                }
                else if (ParseKeywordSequence(Keyword.NOT, Keyword.MATERIALIZED))
                {
                    isMaterialized = CteAsMaterialized.NotMaterialized;

                }
            }

            var query = ExpectParens(() => ParseQuery());
            var alias = new TableAlias(name);
            cte = new CommonTableExpression(alias, query.Query, Materialized: isMaterialized);
        }
        else
        {
            var columns = ParseParenthesizedColumnList(IsOptional.Optional, false);
            if (!columns.Any())
            {
                columns = null;
            }

            ExpectKeyword(Keyword.AS);
            CteAsMaterialized? isMaterialized = null;
            if (_dialect is PostgreSqlDialect)
            {
                if (ParseKeyword(Keyword.MATERIALIZED))
                {
                    isMaterialized = CteAsMaterialized.Materialized;
                }
                else if (ParseKeywordSequence(Keyword.NOT, Keyword.MATERIALIZED))
                {
                    isMaterialized = CteAsMaterialized.NotMaterialized;

                }
            }
            var query = ExpectParens(() => ParseQuery());

            var alias = new TableAlias(name, columns);
            cte = new CommonTableExpression(alias, query.Query, Materialized: isMaterialized);
        }

        if (ParseKeyword(Keyword.FROM))
        {
            cte.From = ParseIdentifier();
        }

        return cte;
    }
    /// <summary>
    /// Parse a `FOR JSON` clause
    /// </summary>
    public ForClause ParseForJson()
    {
        ForJson forJson;

        if (ParseKeyword(Keyword.AUTO))
        {
            forJson = new ForJson.Auto();
        }
        else if (ParseKeyword(Keyword.PATH))
        {
            forJson = new ForJson.Path();
        }
        else
        {
            throw Expected("FOR JSON [AUTO | PATH ]", PeekToken());
        }

        string? root = null;
        var includeNullValues = false;
        var withoutArrayWrapper = false;

        while (PeekToken() is Comma)
        {
            NextToken();
            if (ParseKeyword(Keyword.ROOT))
            {
                root = ExpectParens(ParseLiteralString);
            }
            else if (ParseKeyword(Keyword.INCLUDE_NULL_VALUES))
            {
                includeNullValues = true;
            }
            else if (ParseKeyword(Keyword.WITHOUT_ARRAY_WRAPPER))
            {
                withoutArrayWrapper = true;
            }
        }

        return new ForClause.Json(forJson, root, includeNullValues, withoutArrayWrapper);
    }
    /// <summary>
    /// Parse a `FOR XML` clause
    /// </summary>
    public ForClause ParseForXml()
    {
        ForXml forXml;

        if (ParseKeyword(Keyword.RAW))
        {
            string? elementName = null;
            if (PeekToken() is LeftParen)
            {
                elementName = ExpectParens(ParseLiteralString);
            }

            forXml = new ForXml.Raw(elementName);
        }
        else if (ParseKeyword(Keyword.AUTO))
        {
            forXml = new ForXml.Auto();
        }
        else if (ParseKeyword(Keyword.EXPLICIT))
        {
            forXml = new ForXml.Explicit();
        }
        else if (ParseKeyword(Keyword.PATH))
        {
            var elementName = ExpectParens(ParseLiteralString);
            forXml = new ForXml.Path(elementName);
        }
        else
        {
            throw Expected("FOR XML [RAW | AUTO | EXPLICIT | PATH ]", PeekToken());
        }

        var elements = false;
        var binaryBase64 = false;
        string? root = null;
        var type = false;

        while (PeekToken() is Comma)
        {
            NextToken();
            if (ParseKeyword(Keyword.ELEMENTS))
            {
                elements = true;
            }
            else if (ParseKeyword(Keyword.BINARY))
            {
                ExpectKeyword(Keyword.BASE64);
                binaryBase64 = true;
            }
            else if (ParseKeyword(Keyword.ROOT))
            {
                root = ExpectParens(ParseLiteralString);
            }
            else if (ParseKeyword(Keyword.TYPE))
            {
                type = true;
            }
        }

        return new ForClause.Xml(forXml, elements, binaryBase64, root, type);
    }
    // ReSharper disable once GrammarMistakeInComment
    /// <summary>
    /// Parse a "query body", which is an expression with roughly the
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

        return ParseRemainingSetExpressions(expr, precedence);


    }

    private SetExpression ParseRemainingSetExpressions(SetExpression expr, int precedence)
    {

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
            };
        }

        SetQuantifier ParseSetQualifier(SetOperator op)
        {
            return op switch
            {
                SetOperator.Except or SetOperator.Intersect or SetOperator.Union
                    when ParseKeywordSequence(Keyword.DISTINCT, Keyword.BY, Keyword.NAME) =>
                    SetQuantifier.DistinctByName,

                SetOperator.Except or SetOperator.Intersect or SetOperator.Union
                    when ParseKeywordSequence(Keyword.BY, Keyword.NAME) =>
                    SetQuantifier.ByName,

                SetOperator.Except or SetOperator.Intersect or SetOperator.Union
                    when ParseKeyword(Keyword.ALL) => ParseKeywordSequence(Keyword.BY, Keyword.NAME)
                        ? SetQuantifier.AllByName
                        : SetQuantifier.All,

                SetOperator.Except or SetOperator.Intersect or SetOperator.Union
                    when ParseKeyword(Keyword.DISTINCT) =>
                    SetQuantifier.Distinct,

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
        ValueTableMode? valueTableMode = null;
        if (_dialect is BigQueryDialect && ParseKeyword(Keyword.AS))
        {
            if (ParseKeyword(Keyword.VALUE))
            {
                valueTableMode = new ValueTableMode.AsValue();
            }
            else if (ParseKeyword(Keyword.STRUCT))
            {
                valueTableMode = new ValueTableMode.AsStruct();
            }
        }

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

        Expression? preWhere = null;
        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.PREWHERE))
        {
            preWhere = ParseExpr();
        }

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);

        GroupByExpression? groupBy = null;

        if (ParseKeywordSequence(Keyword.GROUP, Keyword.BY))
        {
            Sequence<Expression>? expressions = null;

            if (!ParseKeyword(Keyword.ALL))
            {
                expressions = ParseCommaSeparated(ParseGroupByExpr);
            }

            Sequence<GroupByWithModifier>? modifiers = null;

            if (_dialect is ClickHouseDialect or GenericDialect)
            {
                while (true)
                {
                    if (!ParseKeyword(Keyword.WITH))
                    {
                        break;
                    }

                    var keyword = ExpectOneOfKeywords(Keyword.ROLLUP, Keyword.CUBE, Keyword.TOTALS);

                    var modifier = keyword switch
                    {
                        Keyword.ROLLUP => GroupByWithModifier.Rollup,
                        Keyword.CUBE => GroupByWithModifier.Cube,
                        Keyword.TOTALS => GroupByWithModifier.Totals,
                        _ => throw Expected("to match GroupBy modifier keyword", PeekToken())
                    };

                    modifiers ??= [];
                    modifiers.Add(modifier);
                }
            }

            groupBy = expressions == null
                ? new GroupByExpression.All(modifiers)
                : new GroupByExpression.Expressions(expressions, modifiers);
        }

        var clusterBy = ParseInit(ParseKeywordSequence(Keyword.CLUSTER, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var distributeBy = ParseInit(ParseKeywordSequence(Keyword.DISTRIBUTE, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var sortBy = ParseInit(ParseKeywordSequence(Keyword.SORT, Keyword.BY), () => ParseCommaSeparated(ParseExpr));

        var having = ParseInit(ParseKeyword(Keyword.HAVING), ParseExpr);

        var (namedWindows, qualify, windowBeforeQualify) = ParseWindow();

        var connectBy = ParseConnect();

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
            QualifyBy = qualify,
            ValueTableMode = valueTableMode,
            ConnectBy = connectBy,
            WindowBeforeQualify = windowBeforeQualify,
            PreWhere = preWhere
        };

        ConnectBy? ParseConnect()
        {
            if (_dialect.SupportsConnectBy && ParseOneOfKeywords(Keyword.START, Keyword.CONNECT) != Keyword.undefined)
            {
                PrevToken();
                return ParseConnectBy();
            }

            return null;
        }

        (Sequence<NamedWindowDefinition>?, Expression?, bool) ParseWindow()
        {
            if (ParseKeyword(Keyword.WINDOW))
            {
                var windows = ParseCommaSeparated(ParseNamedWindow);

                if (ParseKeyword(Keyword.QUALIFY))
                {
                    return (windows, ParseExpr(), true);
                }

                return (windows, null, true);
            }
            else if (ParseKeyword(Keyword.QUALIFY))
            {
                var qualifyExpr = ParseExpr();
                if (ParseKeyword(Keyword.WINDOW))
                {
                    return (ParseCommaSeparated(ParseNamedWindow), qualifyExpr, false);
                }

                return (null, qualifyExpr, false);
            }
            else
            {
                return (null, null, false);
            }
        }
    }

    private T WithState<T>(ParserState state, Func<T> action)
    {
        var currentState = _parserState;
        _parserState = state;
        var result = action();
        _parserState = currentState;
        return result;
    }

    public ConnectBy ParseConnectBy()
    {
        if (ParseKeywordSequence(Keyword.CONNECT, Keyword.BY))
        {
            var rel = WithState(ParserState.ConnectBy, () => ParseCommaSeparated(ParseExpr));
            ExpectKeywords(Keyword.START, Keyword.WITH);
            return new ConnectBy(ParseExpr(), rel);
        }

        ExpectKeywords(Keyword.START, Keyword.WITH);
        var conditionExpression = ParseExpr();
        ExpectKeywords(Keyword.CONNECT, Keyword.BY);
        var conditionRelationships = WithState(ParserState.ConnectBy, () => ParseCommaSeparated(ParseExpr));
        return new ConnectBy(conditionExpression, conditionRelationships);
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

        OneOrManyWithParens<ObjectName> variables;

        if (ParseKeywordSequence(Keyword.TIME, Keyword.ZONE))
        {
            variables = new OneOrManyWithParens<ObjectName>.One(new ObjectName("TIMEZONE"));
        }
        else if (_dialect.SupportsParenthesizedSetVariables && ConsumeToken<LeftParen>())
        {
            var objectNames = ParseCommaSeparated(ParseIdentifier).Select(i => new ObjectName(i));

            variables = new OneOrManyWithParens<ObjectName>.Many(new Sequence<ObjectName>(objectNames));
            ExpectToken<RightParen>();
        }
        else
        {
            variables = new OneOrManyWithParens<ObjectName>.One(ParseObjectName(false));
        }

        if (variables is OneOrManyWithParens<ObjectName>.One one &&
            one.Value.ToString().ToUpperInvariant() == "NAMES" &&
            _dialect is MySqlDialect or GenericDialect)
        {
            if (ParseKeyword(Keyword.DEFAULT))
            {
                return new SetNamesDefault();
            }

            var charsetName = ParseLiteralString();
            var collationName = ParseKeyword(Keyword.COLLATE) ? ParseLiteralString() : null;
            return new SetNames(charsetName, collationName);
        }

        var parenthesizedAssignment = variables is OneOrManyWithParens<ObjectName>.Many;

        if (ConsumeToken<Equal>() || ParseKeyword(Keyword.TO))
        {
            if (parenthesizedAssignment)
            {
                ExpectToken<LeftParen>();
            }

            var values = new Sequence<Expression>();
            while (true)
            {
                try
                {
                    var subQuery = TryParseExpressionSubQuery();
                    var value = subQuery ?? ParseExpr();

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

                if (parenthesizedAssignment)
                {
                    ExpectToken<RightParen>();
                }

                return new SetVariable(
                    modifier == Keyword.LOCAL,
                    modifier == Keyword.HIVEVAR,
                    variables,
                    values);
            }
        }

        // TODO set variable expectation

        switch (variables.ToString())
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

    public Statement ParseRelease()
    {
        ParseKeyword(Keyword.SAVEPOINT);
        var name = ParseIdentifier();
        return new ReleaseSavepoint(name);
    }

    public Statement ParseRollback()
    {
        var chain = ParseCommitRollbackChain();
        var savepoint = ParseRollbackSavepoint();
        return new Rollback(chain, savepoint);
    }

    public Ident? ParseRollbackSavepoint()
    {
        if (!ParseKeyword(Keyword.TO))
        {
            return null;
        }

        ParseKeyword(Keyword.SAVEPOINT);
        return ParseIdentifier();
    }

    public Statement ParseShow()
    {
        var extended = ParseKeyword(Keyword.EXTENDED);
        var full = ParseKeyword(Keyword.FULL);
        var session = ParseKeyword(Keyword.SESSION);
        var global = ParseKeyword(Keyword.GLOBAL);

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

        if (_dialect is MySqlDialect or GenericDialect&& ParseKeyword(Keyword.VARIABLES))
        {
            return new ShowVariables(ParseShowStatementFilter(), global, session);
        }

        if ( _dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.STATUS))
        {
            return new ShowStatus(ParseShowStatementFilter(), session, global);
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
            var @global = ParseKeyword(Keyword.GLOBAL);

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
                    JoinOperator = joinOperator,
                    Global = @global
                };
            }
            else if (ParseKeyword(Keyword.OUTER))
            {
                ExpectKeyword(Keyword.APPLY);
                join = new Join
                {
                    Relation = ParseTableFactor(),
                    JoinOperator = new JoinOperator.OuterApply(),
                    Global = @global
                };
            }
            else if (ParseKeyword(Keyword.ASOF))
            {
                ExpectKeyword(Keyword.JOIN);

                var asOfRelation = ParseTableFactor();
                ExpectKeyword(Keyword.MATCH_CONDITION);
                var matchCondition = ExpectParens(ParseExpr);
                join = new Join(asOfRelation, new JoinOperator.AsOf(matchCondition, ParseJoinConstraint(false)), @global);
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
                    JoinOperator = joinAction(joniConstraint),
                    Global = @global
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
            if (ConsumeToken<LeftParen>())
            {
                return ParseDerivedTableFactor(IsLateral.Lateral);
            }

            var fnName = ParseObjectName();
            ExpectLeftParen();
            var fnArgs = ParseOptionalArgs();
            var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
            return new TableFactor.Function(true, fnName, fnArgs) { Alias = alias };
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
                Keyword kw;
                while ((kw = ParseOneOfKeywords(Keyword.PIVOT, Keyword.UNPIVOT)) != Keyword.undefined)
                {
                    result = kw switch
                    {
                        Keyword.PIVOT => ParsePivotTableFactor(result),
                        Keyword.UNPIVOT => ParseUnpivotTableFactor(result),
                        _ => throw new ParserException("Unable to parse Pivot table factor", PeekToken().Location)
                    };
                }

                return result;
            }

            // ReSharper disable once GrammarMistakeInComment
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
                            or TableFactor.Function
                            or TableFactor.UnNest
                            or TableFactor.JsonTable
                            or TableFactor.TableFunction
                            or TableFactor.Pivot
                            or TableFactor.Unpivot
                            or TableFactor.MatchRecognize
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

            var withOrdinality = ParseKeywordSequence(Keyword.WITH, Keyword.ORDINALITY);

            var alias = ParseOptionalTableAlias(Keywords.ReservedForColumnAlias);

            var withOffset = ParseKeywordSequence(Keyword.WITH, Keyword.OFFSET);
            var withOffsetAlias = withOffset ? ParseOptionalAlias(Keywords.ReservedForColumnAlias) : null;
            return new TableFactor.UnNest(expressions)
            {
                Alias = alias,
                WithOffset = withOffset,
                WithOffsetAlias = withOffsetAlias,
                WithOrdinality = withOrdinality
            };
        }

        var peekedTokens = PeekTokens(2);
        if (_dialect is SnowflakeDialect or DatabricksDialect or GenericDialect &&
            peekedTokens[0] is Word { Keyword: Keyword.VALUES } &&
            peekedTokens[1] is LeftParen)
        {
            ExpectKeyword(Keyword.VALUES);
            // Snowflake and Databricks allow syntax like below:
            // SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t (col1, col2)
            // where there are no parentheses around the VALUES clause.
            var values = new SetExpression.ValuesExpression(ParseValues(false));
            var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
            return new TableFactor.Derived(new Query(values))
            {
                Alias = alias
            };
        }

        var nextToken = PeekToken();
        if (nextToken is Word { Keyword: Keyword.JSON_TABLE } && PeekNthTokenIs<LeftParen>(1))
        {
            ExpectKeyword(Keyword.JSON_TABLE);
            var jsonTable = ExpectParens(() =>
            {
                var jsonExpr = ParseExpr();
                ExpectToken<Comma>();
                var path = ParseValue();
                ExpectKeyword(Keyword.COLUMNS);
                var columns = ExpectParens(() => ParseCommaSeparated(ParseJsonTableColumnDef));
                return new TableFactor.JsonTable(jsonExpr, path, columns);
            });
            jsonTable.Alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);
            return jsonTable;
        }

        var name = ParseObjectNameWithClause(true);

        Sequence<Ident>? partitions = null;
        if (_dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.PARTITION))
        {
            partitions = ParseIdentifiers();
        }

        // Parse potential version qualifier
        var version = ParseTableVersion();

        // Postgres, MSSQL: table-valued functions:
        var args = ParseInit(ConsumeToken<LeftParen>(), ParseOptionalArgs);
        var ordinality = ParseKeywordSequence(Keyword.WITH, Keyword.ORDINALITY);
        var optionalAlias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

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
            Version = version,
            Partitions = partitions,
            WithOrdinality = ordinality
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

        if (_dialect.SupportsMatchRecognize && ParseKeyword(Keyword.MATCH_RECOGNIZE))
        {
            table = ParseMatchRecognize(table);
        }

        return table;
    }

    public TableFactor ParseMatchRecognize(TableFactor table)
    {
        MatchRecognizePattern pattern = null!;
        Sequence<Expression>? partitionBy = null;
        Sequence<OrderByExpression>? orderBy = null;
        Sequence<Measure>? measures = null;
        RowsPerMatch? rowsPerMatch = null;
        AfterMatchSkip? afterMatchSkip = null;

        Sequence<SymbolDefinition> symbols = ExpectParens(() =>
        {
            partitionBy = ParseKeywordSequence(Keyword.PARTITION, Keyword.BY)
                ? ParseCommaSeparated(ParseExpr)
                : [];

            orderBy = ParseKeywordSequence(Keyword.ORDER, Keyword.BY)
                ? ParseCommaSeparated(ParseOrderByExpr)
                : [];

            measures = ParseKeyword(Keyword.MEASURES)
                ? ParseCommaSeparated(() =>
                {
                    var measure = ParseExpr();
                    ParseKeyword(Keyword.AS);
                    var alias = ParseIdentifier();
                    return new Measure(measure, alias);
                })
                : [];

            if (ParseKeywordSequence(Keyword.ONE, Keyword.ROW, Keyword.PER, Keyword.MATCH))
            {
                rowsPerMatch = new RowsPerMatch.OneRow();
            }
            else if (ParseKeywordSequence(Keyword.ALL, Keyword.ROW, Keyword.PER, Keyword.MATCH))
            {
                EmptyMatchesMode? mode = null;

                if (ParseKeywordSequence(Keyword.SHOW, Keyword.EMPTY, Keyword.MATCHES))
                {
                    mode = EmptyMatchesMode.Show;
                }
                else if (ParseKeywordSequence(Keyword.OMIT, Keyword.EMPTY, Keyword.MATCHES))
                {
                    mode = EmptyMatchesMode.Omit;
                }
                else if (ParseKeywordSequence(Keyword.WITH, Keyword.UNMATCHED, Keyword.ROWS))
                {
                    mode = EmptyMatchesMode.WithUnmatched;
                }

                rowsPerMatch = new RowsPerMatch.AllRows(mode);
            }

            if (ParseKeywordSequence(Keyword.AFTER, Keyword.MATCH, Keyword.SKIP))
            {
                if (ParseKeywordSequence(Keyword.PAST, Keyword.LAST, Keyword.ROW))
                {
                    afterMatchSkip = new AfterMatchSkip.PastLastRow();
                }
                else if (ParseKeywordSequence(Keyword.TO, Keyword.NEXT, Keyword.ROW))
                {
                    afterMatchSkip = new AfterMatchSkip.ToNextRow();
                }
                else if (ParseKeywordSequence(Keyword.TO, Keyword.FIRST))
                {
                    afterMatchSkip = new AfterMatchSkip.ToFirst(ParseIdentifier());
                }
                else if (ParseKeywordSequence(Keyword.TO, Keyword.LAST))
                {
                    afterMatchSkip = new AfterMatchSkip.ToLast(ParseIdentifier());
                }
                else
                {
                    throw Expected("after match skip option", NextToken());
                }
            }


            ExpectKeyword(Keyword.PATTERN);
            pattern = ExpectParens(ParsePattern);
            ExpectKeyword(Keyword.DEFINE);

            return ParseCommaSeparated(() =>
            {
                var symbol = ParseIdentifier();
                ExpectKeyword(Keyword.AS);
                var definition = ParseExpr();
                return new SymbolDefinition(symbol, definition);
            });
        });

        var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

        return new TableFactor.MatchRecognize(table,
            partitionBy,
            orderBy,
            measures,
            rowsPerMatch,
            afterMatchSkip,
            pattern,
            symbols,
            alias);
    }

    public MatchRecognizePattern ParseBasePattern()
    {
        var nextToken = NextToken();

        switch (nextToken)
        {
            case Caret:
                return new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.Start());
            case Placeholder:
                return new MatchRecognizePattern.Symbol(new MatchRecognizeSymbol.End());
            case LeftBrace:
                {
                    ExpectToken<Minus>();
                    var symbol = new MatchRecognizeSymbol.Named(ParseIdentifier());
                    ExpectToken<Minus>();
                    ExpectToken<RightBrace>();
                    return new MatchRecognizePattern.Exclude(symbol);
                }
            case Word { Value: "PERMUTE" }:
                {
                    Sequence<MatchRecognizeSymbol> symbols = ExpectParens(() =>
                        ParseCommaSeparated<MatchRecognizeSymbol>(() => new MatchRecognizeSymbol.Named(ParseIdentifier())));
                    return new MatchRecognizePattern.Permute(symbols);
                }
            case LeftParen:
                {
                    var pattern = ParsePattern();
                    ExpectRightParen();
                    return new MatchRecognizePattern.Group(pattern);
                }
        }

        PrevToken();
        var ident = ParseIdentifier();
        var named = new MatchRecognizeSymbol.Named(ident);
        return new MatchRecognizePattern.Symbol(named);
    }

    public MatchRecognizePattern ParseRepetitionPattern()
    {
        var pattern = ParseBasePattern();
        var loop = true;

        while (loop)
        {
            var token = NextToken();

            var quantifier = token switch
            {
                Multiply => new RepetitionQualifier.ZeroOrMore(),
                Plus => new RepetitionQualifier.OneOrMore(),
                Placeholder { Value: "?" } => new RepetitionQualifier.AtMostOne(),
                LeftBrace => ParseLeftBrace(),
                _ => Break()
            };

            if (!loop && quantifier == null)
            {
                break;
            }

            pattern = new MatchRecognizePattern.Repetition(pattern, quantifier!);
        }

        return pattern;

        RepetitionQualifier ParseLeftBrace()
        {
            var next = NextToken();
            switch (next)
            {
                case Comma:
                    var t = NextToken();
                    if (t is not Number n)
                    {
                        throw Expected("literal number", t);
                    }

                    ExpectToken<RightBrace>();
                    return new RepetitionQualifier.AtMost(int.Parse(n.Value));

                case Number num when ConsumeToken<Comma>():
                    var tn = NextToken();

                    if (tn is Number m)
                    {
                        ExpectToken<RightBrace>();
                        return new RepetitionQualifier.Range(int.Parse(num.Value), int.Parse(m.Value));
                    }

                    if (tn is RightBrace)
                    {
                        return new RepetitionQualifier.AtLeast(int.Parse(num.Value));
                    }

                    throw Expected("} or upper bound", tn);

                case Number nu:
                    ExpectToken<RightBrace>();
                    return new RepetitionQualifier.Exactly(int.Parse(nu.Value));

                default:
                    throw Expected("qualifier range", next);
            }
        }

        RepetitionQualifier? Break()
        {
            PrevToken();
            loop = false;
            return null;
        }
    }

    public MatchRecognizePattern ParsePattern()
    {
        var pattern = ParseConcatPattern();

        if (ConsumeToken<Pipe>())
        {
            var next = ParsePattern();
            if (next is MatchRecognizePattern.Alternation alt)
            {
                alt.Patterns.Insert(0, pattern);
                return new MatchRecognizePattern.Alternation(alt.Patterns);
            }
            else
            {
                return new MatchRecognizePattern.Alternation([pattern, next]);
            }
        }
        else
        {
            return pattern;
        }
    }

    public MatchRecognizePattern ParseConcatPattern()
    {
        var patterns = new Sequence<MatchRecognizePattern> { ParseRepetitionPattern() };

        while (PeekToken() is not RightParen and not Pipe)
        {
            patterns.Add(ParseRepetitionPattern());
        }

        if (patterns.Count == 1)
        {
            return patterns[0];
        }

        return new MatchRecognizePattern.Concat(patterns);
    }

    public JsonTableColumn ParseJsonTableColumnDef()
    {
        var name = ParseIdentifier();
        var type = ParseDataType();
        var exists = ParseKeyword(Keyword.EXISTS);
        ExpectKeyword(Keyword.PATH);
        var path = ParseValue();

        JsonTableColumnErrorHandling? onEmpty = null;
        JsonTableColumnErrorHandling? onError = null;

        while (ParseJsonTableColumnErrorHandling() is { } handling)
        {
            if (ParseKeyword(Keyword.EMPTY))
            {
                onEmpty = handling;
            }
            else
            {
                ExpectKeyword(Keyword.ERROR);
                onError = handling;
            }
        }

        return new JsonTableColumn(name, type, path, exists, onEmpty, onError);
    }

    public JsonTableColumnErrorHandling? ParseJsonTableColumnErrorHandling()
    {
        JsonTableColumnErrorHandling res;

        if (ParseKeyword(Keyword.NULL))
        {
            res = new JsonTableColumnErrorHandling.Null();
        }
        else if (ParseKeyword(Keyword.ERROR))
        {
            res = new JsonTableColumnErrorHandling.Error();
        }
        else if (ParseKeyword(Keyword.DEFAULT))
        {
            res = new JsonTableColumnErrorHandling.Default(ParseValue());
        }
        else
        {
            return null;
        }

        ExpectKeyword(Keyword.ON);

        return res;
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

    public ExpressionWithAlias ParseAliasedFunctionCall()
    {
        var next = NextToken();

        if (next is not Word w)
        {
            throw Expected("a function identifier", PeekToken());
        }

        var expr = ParseFunction(new ObjectName([w.Value]));
        var alias = ParseKeyword(Keyword.AS) ? ParseIdentifier() : null;

        return new ExpressionWithAlias(expr, alias);
    }

    public ExpressionWithAlias ParseExpressionWithAlias()
    {
        var expr = ParseExpr();
        var alias = ParseKeyword(Keyword.AS) ? ParseIdentifier() : null;
        return new ExpressionWithAlias(expr, alias);
    }

    private TableFactor ParsePivotTableFactor(TableFactor table)
    {
        Sequence<ExpressionWithAlias> aggregateFunctions = null!;
        Sequence<Ident> valueColumn = null!;
        Expression? defaultOnNull = null;

        var valueSource = ExpectParens(() =>
        {
            aggregateFunctions = ParseCommaSeparated(ParseAliasedFunctionCall);
            ExpectKeyword(Keyword.FOR);
            valueColumn = ParseObjectName().Values;
            ExpectKeyword(Keyword.IN);

            var source = ExpectParens<PivotValueSource>(() =>
            {
                if (ParseKeyword(Keyword.ANY))
                {
                    var orderBy = ParseKeywordSequence(Keyword.ORDER, Keyword.BY)
                        ? ParseCommaSeparated(ParseOrderByExpr)
                        : null;

                    return new PivotValueSource.Any(orderBy);
                }

                if (ParseOneOfKeywords(Keyword.SELECT, Keyword.WITH) == Keyword.undefined)
                {
                    return new PivotValueSource.List(ParseCommaSeparated(ParseExpressionWithAlias));
                }

                PrevToken();
                return new PivotValueSource.Subquery(ParseQuery());
            });

            if (ParseKeywordSequence(Keyword.DEFAULT, Keyword.ON, Keyword.NULL))
            {
                defaultOnNull = ExpectParens(ParseExpr);
            }

            return source;
        });


        var alias = ParseOptionalTableAlias(Keywords.ReservedForTableAlias);

        return new TableFactor.Pivot(table, aggregateFunctions, valueColumn, valueSource, defaultOnNull, alias);
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
            var permissions = ParseActionsList();
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
                    // This will cover all future added keywords to
                    // parse_grant_permission and unhandled in this match
                    _ => throw Expected("grant privilege keyword", PeekToken())
                };
                return action;
            }).ToSequence();

            //_options.TrailingCommas = oldValue;

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

    public Sequence<ParsedAction> ParseActionsList()
    {
        var values = new Sequence<ParsedAction>();

        while (true)
        {
            values.Add(ParseGrantPermission());
            if (!ConsumeToken<Comma>())
            {
                break;
            }

            if (!_options.TrailingCommas) { continue; }
            var next = PeekToken();

            if (next is Word { Keyword: Keyword.ON } or RightParen or SemiColon or RightBracket or RightBrace or EOF)
            {
                break;
            }
        }

        return values;
    }

    public ParsedAction ParseGrantPermission()
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

                    return new ParsedAction(keyword, columns);
                }

            case Keyword.undefined:
                throw Expected("a privilege keyword", PeekToken());

            default:
                return new ParsedAction(keyword);
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

    public Statement ParseInsert()
    {
        var orConflict = _dialect is not SQLiteDialect ? SqliteOnConflict.None :
            ParseKeywordSequence(Keyword.OR, Keyword.REPLACE) ? SqliteOnConflict.Replace :
            ParseKeywordSequence(Keyword.OR, Keyword.ROLLBACK) ? SqliteOnConflict.Rollback :
            ParseKeywordSequence(Keyword.OR, Keyword.ABORT) ? SqliteOnConflict.Abort :
            ParseKeywordSequence(Keyword.OR, Keyword.FAIL) ? SqliteOnConflict.Fail :
            ParseKeywordSequence(Keyword.OR, Keyword.IGNORE) ? SqliteOnConflict.Ignore :
            ParseKeywordSequence(Keyword.REPLACE) ? SqliteOnConflict.Replace :
            SqliteOnConflict.None;

        var priority = _dialect is not MySqlDialect or GenericDialect ? MySqlInsertPriority.None :
            ParseKeyword(Keyword.LOW_PRIORITY) ? MySqlInsertPriority.LowPriority :
            ParseKeyword(Keyword.DELAYED) ? MySqlInsertPriority.Delayed :
            ParseKeyword(Keyword.HIGH_PRIORITY) ? MySqlInsertPriority.HighPriority :
            MySqlInsertPriority.None;


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

        var tableAlias = _dialect is PostgreSqlDialect && ParseKeyword(Keyword.AS) ? ParseIdentifier() : null;

        var isMySql = _dialect is MySqlDialect;
        Sequence<Ident>? columns = null;
        Sequence<Expression>? partitioned = null;
        Sequence<Ident>? afterColumns = null;
        Statement.Select? source = null;

        if (!ParseKeywordSequence(Keyword.DEFAULT, Keyword.VALUES))
        {
            columns = ParseParenthesizedColumnList(IsOptional.Optional, isMySql);
            if (ParseKeyword(Keyword.PARTITION))
            {
                partitioned = ExpectParens(() => ParseCommaSeparated(ParseExpr));
            }

            if (_dialect is HiveDialect)
            {
                afterColumns = ParseParenthesizedColumnList(IsOptional.Optional, false);
            }

            source = ParseQuery();
        }

        var insertAliases = _dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.AS)
            ? ParseInsertAlias()
            : null;

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

        return new Insert(new InsertOperation(tableName, source)
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
            Returning = returning,
            ReplaceInto = false,
            Priority = priority,
            Alias = tableAlias,
            InsertAlias = insertAliases
        });

        InsertAliases ParseInsertAlias()
        {
            var rowAlias = ParseObjectName();
            var columnAliases = ParseParenthesizedColumnList(IsOptional.Optional, false);

            return new InsertAliases(rowAlias, columnAliases);
        }
    }

    public Statement ParseReplace()
    {
        if (_dialect is not MySqlDialect or GenericDialect)
        {
            throw new ParserException("Unsupported statement REPLACE", PeekToken().Location);
        }

        var insert = ParseInsert();
        if (insert is Insert i)
        {
            i.InsertOperation.ReplaceInto = true;
        }

        return insert;
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
                or MsSqlDialect
                or SQLiteDialect)
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
    public Statement.Assignment ParseAssignment()
    {
        var target = ParseAssignmentTarget();
        ExpectToken<Equal>();
        var expr = ParseExpr();
        return new Statement.Assignment(target, expr);
    }

    private AssignmentTarget ParseAssignmentTarget()
    {
        if (ConsumeToken<LeftParen>())
        {
            var columns = ParseCommaSeparated(ParseObjectName);
            ExpectRightParen();
            return new AssignmentTarget.Tuple(columns);
        }

        return new AssignmentTarget.ColumnName(ParseObjectName());
    }


    public FunctionArg ParseFunctionArgs()
    {
        if (PeekNthTokenIs<RightArrow>(1))
        {
            var name = ParseIdentifier();
            ExpectToken<RightArrow>();
            return new FunctionArg.Named(name, WildcardToFnArg(ParseWildcardExpr()),
                new FunctionArgOperator.RightArrow());
        }
        else if (_dialect.SupportsNamedFunctionArgsWithEqOperator && PeekNthTokenIs<Equal>(1))
        {
            var name = ParseIdentifier();
            ExpectToken<Equal>();
            return new FunctionArg.Named(name, WildcardToFnArg(ParseWildcardExpr()), new FunctionArgOperator.Equal());
        }
        else if (_dialect is DuckDbDialect or GenericDialect && PeekNthTokenIs<Tokens.Assignment>(1))
        {
            var name = ParseIdentifier();
            ExpectToken<Tokens.Assignment>();
            var arg = ParseExpr();
            return new FunctionArg.Named(name, new FunctionArgExpression.FunctionExpression(arg), new FunctionArgOperator.Assignment());
        }
        else
        {
            return new FunctionArg.Unnamed(WildcardToFnArg(ParseWildcardExpr()));
        }

        FunctionArgExpression WildcardToFnArg(Expression wildcard)
        {
            FunctionArgExpression functionExpr = wildcard switch
            {
                QualifiedWildcard q => new FunctionArgExpression.QualifiedWildcard(q.Name),
                Wildcard => new FunctionArgExpression.Wildcard(),
                _ => new FunctionArgExpression.FunctionExpression(wildcard)
            };
            return functionExpr;
        }
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
    /// <summary>
    /// Parse a comma-delimited list of projections after SELECT
    /// </summary>
    /// <returns></returns>
    public SelectItem ParseSelectItem()
    {
        var wildcardExpr = ParseWildcardExpr();

        if (wildcardExpr is QualifiedWildcard q)
        {
            return new SelectItem.QualifiedWildcard(q.Name, ParseWildcardAdditionalOptions());
        }

        if (wildcardExpr is Wildcard)
        {
            return new SelectItem.Wildcard(ParseWildcardAdditionalOptions());
        }

        if (wildcardExpr is Identifier v && v.Ident.Value.ToLower().Equals("from") && v.Ident.QuoteStyle == null)
        {
            throw Expected($"Expected an expression, found: {v}");
        }

        var alias = ParseOptionalAlias(Keywords.ReservedForColumnAlias);

        if (alias != null)
        {
            return new SelectItem.ExpressionWithAlias(wildcardExpr, alias);
        }

        return new SelectItem.UnnamedExpression(wildcardExpr);
    }
    /// <summary>
    /// Parse an [`WildcardAdditionalOptions`](WildcardAdditionalOptions) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    /// </summary>
    /// <returns></returns>
    public WildcardAdditionalOptions ParseWildcardAdditionalOptions()
    {
        IlikeSelectItem? ilikeSelectItem = null;
        if (_dialect is SnowflakeDialect or GenericDialect)
        {
            ilikeSelectItem = ParseOptionalSelectItemIlike();
        }

        ExcludeSelectItem? optExclude = null;
        if (_dialect is GenericDialect or DuckDbDialect or SnowflakeDialect)
        {
            optExclude = ParseOptionalSelectItemExclude();
        }

        ExceptSelectItem? optExcept = null;
        if (_dialect.SupportsSelectWildcardExcept)
        {
            optExcept = ParseOptionalSelectItemExcept();
        }

        ReplaceSelectItem? optReplace = null;
        if (_dialect is GenericDialect
            or BigQueryDialect
            or ClickHouseDialect
            or DuckDbDialect
            or SnowflakeDialect)
        {
            optReplace = ParseOptionalSelectItemReplace();
        }

        RenameSelectItem? optRename = null;
        if (_dialect is GenericDialect or SnowflakeDialect)
        {
            optRename = ParseOptionalSelectItemRename();
        }

        return new WildcardAdditionalOptions
        {
            ILikeOption = ilikeSelectItem,
            ExcludeOption = optExclude,
            ExceptOption = optExcept,
            RenameOption = optRename,
            ReplaceOption = optReplace
        };
    }
    /// <summary>
    /// Parse an [`Ilike`](IlikeSelectItem) information for wildcard select items.
    ///
    /// If it is not possible to parse it, will return an option.
    /// </summary>
    /// <returns></returns>
    public IlikeSelectItem? ParseOptionalSelectItemIlike()
    {
        IlikeSelectItem? iLIke = null;

        if (!ParseKeyword(Keyword.ILIKE)) { return iLIke; }

        var next = NextToken();
        var pattern = next switch
        {
            SingleQuotedString s => s.Value,
            _ => throw Expected("ilike pattern", next)
        };

        iLIke = new IlikeSelectItem(pattern);

        return iLIke;
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

        WithFill? withFill = null;

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeywordSequence(Keyword.WITH, Keyword.FILL))
        {
            withFill = ParseWithFill();
        }

        return new OrderByExpression(expr, asc, nullsFirst, withFill);
    }
    /// <summary>
    /// Parse a WITH FILL clause (ClickHouse dialect)
    /// that follow the WITH FILL keywords in a ORDER BY clause
    /// </summary>
    /// <returns>WithFill instance</returns>
    public WithFill ParseWithFill()
    {
        var from = ParseKeyword(Keyword.FROM) ? ParseExpr() : null;
        var to = ParseKeyword(Keyword.TO) ? ParseExpr() : null;
        var step = ParseKeyword(Keyword.STEP) ? ParseExpr() : null;

        return new WithFill(from, to, step);
    }
    /// <summary>
    ///Parse a set of comma separated INTERPOLATE expressions (ClickHouse dialect)
    /// that follow the INTERPOLATE keyword in an ORDER BY clause with the WITH FILL modifier
    /// </summary>
    /// <returns>Optional Interpolate</returns>
    public Interpolate? ParseInterpolations()
    {
        if (!ParseKeyword(Keyword.INTERPOLATE))
        {
            return null;
        }

        if (!ConsumeToken<LeftParen>()) { return new Interpolate(null); }

        var interpolations = ParseCommaSeparated0(ParseInterpolation, typeof(RightParen));
        ExpectRightParen();
        return new Interpolate(interpolations);
    }
    /// <summary>
    /// Parse a INTERPOLATE expression (ClickHouse dialect)
    /// </summary>
    /// <returns>Interpolate expression</returns>
    private InterpolateExpression ParseInterpolation()
    {
        var column = ParseIdentifier();
        var expression = ParseKeyword(Keyword.AS) ? ParseExpr() : null;
        return new InterpolateExpression(column, expression);
    }
    /// <summary>
    /// Parse a TOP clause, MSSQL equivalent of LIMIT,
    /// that follows after `SELECT [DISTINCT]`.
    /// </summary>
    /// <returns></returns>
    /// <exception cref="NotImplementedException"></exception>
    public Top ParseTop()
    {
        TopQuantity? quantity = null;
        if (ConsumeToken<LeftParen>())
        {
            var quantityExp = ParseExpr();
            ExpectRightParen();
            quantity = new TopQuantity.TopExpression(quantityExp);
        }
        else
        {
            var next = NextToken();
            if (next is Number n)
            {
                if (long.TryParse(n.Value, out var longVal))
                {
                    quantity = new TopQuantity.Constant(longVal);
                }
            }
            else
            {
                throw Expected("literal int", next);
            }
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

    public StartTransaction ParseStartTransaction()
    {
        ExpectKeyword(Keyword.TRANSACTION);
        return new StartTransaction(ParseTransactionModes(), false);
    }

    public Statement ParseBegin()
    {
        TransactionModifier? modifier = null;

        if (!_dialect.SupportsStartTransactionModifier)
        {
            modifier = null;
        }
        else if (ParseKeyword(Keyword.DEFERRED))
        {
            modifier = TransactionModifier.Deferred;
        }
        else if (ParseKeyword(Keyword.IMMEDIATE))
        {
            modifier = TransactionModifier.Immediate;
        }
        else if (ParseKeyword(Keyword.EXCLUSIVE))
        {
            modifier = TransactionModifier.Exclusive;
        }

        _ = ParseOneOfKeywords(Keyword.TRANSACTION, Keyword.WORK);
        return new StartTransaction(ParseTransactionModes(), true, modifier);
    }

    public Statement ParseEnd()
    {
        return new Commit(ParseCommitRollbackChain());
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

        Sequence<Expression>? usingExpressions = null;

        if (ParseKeyword(Keyword.USING))
        {
            usingExpressions = [ParseExpr()];

            while (ConsumeToken<Comma>())
            {
                usingExpressions.Add(ParseExpr());
            }
        }

        return new Execute(name, parameters, usingExpressions);
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

    public Statement ParseUnload()
    {
        Query query = ExpectParens(() => ParseQuery());

        ExpectKeyword(Keyword.TO);

        var to = ParseIdentifier();

        var withOptions = ParseOptions(Keyword.WITH);

        return new Unload(query, to, withOptions);
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

            var clauseKind = MergeClauseKind.Matched;
            if (ParseKeyword(Keyword.NOT))
            {
                clauseKind = MergeClauseKind.NotMatched;
            }

            ExpectKeyword(Keyword.MATCHED);

            if (clauseKind == MergeClauseKind.NotMatched && ParseKeywordSequence(Keyword.BY, Keyword.SOURCE))
            {
                clauseKind = MergeClauseKind.NotMatchedBySource;
            }
            else if (clauseKind == MergeClauseKind.NotMatched && ParseKeywordSequence(Keyword.BY, Keyword.TARGET))
            {
                clauseKind = MergeClauseKind.NotMatchedByTarget;
            }

            var predicate = ParseInit(ParseKeyword(Keyword.AND), ParseExpr);
            ExpectKeyword(Keyword.THEN);

            var keyword = ParseOneOfKeywords(Keyword.UPDATE, Keyword.INSERT, Keyword.DELETE);

            MergeAction mergeAction;

            switch (keyword)
            {
                case Keyword.UPDATE:
                    if (clauseKind is MergeClauseKind.NotMatched or MergeClauseKind.NotMatchedByTarget)
                    {
                        throw new ParserException($"UPDATE is not allowed in a {clauseKind} merge clause");
                    }

                    ExpectKeyword(Keyword.SET);
                    var assignments = ParseCommaSeparated(ParseAssignment);
                    mergeAction = new MergeAction.Update(assignments);
                    break;

                case Keyword.DELETE:
                    if (clauseKind is MergeClauseKind.NotMatched or MergeClauseKind.NotMatchedByTarget)
                    {
                        throw new ParserException($"DELETE is not allowed in a {clauseKind} merge clause");
                    }

                    mergeAction = new MergeAction.Delete();
                    break;

                case Keyword.INSERT:
                    if (clauseKind != MergeClauseKind.NotMatched && clauseKind != MergeClauseKind.NotMatchedByTarget)
                    {
                        throw new ParserException($"INSERT is not allowed in a {clauseKind} merge clause");
                    }

                    var isMySql = _dialect is MySqlDialect;
                    var columns = ParseParenthesizedColumnList(IsOptional.Optional, isMySql);

                    MergeInsertKind kind;
                    if (_dialect is BigQueryDialect or GenericDialect && ParseKeyword(Keyword.ROW))
                    {
                        kind = new MergeInsertKind.Row();
                    }
                    else
                    {
                        ExpectKeyword(Keyword.VALUES);
                        var values = ParseValues(isMySql);
                        kind = new MergeInsertKind.Values(values);
                    }

                    mergeAction = new MergeAction.Insert(new MergeInsertExpression(columns, kind));
                    break;

                default:
                    throw Expected("UPDATE, DELETE or INSERT in merge clause");
            }

            clauses.Add(new MergeClause(clauseKind, mergeAction, predicate));
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
            var value = ParsePragmaValue();
            ExpectRightParen();
            return new Pragma(name, value, false);
        }

        if (ConsumeToken<Equal>())
        {
            return new Pragma(name, ParsePragmaValue(), true);
        }

        return new Pragma(name, null, false);
    }

    public Value ParsePragmaValue()
    {
        var value = ParseValue();

        if (value is Value.SingleQuotedString or Value.DoubleQuotedString or Value.Number or Value.Placeholder)
        {
            return value;
        }

        PrevToken();
        throw Expected("number or string or ? placeholder", PeekToken());
    }
    /// <summary>
    /// INSTALL [ extension_name ]
    /// </summary>
    /// <returns></returns>
    public Statement ParseInstall()
    {
        return new Install(ParseIdentifier());
    }
    /// <summary>
    /// INSTALL [ extension_name ]
    /// </summary>
    /// <returns></returns>
    public Statement ParseLoad()
    {
        return new Load(ParseIdentifier());
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
            sequenceOptions.Add(new SequenceOptions.MinValue(expr));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MINVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MinValue(null));
        }

        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if (ParseKeywordSequence(Keyword.MAXVALUE))
        {
            var expr = new LiteralValue(ParseNumberValue());
            sequenceOptions.Add(new SequenceOptions.MaxValue(expr));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MAXVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MaxValue(null));
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
        if (ParseKeywordSequence(Keyword.NO, Keyword.CYCLE))
        {
            sequenceOptions.Add(new SequenceOptions.Cycle(true));
        }
        else if (ParseKeyword(Keyword.CYCLE))
        {
            sequenceOptions.Add(new SequenceOptions.Cycle(false));
        }

        return sequenceOptions;
    }

    public static string Found(Token token)
    {
        var location = token is EOF ? null : $", {token.Location}";
        return $", found {token}{location}";
    }
}

public record ParsedAction(Keyword Keyword, Sequence<Ident>? Idents = null);