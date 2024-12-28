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
using HiveRowDelimiter = SqlParser.Ast.HiveRowDelimiter;
using Subscript = SqlParser.Ast.Subscript;
// ReSharper disable InconsistentNaming

namespace SqlParser;

// This record type fills in the outcome from the Rust project's macro that
// intercepts control flow depending on parsing result.  The same flow is
// used in the parser, and the outcome of the lambda matches this record.  
public record MaybeParsed<T>(bool Parsed, T Result);

public partial class Parser
{
    /// <summary>
    /// Builds a parser with a SQL fragment that is tokenized but not yet parsed.  This
    /// allows the parser to be used for with subsets of SQL calling any of the parser's
    /// underlying parsing methods.
    /// </summary>
    /// <param name="sql">SQL fragment to tokenize</param>
    /// <param name="dialect">SQL dialect instance</param>
    /// <param name="options">Parsing options</param>
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

    public OptimizeTable ParseOptimizeTable()
    {
        ExpectKeyword(Keyword.TABLE);

        var name = ParseObjectName();
        var onCluster = ParseOptionalOnCluster();

        Partition? partition = null;

        if (ParseKeyword(Keyword.PARTITION))
        {
            if (ParseKeywordSequence(Keyword.ID))
            {
                partition = new Partition.Identifier(ParseIdentifier());
            }
            else
            {
                partition = new Partition.Expr(ParseExpr());
            }
        }

        var includeFinal = ParseKeyword(Keyword.FINAL);
        Deduplicate? deduplicate = null;

        if (ParseKeyword(Keyword.DEDUPLICATE))
        {
            if (ParseKeyword(Keyword.BY))
            {
                deduplicate = new Deduplicate.ByExpression(ParseExpr());
            }
            else
            {
                deduplicate = new Deduplicate.All();
            }
        }

        return new OptimizeTable(name, onCluster, partition, includeFinal, deduplicate);
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
        var only = ParseKeyword(Keyword.ONLY);
        var tableNames = new Sequence<TruncateTableTarget>(ParseCommaSeparated(ParseObjectName).Select(o => new TruncateTableTarget(o)));
        var partitions = ParseInit(ParseKeyword(Keyword.PARTITION),
            () => { return ExpectParens(() => ParseCommaSeparated(ParseExpr)); });

        TruncateIdentityOption? identity = null;
        TruncateCascadeOption? cascade = null;

        if (_dialect is PostgreSqlDialect or GenericDialect)
        {
            if (ParseKeywordSequence(Keyword.RESTART, Keyword.IDENTITY))
            {
                identity = TruncateIdentityOption.Restart;

            }
            else if (ParseKeywordSequence(Keyword.CONTINUE, Keyword.IDENTITY))
            {
                identity = TruncateIdentityOption.Continue;
            }

            if (ParseKeyword(Keyword.CASCADE))
            {
                cascade = TruncateCascadeOption.Cascade;
            }
            else if (ParseKeyword(Keyword.RESTRICT))
            {
                cascade = TruncateCascadeOption.Restrict;
            }
        }

        var onCluster = ParseOptionalOnCluster();

        return new Truncate(tableNames, table, only, partitions, identity, cascade, onCluster);
    }

    public AttachDuckDbDatabase ParseAttachDuckDbDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var ifNotExists = ParseIfNotExists();
        var path = ParseIdentifier();
        var alias = ParseInit(ParseKeyword(Keyword.AS), ParseIdentifier);

        var attachOptions = ParseAttachDuckDbDatabaseOptions();
        return new AttachDuckDbDatabase(ifNotExists, database, path, alias, attachOptions);
    }

    public DetachDuckDbDatabase ParseDetachDuckDbDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var ifExists = ParseIfExists();
        var alias = ParseIdentifier();
        return new DetachDuckDbDatabase(ifExists, database, alias);
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

        var (fieldType, trailingBracket, _) = ParseDataTypeHelper();

        return (new StructField(fieldType, fieldName), trailingBracket);
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
                if (w.Keyword is Keyword.WITH or Keyword.WITHOUT)
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
        var windowName = ParseInit(PeekToken() is Word { Keyword: Keyword.undefined }, () => MaybeParse(ParseIdentifier));
        var partitionBy = ParseInit(ParseKeywordSequence(Keyword.PARTITION, Keyword.BY), () => ParseCommaSeparated(ParseExpr));
        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () => ParseCommaSeparated(ParseOrderByExpr));
        var windowFrame = ParseInit(!ConsumeToken<RightParen>(), () =>
        {
            var windowFrame = ParseWindowFrame();
            ExpectRightParen();
            return windowFrame;
        });

        return new WindowSpec(partitionBy, orderBy, windowFrame, windowName);
    }

    public ProcedureParam ParseProcedureParam()
    {
        var name = ParseIdentifier();
        var dataType = ParseDataType();
        return new ProcedureParam(name, dataType);
    }

    public DateTimeField ParseDateTimeField()
    {
        var token = NextToken();

        if (_dialect.AllowExtractSingleQuotes && token is SingleQuotedString)
        {
            PrevToken();
            var custom = ParseIdentifier();
            return new DateTimeField.Custom(custom);
        }

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
            _ when _dialect.AllowExtractSingleQuotes => ParseCustomDate(),
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

    public bool NextTokenIsTemporalUnit()
    {
        var next = PeekToken();

        return next is Word w && Extensions.DateTimeFields.Any(kwd => kwd == w.Keyword);
    }

    public Subscript ParseSubscriptInner()
    {
        var lowerBound = ParseInit(!ConsumeToken<Colon>(), ParseExpr);

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

        var stride = ParseInit(!ConsumeToken<RightBracket>(), ParseExpr);

        if (stride != null)
        {
            ExpectToken<RightBracket>();
        }

        return new Subscript.Slice(lowerBound, upperBound, stride);
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

    public Owner ParseOwner()
    {
        var owner = ParseOneOfKeywords(Keyword.CURRENT_USER, Keyword.CURRENT_ROLE, Keyword.SESSION_USER);

        return owner switch
        {
            Keyword.CURRENT_USER => new Owner.CurrentUser(),
            Keyword.CURRENT_ROLE => new Owner.CurrentRole(),
            Keyword.SESSION_USER => new Owner.SessionUser(),
            _ => ParseOwnerName()
        };

        Owner ParseOwnerName()
        {
            var ident = ParseIdentifier();
            return new Owner.Identity(ident);
        }
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
    /// <summary>
    /// Parse DuckDb macro argument
    /// </summary>
    /// <returns>Macro argument</returns>
    public MacroArg ParseMacroArg()
    {
        var name = ParseIdentifier();
        var defaultExpression = ParseInit(ConsumeToken<Tokens.Assignment>() || ConsumeToken<RightArrow>(), ParseExpr);

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

    public CreateTrigger ParseCreateTrigger(bool orReplace, bool isConstraint)
    {
        if (_dialect is not PostgreSqlDialect and not GenericDialect)
        {
            PrevToken();
            throw Expected("an object of type after CREATE", PeekToken());
        }

        var name = ParseObjectName();
        var period = ParseTriggerPeriod();
        var events = ParseKeywordSeparated(Keyword.OR, ParseTriggerEvent);
        ExpectKeyword(Keyword.ON);
        var tableName = ParseObjectName();

        var referencedTableName = ParseInit(ParseKeyword(Keyword.FROM), () => ParseObjectName(true));

        var characteristics = ParseConstraintCharacteristics();

        Sequence<TriggerReferencing>? referencing = null;

        if (ParseKeyword(Keyword.REFERENCING))
        {
            while (true)
            {
                var refer = ParseTriggerReferencing();
                if (refer != null)
                {
                    referencing ??= new Sequence<TriggerReferencing>();
                    referencing.Add(refer);
                }
                else
                {
                    break;
                }
            }
        }

        ExpectKeyword(Keyword.FOR);

        var includeEach = ParseKeyword(Keyword.EACH);
        var triggerObject = ParseOneOfKeywords(Keyword.ROW, Keyword.STATEMENT) switch
        {
            Keyword.ROW => TriggerObject.Row,
            Keyword.STATEMENT => TriggerObject.Statement,
            _ => throw Expected("ROW or STATEMENT")
        };

        var condition = ParseInit(ParseKeyword(Keyword.WHEN), ParseExpr);

        ExpectKeyword(Keyword.EXECUTE);

        var execBody = ParseTriggerExecBody();

        return new CreateTrigger(name)
        {
            OrReplace = orReplace,
            IsConstraint = isConstraint,
            Period = period,
            Events = events,
            TableName = tableName,
            ReferencedTableName = referencedTableName,
            Referencing = referencing,
            TriggerObject = triggerObject,
            IncludeEach = includeEach,
            Condition = condition,
            ExecBody = execBody,
            Characteristics = characteristics
        };
    }

    public DropTrigger ParseDropTrigger()
    {
        if (_dialect is not PostgreSqlDialect or not GenericDialect)
        {
            PrevToken();
            throw Expected("an object of type after DROP", PeekToken());
        }

        var ifExists = ParseIfExists();
        var triggerName = ParseObjectName();
        ExpectKeyword(Keyword.ON);
        var tableName = ParseObjectName();
        var optionKeyword = ParseOneOfKeywords(Keyword.CASCADE, Keyword.RESTRICT);

        var option = optionKeyword switch
        {
            Keyword.CASCADE => ReferentialAction.Cascade,
            Keyword.RESTRICT => ReferentialAction.Restrict,
            _ => throw Expected("CASCADE or RESTRICT")
        };

        return new DropTrigger(ifExists, triggerName, tableName, option);
    }

    public TriggerPeriod ParseTriggerPeriod()
    {
        return ExpectOneOfKeywords(Keyword.BEFORE, Keyword.AFTER, Keyword.INSTEAD) switch
        {
            Keyword.BEFORE => TriggerPeriod.Before,
            Keyword.AFTER => TriggerPeriod.After,
            Keyword.INSTEAD => ParseInstead(),
            _ => throw Expected("BEFORE, AFTER, INSTEAD")
        };

        TriggerPeriod ParseInstead()
        {
            ExpectKeyword(Keyword.OF);
            return TriggerPeriod.InsteadOf;
        }
    }

    public TriggerEvent ParseTriggerEvent()
    {
        return ExpectOneOfKeywords(Keyword.INSERT, Keyword.UPDATE, Keyword.DELETE, Keyword.TRUNCATE) switch
        {
            Keyword.INSERT => new TriggerEvent.Insert(),
            Keyword.UPDATE => ParseTriggerUpdate(),
            Keyword.DELETE => new TriggerEvent.Delete(),
            Keyword.TRUNCATE => new TriggerEvent.Truncate(),
            _ => throw Expected("INSERT, UPDATE, DELETE, TRUNCATE")
        };

        TriggerEvent ParseTriggerUpdate()
        {
            if (ParseKeyword(Keyword.OF))
            {
                var cols = ParseCommaSeparated(ParseIdentifier);

                return new TriggerEvent.Update(cols);
            }

            return new TriggerEvent.Update();
        }
    }

    public TriggerReferencing? ParseTriggerReferencing()
    {
        TriggerReferencingType referType = ParseOneOfKeywords(Keyword.OLD, Keyword.NEW) switch
        {
            Keyword.OLD => TriggerReferencingType.OldTable,
            Keyword.NEW => TriggerReferencingType.NewTable,
            _ => throw Expected("OLD or NEW")
        };

        var isAs = ParseKeyword(Keyword.AS);

        var transitionRelationName = ParseObjectName();

        return new TriggerReferencing(isAs, transitionRelationName, referType);
    }

    public TriggerExecBody ParseTriggerExecBody()
    {
        TriggerExecBodyType bodyType = ExpectOneOfKeywords(Keyword.FUNCTION, Keyword.PROCEDURE) switch
        {
            Keyword.FUNCTION => TriggerExecBodyType.Function,
            Keyword.PROCEDURE => TriggerExecBodyType.Procedure,
            _ => throw Expected("FUNCTION or PROCEDURE")
        };
        var description = ParseFunctionDescription();

        return new TriggerExecBody(bodyType, description);
    }

    public DropProcedure ParseDropProcedure()
    {
        var ifExists = ParseIfExists();
        var procDesc = ParseCommaSeparated(ParseFunctionDescription);
        var option = ParseOptionalReferentialAction();

        return new DropProcedure(ifExists, procDesc, option);
    }

    public FunctionDesc ParseFunctionDescription()
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

        return new FunctionDesc(name, args);
    }

    private DropSecret ParseDropSecret(bool temporary, bool persistent)
    {
        var ifExists = ParseIfExists();
        var name = ParseIdentifier();
        var storageSpecifier = ParseInit(ParseKeyword(Keyword.FROM), ParseIdentifier);

        bool? temp = (temporary, persistent) switch
        {
            (true, false) => true,
            (false, true) => false,
            (false, false) => null,
            _ => throw Expected("TEMPORARY or PERSISTENT", PeekToken())
        };

        return new DropSecret(ifExists, temp, name, storageSpecifier);
    }

    public ReferentialAction? ParseOptionalReferentialAction()
    {
        var keyword = ParseOneOfKeywords(Keyword.CASCADE, Keyword.RESTRICT);

        return keyword switch
        {
            Keyword.CASCADE => ReferentialAction.Cascade,
            Keyword.RESTRICT => ReferentialAction.Restrict,
            _ => ReferentialAction.None
        };
    }
    /// <summary>
    ///  DROP FUNCTION [ IF EXISTS ] name [ ( [ [ argmode ] [ argname ] argtype [, ...] ] ) ] [, ...]
    /// [ CASCADE | RESTRICT ]
    /// </summary>
    public DropFunction ParseDropFunction()
    {
        var ifExists = ParseIfExists();
        var funcDesc = ParseCommaSeparated(ParseFunctionDesc);
        var option = ParseOptionalReferentialAction();

        return new DropFunction(ifExists, funcDesc, option.Value);

        FunctionDesc ParseFunctionDesc()
        {
            var name = ParseObjectName();
            Sequence<OperateFunctionArg>? args = null;

            if (ConsumeToken<LeftParen>())
            {
                if (ConsumeToken<RightParen>())
                {
                    return new FunctionDesc(name);
                }

                var opArgs = ParseCommaSeparated(ParseFunctionArg);
                ExpectRightParen();
                args = opArgs;
            }

            return new FunctionDesc(name, args);
        }
    }

    public DropPolicy ParseDropPolicy()
    {
        var ifExists = ParseIfExists();
        var name = ParseIdentifier();
        ExpectKeyword(Keyword.ON);
        var tableName = ParseObjectName();
        var option = ParseOptionalReferentialAction();

        return new DropPolicy(ifExists, name, tableName, option);
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

        var include = ParseInit(ParseKeyword(Keyword.INCLUDE), () =>
            {
                return ExpectParens(() => ParseCommaSeparated(ParseIdentifier));
            });

        var nullsDistinct = ParseInit<bool?>(ParseKeyword(Keyword.NULLS), () =>
            {
                var not = ParseKeyword(Keyword.NOT);
                ExpectKeyword(Keyword.DISTINCT);
                return !not;
            });

        var withExpressions = ParseInit(
            _dialect.SupportsCreateIndexWithClause && ParseKeyword(Keyword.WITH), () =>
            {
                return ExpectParens(() => ParseCommaSeparated(ParseExpr));
            });

        var predicate = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);

        return new Ast.CreateIndex(indexName, tableName)
        {
            Using = @using,
            Columns = columns,
            Unique = unique,
            IfNotExists = ifNotExists,
            Concurrently = concurrently,
            Include = include,
            NullsDistinct = nullsDistinct,
            With = withExpressions,
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

    public ClusteredBy? ParseOptionalClusteredBy()
    {
        return ParseInit(
            _dialect is HiveDialect or GenericDialect && ParseKeywordSequence(Keyword.CLUSTERED, Keyword.BY),
            () =>
            {
                var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
                var sortedBy = ParseInit(ParseKeywordSequence(Keyword.SORTED, Keyword.BY),
                    () =>
                    {
                        return ExpectParens(() => ParseCommaSeparated(ParseOrderByExpr));
                    });

                ExpectKeyword(Keyword.INTO);
                var numBuckets = ParseNumberValue();
                ExpectKeyword(Keyword.BUCKETS);

                return new ClusteredBy(columns, sortedBy, numBuckets);
            });
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

        var onCluster = ParseOptionalOnCluster();

        var like = ParseInit<ObjectName?>(ParseKeyword(Keyword.LIKE) || ParseKeyword(Keyword.ILIKE), () => ParseObjectName(allowUnquotedHyphen));

        var clone = ParseInit<ObjectName?>(ParseKeyword(Keyword.CLONE), () => ParseObjectName(allowUnquotedHyphen));

        var (columns, constraints) = ParseColumns();
        CommentDef? comment = null;

        if (_dialect is HiveDialect && ParseKeyword(Keyword.COMMENT))
        {
            var next = NextToken();
            if (next is SingleQuotedString s)
            {
                comment = new CommentDef.AfterColumnDefsWithoutEq(s.Value);
            }
            else
            {
                throw Expected("comment", next);
            }
        }

        // SQLite supports `WITHOUT ROWID` at the end of `CREATE TABLE`
        var withoutRowId = ParseKeywordSequence(Keyword.WITHOUT, Keyword.ROWID);

        var hiveDistribution = ParseHiveDistribution();
        var clusteredBy = ParseOptionalClusteredBy();
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


                var peeked = PeekToken();
                var parameters = ParseInit(peeked is LeftParen,
                    () =>
                    {
                        return ExpectParens(() => ParseCommaSeparated(ParseIdentifier));
                    });
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

        var primaryKey = ParseInit(
            _dialect is ClickHouseDialect or GenericDialect && ParseKeywordSequence(Keyword.PRIMARY, Keyword.KEY),
             ParseExpr);

        var orderBy = ParseInit(ParseKeywordSequence(Keyword.ORDER, Keyword.BY), () =>
        {
            OneOrManyWithParens<Expression> orderExpression;

            if (ConsumeToken<LeftParen>())
            {
                var cols = ParseInit(PeekToken() is not RightParen, () => ParseCommaSeparated(ParseExpr));

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

        if (_dialect is not HiveDialect && ParseKeyword(Keyword.COMMENT))
        {
            PrevToken();
            comment = ParseOptionalInlineComment();
        }

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
            ClusteredBy = clusteredBy,
            Options = createTableConfig.Options
        };
    }

    private Ident? ParseOptionalOnCluster()
    {
        if (ParseKeywordSequence(Keyword.ON, Keyword.CLUSTER))
        {
            return ParseIdentifier();
        }

        return null;
    }

    public CreateTableConfiguration ParseOptionalCreateTableConfig()
    {

        WrappedCollection<Ident>? clusterBy = null;
        Sequence<SqlOption>? options = null;

        var partitionBy = ParseInit(
            _dialect is BigQueryDialect or PostgreSqlDialect && ParseKeywordSequence(Keyword.PARTITION, Keyword.BY),
            ParseExpr);

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
                options ??= [];
                options.Add(new ColumnOptionDef(opt));
            }
            else if (_dialect is MySqlDialect or SnowflakeDialect or GenericDialect && ParseKeyword(Keyword.COLLATE))
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

    public Keyword? ParseSqLiteConflictClause()
    {
        if (!ParseKeywordSequence(Keyword.ON, Keyword.CONFLICT)) return null;
        return ParseOneOfKeywords(Keyword.ROLLBACK, Keyword.ABORT, Keyword.FAIL, Keyword.IGNORE, Keyword.REPLACE);
    }

    public ColumnOption? ParseOptionalColumnOption()
    {
        var option = _dialect.ParseColumnOption(this);
        if (option!=null)
        {
            return option;
        }

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
            var characteristics = ParseConstraintCharacteristics();
            return new ColumnOption.Unique(true)
            {
                Characteristics = characteristics,
            };
        }

        if (ParseKeyword(Keyword.UNIQUE))
        {
            var conflict = _dialect is SQLiteDialect ? ParseSqLiteConflictClause() : null;
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
            return new ColumnOption.DialectSpecific([new Word("AUTO_INCREMENT") ]);
        }

        if (_dialect is SQLiteDialect or GenericDialect && ParseKeyword(Keyword.AUTOINCREMENT))
        {
            // Support AUTOINCREMENT for SQLite
            return new ColumnOption.DialectSpecific([new Word("AUTOINCREMENT") ]);
        }

        if (_dialect.SupportsAscDescInColumnDefinition && ParseKeyword(Keyword.ASC))
        {
            return new ColumnOption.DialectSpecific([new Word("ASC")]);
        }

        if (_dialect.SupportsAscDescInColumnDefinition && ParseKeyword(Keyword.DESC))
        {
            return new ColumnOption.DialectSpecific([new Word("DESC")]);
        }

        if (_dialect is MySqlDialect or GenericDialect && ParseKeywordSequence(Keyword.ON, Keyword.UPDATE))
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

        if (_dialect is MsSqlDialect or GenericDialect && ParseKeyword(Keyword.IDENTITY))
        {
            var parameters = ParseInit(ConsumeToken<LeftParen>(),
                () =>
                {
                    var seed = ParseNumber();
                    ExpectToken<Comma>();
                    var increment = ParseNumber();
                    ExpectToken<RightParen>();

                    return new IdentityPropertyFormatKind.FunctionCall(new IdentityParameters(seed, increment));
                });

            return new ColumnOption.Identity(new IdentityPropertyKind.Identity(new IdentityProperty(parameters, null)));
        }

        if (_dialect is SQLiteDialect or GenericDialect && ParseKeywordSequence(Keyword.ON, Keyword.CONFLICT))
        {
            return new ColumnOption.OnConflict(ExpectOneOfKeywords(Keyword.ROLLBACK, Keyword.ABORT, Keyword.FAIL, Keyword.IGNORE, Keyword.REPLACE));
        }

        return null;
    }

    public Tag ParseTag()
    {
        var name = ParseIdentifier();
        ExpectToken<Equal>();
        var value = ParseLiteralString();

        return new Tag(name, value);
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

    bool ParseAnyOptionalTableConstraints(Action<TableConstraint> action)
    {
        bool any = false;
        while (true)
        {
            var constraint = ParseOptionalTableConstraint(any, false);
            if (constraint == null) return any;
            action(constraint);
            any = true;
        }
    }

    public TableConstraint? ParseOptionalTableConstraint(bool isSubsequentConstraint, bool isAlterTable)
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

            if (_dialect is PostgreSqlDialect && isAlterTable && PeekToken() is Word { Keyword: Keyword.USING })
            {
                ParseKeywordSequence(Keyword.USING, Keyword.INDEX);
                var indexName = ParseIdentifier();
                var characteristics = ParseConstraintCharacteristics();

                return new TableConstraint.PostgresAlterTableIndex(name, indexName)
                {
                    Characteristics = characteristics,
                    IsPrimaryKey = isPrimary
                };
            }
            else
            {
                // Optional constraint name
                var identName = MaybeParse(ParseIdentifier) ?? name;
                var columns = ParseParenthesizedColumnList(IsOptional.Mandatory, false);
                var conflict = _dialect is SQLiteDialect ? ParseSqLiteConflictClause() : null;
                var characteristics = ParseConstraintCharacteristics();
                return new TableConstraint.Unique(columns)
                {
                    Name = identName,
                    IsPrimaryKey = isPrimary,
                    Conflict = conflict,
                    Characteristics = characteristics
                };
            }
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
        var isMsSql = _dialect is MsSqlDialect;

        return PeekToken() switch
        {
            Word w when isMsSql && w.Keyword == Keyword.HEAP => new SqlOption.Identifier(ParseIdentifier()),
            Word p when isMsSql && p.Keyword is Keyword.PARTITION => ParseOptionPartition(),
            Word p when isMsSql && p.Keyword is Keyword.CLUSTERED => ParseOptionClustered(),
            _ => ParseOption()
        };

        SqlOption ParseOption()
        {
            var name = ParseIdentifier();
            ExpectToken<Equal>();
            var value = ParseExpr();
            return new SqlOption.KeyValue(name, value);
        }

        SqlOption ParseOptionPartition()
        {
            ExpectKeyword(Keyword.PARTITION);
            ExpectLeftParen();
            var columnName = ParseIdentifier();

            ExpectKeyword(Keyword.RANGE);
            PartitionRangeDirection? rangeDirection = null;

            if (ParseKeyword(Keyword.LEFT))
            {
                rangeDirection = PartitionRangeDirection.Left;
            }
            else if (ParseKeyword(Keyword.RIGHT))
            {
                rangeDirection = PartitionRangeDirection.Right;
            }

            ExpectKeywords(Keyword.FOR, Keyword.VALUES);
            ExpectLeftParen();

            var forValues = ParseCommaSeparated(ParseExpr);

            ExpectRightParen();
            ExpectRightParen();

            return new SqlOption.Partition(columnName, forValues, rangeDirection);
        }

        SqlOption ParseOptionClustered()
        {
            if (ParseKeywordSequence(Keyword.CLUSTERED, Keyword.COLUMNSTORE, Keyword.INDEX, Keyword.ORDER))
            {
                return new SqlOption.Clustered(new TableOptionsClustered.ColumnstoreIndexOrder(
                        ParseParenthesizedColumnList(IsOptional.Mandatory, false)));
            }
            else if (ParseKeywordSequence(Keyword.CLUSTERED, Keyword.COLUMNSTORE, Keyword.INDEX))
            {
                return new SqlOption.Clustered(new TableOptionsClustered.ColumnstoreIndex());
            }
            else if (ParseKeywordSequence(Keyword.CLUSTERED, Keyword.INDEX))
            {
                var columns = ExpectParens(() =>
                {
                    return ParseCommaSeparated(() =>
                    {
                        var name = ParseIdentifier();
                        var asc = ParseAscDesc();
                        return new ClusteredIndex(name, asc);
                    });
                });

                return new SqlOption.Clustered(new TableOptionsClustered.Index(columns));
            }

            throw new ParserException("invalid CLUSTERED sequence");
        }
    }

    public ViewColumnDef ParseViewColumn()
    {
        var name = ParseIdentifier();
        var parseComment = (_dialect is BigQueryDialect or GenericDialect && ParseKeyword(Keyword.OPTIONS)) ||
                           (_dialect is SnowflakeDialect or GenericDialect && ParseKeyword(Keyword.COMMENT));

        var options = ParseInit(parseComment,
             () =>
             {
                 PrevToken();
                 var option = ParseOptionalColumnOption();
                 return new Sequence<ColumnOption>{ option! };
             });

        var dataType = ParseInit(_dialect is ClickHouseDialect, ParseDataType);

        return new ViewColumnDef(name, dataType, options);
    }

    private AlterTableOperation ParseAlterTableOperation()
    {
        AlterTableOperation operation;

        if (ParseKeyword(Keyword.ADD))
        {
            var constraint = ParseOptionalTableConstraint(false, true);
            if (constraint != null)
            {
                operation = new AddConstraint(constraint);
            }
            else if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.PROJECTION))
            {
                return ParseAlterTableAddProjection();
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
                        newPartitions.Add(new Partition.Partitions(partition));
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
        else if (ParseKeywordSequence(Keyword.CLEAR, Keyword.PROJECTION) &&
                 _dialect is ClickHouseDialect or GenericDialect)
        {
            var ifExists = ParseIfExists();
            var name = ParseIdentifier();
            var partition = ParseKeywordSequence(Keyword.IN, Keyword.PARTITION) ? ParseIdentifier() : null;
            return new ClearProjection(ifExists, name, partition);
        }
        else if (ParseKeywordSequence(Keyword.MATERIALIZE, Keyword.PROJECTION) &&
                 _dialect is ClickHouseDialect or GenericDialect)
        {
            var ifExists = ParseIfExists();
            var name = ParseIdentifier();
            var partition = ParseKeywordSequence(Keyword.IN, Keyword.PARTITION) ? ParseIdentifier() : null;
            return new MaterializeProjection(ifExists, name, partition);
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
            else if (ParseKeyword(Keyword.PROJECTION))
            {
                var ifExists = ParseIfExists();
                var name = ParseIdentifier();
                return new DropProjection(ifExists, name);
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
                var @using = ParseInit(isPostgres && ParseKeyword(Keyword.USING), ParseExpr);

                op = new AlterColumnOperation.SetDataType(dataType, @using);
            }
            else if (ParseKeywordSequence(Keyword.ADD, Keyword.GENERATED))
            {
                GeneratedAs? genAs = ParseKeyword(Keyword.ALWAYS) ? GeneratedAs.Always :
                    ParseKeywordSequence(Keyword.BY, Keyword.DEFAULT) ? GeneratedAs.ByDefault :
                    null;

                ExpectKeywords(Keyword.AS, Keyword.IDENTITY);

                Sequence<SequenceOptions>? options = ParseInit(PeekToken() is LeftParen, () => ExpectParens(ParseCreateSequenceOptions));

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
        else if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.ATTACH))
        {
            return new AttachPartition(ParsePartOrPartition());
        }
        else if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.DETACH))
        {
            return new DetachPartition(ParsePartOrPartition());
        }
        else if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.FREEZE))
        {
            var partition = ParsePartOrPartition();
            var withName = ParseInit(ParseKeyword(Keyword.WITH),
                () =>
                {
                    ExpectKeyword(Keyword.NAME);
                    return ParseIdentifier();
                });
            return new FreezePartition(partition, withName);
        }
        else if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.UNFREEZE))
        {
            var partition = ParsePartOrPartition();
            var withName = ParseInit(ParseKeyword(Keyword.WITH),
                () =>
                {
                    ExpectKeyword(Keyword.NAME);
                    return ParseIdentifier();
                });
            return new UnfreezePartition(partition, withName);
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

    public GroupByExpression? ParseOptionalGroupBy()
    {
        if (ParseKeywordSequence(Keyword.GROUP, Keyword.BY))
        {
            var expressions = ParseKeyword(Keyword.ALL) ? null : ParseCommaSeparated(ParseGroupByExpr);

            Sequence<GroupByWithModifier>? modifiers = null;

            if (_dialect is ClickHouseDialect or GenericDialect)
            {
                while (true)
                {
                    if (!ParseKeyword(Keyword.WITH))
                    {
                        break;
                    }

                    modifiers ??= [];

                    var keyword = ExpectOneOfKeywords(Keyword.ROLLUP, Keyword.CUBE, Keyword.TOTALS);

                    switch (keyword)
                    {
                        case Keyword.ROLLUP:
                            modifiers.Add(GroupByWithModifier.Rollup);
                            break;

                        case Keyword.CUBE:
                            modifiers.Add(GroupByWithModifier.Cube);
                            break;

                        case Keyword.TOTALS:
                            modifiers.Add(GroupByWithModifier.Totals);
                            break;
                    }
                }
            }

            return expressions != null
                 ? new GroupByExpression.Expressions(expressions, modifiers)
                 : new GroupByExpression.All(modifiers);
        }

        return null;
    }

    public OrderBy? ParseOptionalOrderBy()
    {
        if (!ParseKeywordSequence(Keyword.ORDER, Keyword.BY)) { return null; }

        var orderByExpressions = ParseCommaSeparated(ParseOrderByExpr);
        var interpolate = _dialect is ClickHouseDialect or GenericDialect ? ParseInterpolations() : null;

        return new OrderBy(orderByExpressions, interpolate);
    }

    private Partition ParsePartOrPartition()
    {
        var keyword = ExpectOneOfKeywords(Keyword.PART, Keyword.PARTITION);

        return keyword switch
        {
            Keyword.PART => new Partition.Part(ParseExpr()),
            Keyword.PARTITION => new Partition.Expr(ParseExpr()),
            _ => throw Expected("PART or PARTITION")
        };
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

        var values = ParseInit(target is CopyTarget.Stdin,
            () =>
            {
                ExpectToken<SemiColon>();
                return ParseTabValue();
            });


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
    public (DataType, bool, ParserException?) ParseDataTypeHelper()
    {
        var token = NextToken();
        var trailingBracket = false;
        ParserException? parserException = null;

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
            Word { Keyword: Keyword.STRING } => new DataType.StringType(ParseOptionalPrecision()),
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
            Word { Keyword: Keyword.STRUCT } when _dialect is DuckDbDialect => ParseDuckDbStruct(),
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
            Word { Keyword: Keyword.TRIGGER } => new DataType.Trigger(),
            _ => ParseUnmatched()
        };

        if(parserException != null || _currentException != null)
        {
            return (data, trailingBracket, parserException ?? _currentException);
        }

        // Parse array data types. Note: this is postgresql-specific and different from
        // Keyword.ARRAY syntax from above
        while (ConsumeToken<LeftBracket>())
        {
            var size = ParseInit(_dialect is GenericDialect or DuckDbDialect or PostgreSqlDialect,
                () => (long?)MaybeParseNullable(ParseLiteralUnit));

            ExpectToken<RightBracket>();
            data = new DataType.Array(new ArrayElementTypeDef.SquareBracket(data, size));
        }

        return (data, trailingBracket, parserException);

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
            var (insideType, trailing, ex) = ParseDataTypeHelper();
            parserException = ex;

            trailingBracket = ExpectClosingAngleBracket(trailing);
            return new DataType.Array(new ArrayElementTypeDef.AngleBracket(insideType));
        }

        DataType ParseStruct()
        {
            PrevToken();
            (var fieldDefinitions, trailingBracket) = ParseStructTypeDef(ParseStructFieldDef);
            return new DataType.Struct(fieldDefinitions, StructBracketKind.AngleBrackets);
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
            var (modifiers, ex) = ParseOptionalTypeModifiers();
            parserException = ex;

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

        DataType ParseDuckDbStruct()
        {
            PrevToken();
            return new DataType.Struct(ParseDuckDbStructTypeDef(), StructBracketKind.Parentheses);
        }
        #endregion
    }

    private (ulong, string?) ParseDateTime64()
    {
        ExpectKeyword(Keyword.DATETIME64);

        return ExpectParens(() =>
        {
            var precision = ParseLiteralUnit();
            var timeZone = ParseInit(ConsumeToken<Comma>(), ParseLiteralString);

            return (precision, timeZone);
        });
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
            var (ident, ex) = ParseIdentifierWithClause(inTableClause);
            _currentException ??= ex;

            if (_currentException != null)
            {
                break;
            }

            idents.Add(ident!);
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

    public (Ident? Ident, ParserException? Exception) ParseIdentifierWithClause(bool inTableClause)
    {
        var token = NextToken();
        return token switch
        {
            Word word => (ParseIdent(word), null),
            SingleQuotedString s => (new Ident(s.Value, Symbols.SingleQuote), null),
            DoubleQuotedString s => (new Ident(s.Value, Symbols.DoubleQuote), null),
            _ when !_suppressExceptions => throw Expected("identifier", token),
            _ => (null, Expected("identifier", token))
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
            var scale = ParseInit(ConsumeToken<Comma>(), () => (ulong?)ParseLiteralUnit());

            ExpectRightParen();

            if (scale != null)
            {
                return new PrecisionAndScale(precision, scale.Value);
            }

            return new Precision(precision);
        }

        return new None();
    }

    public Delete ParseDelete()
    {
        Sequence<ObjectName>? tables = null;
        var withFromKeyword = true;

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

    public CommonTableExpression ParseCommonTableExpression()
    {
        var name = ParseIdentifier();

        CommonTableExpression? cte;

        if (ParseKeyword(Keyword.AS))
        {
            var isMaterialized = ParseInit<CteAsMaterialized?>(_dialect is PostgreSqlDialect, () =>
            {
                if (ParseKeyword(Keyword.MATERIALIZED))
                {
                    return CteAsMaterialized.Materialized;
                }

                if (ParseKeywordSequence(Keyword.NOT, Keyword.MATERIALIZED))
                {
                    return CteAsMaterialized.NotMaterialized;
                }

                return null;
            });

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
            CteAsMaterialized? isMaterialized = ParseInit<CteAsMaterialized?>(_dialect is PostgreSqlDialect, () =>
            {
                if (ParseKeyword(Keyword.MATERIALIZED))
                {
                    return CteAsMaterialized.Materialized;
                }
                if (ParseKeywordSequence(Keyword.NOT, Keyword.MATERIALIZED))
                {
                    return CteAsMaterialized.NotMaterialized;
                }

                return null;
            });
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
            var elementName = ParseInit(PeekToken() is LeftParen,
                () => ExpectParens(ParseLiteralString));

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

    public ProjectionSelect ParseProjectionSelect()
    {
        return ExpectParens(() =>
        {
            ExpectKeyword(Keyword.SELECT);
            var projection = ParseProjection();
            var groupBy = ParseOptionalGroupBy();
            var orderBy = ParseOptionalOrderBy();
            return new ProjectionSelect(projection, orderBy, groupBy);
        });
    }

    public AlterTableOperation ParseAlterTableAddProjection()
    {
        var ifNotExists = ParseIfNotExists();
        var name = ParseIdentifier();
        var query = ParseProjectionSelect();
        return new AddProjection(ifNotExists, name, query);
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

        var topBeforeDistinct = false;
        Top? top = null;
        if (_dialect.SupportsTopBeforeDistinct && ParseKeyword(Keyword.TOP))
        {
            top = ParseTop();
            topBeforeDistinct = true;
        }

        var distinct = ParseAllOrDistinct();

        if (!_dialect.SupportsTopBeforeDistinct && ParseKeyword(Keyword.TOP))
        {
            top = ParseTop();
        }

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

        var preWhere = ParseInit(_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.PREWHERE), ParseExpr);

        var selection = ParseInit(ParseKeyword(Keyword.WHERE), ParseExpr);

        var groupBy = ParseOptionalGroupBy();

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
            PreWhere = preWhere,
            TopBeforeDistinct = topBeforeDistinct
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
            
            if (ParseKeyword(Keyword.QUALIFY))
            {
                var qualifyExpr = ParseExpr();
                if (ParseKeyword(Keyword.WINDOW))
                {
                    return (ParseCommaSeparated(ParseNamedWindow), qualifyExpr, false);
                }

                return (null, qualifyExpr, false);
            }

            return (null, null, false);
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

    public CommentDef? ParseOptionalInlineComment()
    {
        return ParseInit<CommentDef>(ParseKeyword(Keyword.COMMENT), () =>
        {
            var hasEq = ConsumeToken<Equal>();
            var next = NextToken();
            if (next is SingleQuotedString str)
            {
                if (hasEq)
                {
                    return new CommentDef.WithEq(str.Value);
                }
                return new CommentDef.WithoutEq(str.Value);
            }

            throw Expected("Comment", next);
        });
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

    public Ident? ParseRollbackSavepoint()
    {
        if (!ParseKeyword(Keyword.TO))
        {
            return null;
        }

        ParseKeyword(Keyword.SAVEPOINT);
        return ParseIdentifier();
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
        var (clause, dbName) = ParseOneOfKeywords(Keyword.FROM, Keyword.IN) switch
        {
            Keyword.FROM => ((ShowClause?)ShowClause.From, ParseIdentifier()),
            Keyword.IN => (ShowClause.In, ParseIdentifier()),
            _ => (null, null)
        };

        var filter = ParseShowStatementFilter();
        return new ShowTables(extended, full, clause, dbName, filter);
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

        var filter = MaybeParse(ParseLiteralString);

        if (filter != null)
        {
            return new ShowStatementFilter.NoKeyword(filter);
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
            var global = ParseKeyword(Keyword.GLOBAL);

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
                    Global = global
                };
            }
            else if (ParseKeyword(Keyword.OUTER))
            {
                ExpectKeyword(Keyword.APPLY);
                join = new Join
                {
                    Relation = ParseTableFactor(),
                    JoinOperator = new JoinOperator.OuterApply(),
                    Global = global
                };
            }
            else if (ParseKeyword(Keyword.ASOF))
            {
                ExpectKeyword(Keyword.JOIN);

                var asOfRelation = ParseTableFactor();
                ExpectKeyword(Keyword.MATCH_CONDITION);
                var matchCondition = ExpectParens(ParseExpr);
                join = new Join(asOfRelation, new JoinOperator.AsOf(matchCondition, ParseJoinConstraint(false)), global);
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
                    Global = global
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

        var partitions = ParseInit(
            _dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.PARTITION), ParseIdentifiers);

        // Parse potential version qualifier
        var version = ParseTableVersion();

        // Postgres, MSSQL: table-valued functions:
        var args = ParseInit(ConsumeToken<LeftParen>(), ParseTableFunctionArgs);

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

    private TableFunctionArgs ParseTableFunctionArgs()
    {
        if (ConsumeToken<RightParen>())
        {
            return new TableFunctionArgs([]);
        }

        var args = new Sequence<FunctionArg>();
        Sequence<Setting>? settings;

        while (true)
        {
            settings = ParseSettings();

            if (settings != null)
            {
                break;
            }

            args.Add(ParseFunctionArgs());
            if (IsParseCommaSeparatedEnd())
            {
                break;
            }
        }

        ExpectRightParen();
        return new TableFunctionArgs(args, settings);
    }

    public TableFactor ParseMatchRecognize(TableFactor table)
    {
        MatchRecognizePattern pattern = null!;
        Sequence<Expression>? partitionBy = null;
        Sequence<OrderByExpression>? orderBy = null;
        Sequence<Measure>? measures = null;
        RowsPerMatch? rowsPerMatch = null;
        AfterMatchSkip? afterMatchSkip = null;

        var symbols = ExpectParens(() =>
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

                if (PeekSubQuery())
                {
                    return new PivotValueSource.Subquery(ParseQuery());
                }

                return new PivotValueSource.List(ParseCommaSeparated(ParseExpressionWithAlias));
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

    private bool PeekSubQuery()
    {
        if (ParseOneOfKeywords(Keyword.SELECT, Keyword.WITH) == Keyword.undefined)
        {
            return false;
        }

        PrevToken();
        return true;
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

        if (wildcardExpr is BinaryOp b && _dialect.SupportsEqualAliasAssignment && b.Left is Identifier leftIdent)
        {
            return new SelectItem.ExpressionWithAlias(b.Right, leftIdent.Ident.Value);
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
        var ilikeSelectItem = ParseInit(_dialect is SnowflakeDialect or GenericDialect, ParseOptionalSelectItemIlike);
        var optExclude = ParseInit(_dialect is GenericDialect or DuckDbDialect or SnowflakeDialect, ParseOptionalSelectItemExclude);
        var optExcept = ParseInit(_dialect.SupportsSelectWildcardExcept, ParseOptionalSelectItemExcept);

        var optReplace = ParseInit(_dialect is GenericDialect
            or BigQueryDialect
            or ClickHouseDialect
            or DuckDbDialect
            or SnowflakeDialect,
          ParseOptionalSelectItemReplace);

        var optRename = ParseInit(_dialect is GenericDialect or SnowflakeDialect, ParseOptionalSelectItemRename);

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

        if (!ParseKeyword(Keyword.ILIKE)) { return null; }

        var next = NextToken();
        var pattern = next switch
        {
            SingleQuotedString s => s.Value,
            _ => throw Expected("ilike pattern", next)
        };

        return new IlikeSelectItem(pattern);
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
        if (!ParseKeyword(Keyword.RENAME))
        {
            return null;
        }

        RenameSelectItem? optRename;

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

    public bool? ParseAscDesc()
    {
        if (ParseKeyword(Keyword.ASC))
        {
            return true;
        }

        if (ParseKeyword(Keyword.DESC))
        {
            return false;
        }

        return null;
    }
    /// <summary>
    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    /// </summary>
    /// <returns>Order By Expression</returns>
    public OrderByExpression ParseOrderByExpr()
    {
        var expr = ParseExpr();

        var asc = ParseAscDesc();

        bool? nullsFirst = null;
        if (ParseKeywordSequence(Keyword.NULLS, Keyword.FIRST))
        {
            nullsFirst = true;
        }
        else if (ParseKeywordSequence(Keyword.NULLS, Keyword.LAST))
        {
            nullsFirst = false;
        }

        var withFill = ParseInit(
            _dialect is ClickHouseDialect or GenericDialect && ParseKeywordSequence(Keyword.WITH, Keyword.FILL),
            ParseWithFill);

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

    public UtilityOption ParseUtilityOption()
    {
        var name = ParseIdentifier();

        var next = PeekToken();

        if (next is Comma or RightParen)
        {
            return new UtilityOption(name);
        }

        var arg = ParseExpr();

        return new UtilityOption(name, arg);
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

    public static string Found(Token token)
    {
        var location = token is EOF ? null : $", {token.Location}";
        return $", found {token}{location}";
    }
}

public record ParsedAction(Keyword Keyword, Sequence<Ident>? Idents = null);