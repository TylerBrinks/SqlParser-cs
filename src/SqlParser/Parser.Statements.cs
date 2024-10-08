using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Statement;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;
using Declare = SqlParser.Ast.Declare;
using Use = SqlParser.Ast.Use;

namespace SqlParser;

public partial class Parser
{
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
            Keyword.USE => ParseUse(),
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
            Keyword.OPTIMIZE when _dialect is ClickHouseDialect or GenericDialect => ParseOptimizeTable(),

            _ => throw Expected("a SQL statement", PeekToken())
        };
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

    public Statement ParseAssert()
    {
        var condition = ParseExpr();
        var message = ParseKeywordSequence(Keyword.AS) ? ParseExpr() : null;

        return new Assert(condition, message);
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

        if (ParseKeyword(Keyword.POLICY))
        {
            return ParseCreatePolicy();
        }

        if (ParseKeyword(Keyword.EXTERNAL))
        {
            return new Statement.CreateTable(ParseCreateExternalTable(orReplace));
        }

        if (ParseKeyword(Keyword.FUNCTION))
        {
            return ParseCreateFunction(orReplace, temporary);
        }

        if (ParseKeyword(Keyword.TRIGGER))
        {
            return ParseCreateTrigger(orReplace, false);
        }

        if (ParseKeywordSequence(Keyword.CONSTRAINT, Keyword.TRIGGER))
        {
            return ParseCreateTrigger(orReplace, true);
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

    public Statement ParseCreatePolicy()
    {
        var name = ParseIdentifier();
        ExpectKeyword(Keyword.ON);
        var tableName = ParseObjectName();

        CreatePolicyType? policyType = null;
        Expression? @using = null;
        Expression? withCheck = null;
        CreatePolicyCommand? command = null;
        Sequence<Owner>? to = null;

        if (ParseKeyword(Keyword.AS))
        {
            var keyword = ExpectOneOfKeywords(Keyword.PERMISSIVE, Keyword.RESTRICTIVE);

            policyType = keyword switch
            {
                Keyword.PERMISSIVE => CreatePolicyType.Permissive,
                Keyword.RESTRICTIVE => CreatePolicyType.Restrictive,
            };
        }

        if (ParseKeyword(Keyword.FOR))
        {
            var keyword = ExpectOneOfKeywords(Keyword.ALL, Keyword.SELECT, Keyword.INSERT, Keyword.UPDATE, Keyword.DELETE);

            command = keyword switch
            {
                Keyword.ALL => CreatePolicyCommand.All,
                Keyword.SELECT => CreatePolicyCommand.Select,
                Keyword.INSERT => CreatePolicyCommand.Insert,
                Keyword.UPDATE => CreatePolicyCommand.Update,
                Keyword.DELETE => CreatePolicyCommand.Delete
            };
        }

        if (ParseKeyword(Keyword.TO))
        {
            to = ParseCommaSeparated(ParseOwner);
        }

        if (ParseKeyword(Keyword.USING))
        {
            @using = ExpectParens(ParseExpr);
        }

        if (ParseKeywordSequence(Keyword.WITH, Keyword.CHECK))
        {
            withCheck = ExpectParens(ParseExpr);
        }

        return new CreatePolicy(name, tableName)
        {
            Command = command,
            PolicyType = policyType,
            To = to,
            Using = @using,
            WithCheck = withCheck
        };
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
        else if (ParseKeyword(Keyword.DATABASE))
        {
            objectType = ObjectType.Database;
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
        else if (ParseKeyword(Keyword.POLICY))
        {
            return ParseDropPolicy();
        }
        else if (ParseKeyword(Keyword.PROCEDURE))
        {
            return ParseDropProcedure();
        }
        else if (ParseKeyword(Keyword.SECRET))
        {
            return ParseDropSecret(temporary, persistent);
        }
        else if (ParseKeyword(Keyword.TRIGGER))
        {
            return ParseDropTrigger();
        }

        if (objectType == null)
        {
            throw Expected("TABLE, VIEW, INDEX, ROLE, SCHEMA, DATABASE, FUNCTION, PROCEDURE, STAGE, TRIGGER, SECRET or SEQUENCE after DROP", PeekToken());
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
    /// <summary>
    /// KILL [CONNECTION | QUERY | MUTATION] processlist_id
    /// </summary>
    /// <param name="describeAlias"></param>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Statement ParseExplain(DescribeAlias describeAlias)
    {
        var analyze = false;
        var verbose = false;
        var format = AnalyzeFormat.None;
        Sequence<UtilityOption>? options = null;

        // Note: DuckDB is compatible with PostgreSQL syntax for this statement,
        // although not all features may be implemented.
        if (_dialect.SupportsExplainWithUtilityOptions && describeAlias == DescribeAlias.Explain && PeekToken() is LeftParen)
        {
            options = ParseUtilityOptions();
        }
        else
        {
            analyze = ParseKeyword(Keyword.ANALYZE);
            verbose = ParseKeyword(Keyword.VERBOSE);
            format = ParseInit(ParseKeyword(Keyword.FORMAT), ParseAnalyzeFormat);
        }

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
                Options = options
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
            // Only allow to use TABLE keyword for DESC|DESCRIBE statement
            var hasTableKeyword = _dialect.DescribeRequiresTableKeyword && ParseKeyword(Keyword.TABLE);
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

    public Statement ParseAttachDatabase()
    {
        var database = ParseKeyword(Keyword.DATABASE);
        var databaseFileName = ParseExpr();
        ExpectKeyword(Keyword.AS);
        var schemaName = ParseIdentifier();

        return new AttachDatabase(schemaName, databaseFileName, database);
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
                    var onCluster = ParseOptionalOnCluster();
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

                    return new AlterTable(tableName, ifExists, only, operations, location, onCluster);
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

        if (_dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.VARIABLES))
        {
            return new ShowVariables(ParseShowStatementFilter(), global, session);
        }

        if (_dialect is MySqlDialect or GenericDialect && ParseKeyword(Keyword.STATUS))
        {
            return new ShowStatus(ParseShowStatementFilter(), session, global);
        }

        return new ShowVariable(ParseIdentifiers());
    }

    public Statement ParseUse()
    {
        Keyword? parsedKeyword = null;

        if (_dialect is HiveDialect)
        {
            if (ParseKeyword(Keyword.DEFAULT))
            {
                return new Statement.Use(new Use.Default());
            }
        }

        if (_dialect is DatabricksDialect)
        {
            parsedKeyword = ParseOneOfKeywords(Keyword.CATALOG, Keyword.DATABASE, Keyword.SCHEMA);
        }
        else if (_dialect is SnowflakeDialect)
        {
            parsedKeyword = ParseOneOfKeywords(Keyword.DATABASE, Keyword.SCHEMA, Keyword.WAREHOUSE);
        }

        var objectName = ParseObjectName();
        Use result = parsedKeyword switch
        {
            Keyword.CATALOG => new Use.Catalog(objectName),
            Keyword.DATABASE => new Use.Database(objectName),
            Keyword.SCHEMA => new Use.Schema(objectName),
            Keyword.WAREHOUSE => new Use.Warehouse(objectName),
            _ => new Use.Object(objectName)
        };

        return new Statement.Use(result);
    }

    public Statement ParseShowCollation()
    {
        return new ShowCollation(ParseShowStatementFilter());
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

    public Statement ParseUnload()
    {
        Query query = ExpectParens(() => ParseQuery());

        ExpectKeyword(Keyword.TO);

        var to = ParseIdentifier();

        var withOptions = ParseOptions(Keyword.WITH);

        return new Unload(query, to, withOptions);
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
}