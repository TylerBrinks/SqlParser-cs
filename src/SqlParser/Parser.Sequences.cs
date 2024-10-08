using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;

namespace SqlParser;

public partial class Parser
{
   
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

    public Sequence<SqlOption>? MaybeParseOptions(Keyword keyword)
    {
        if (PeekToken() is Word w && w.Keyword == keyword)
        {
            return ParseOptions(keyword);
        }

        return null;
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

    public Sequence<StructField> ParseDuckDbStructTypeDef()
    {
        ExpectKeyword(Keyword.STRUCT);
        return ExpectParens(() =>
        {
            return ParseCommaSeparated(() =>
            {
                var fieldName = ParseIdentifier();
                var fieldType = ParseDataType();
                return new StructField(fieldType, fieldName);
            });
        });
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
            if (ParseAnyOptionalTableConstraints(constraint => constraints.Add(constraint)))
            {
                // work has been done already
            }
            else if ((PeekToken() is Word) || (PeekToken() is SingleQuotedString))
            {
                columns.Add(ParseColumnDef());
            }
            else
            {
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

    public Sequence<UtilityOption> ParseUtilityOptions()
    {
        return ExpectParens(() =>
        {
            return ParseCommaSeparated(ParseUtilityOption);
        });
    }

    public Sequence<SequenceOptions> ParseCreateSequenceOptions()
    {
        var sequenceOptions = new Sequence<SequenceOptions>();

        //[ INCREMENT [ BY ] increment ]
        if (ParseKeyword(Keyword.INCREMENT))
        {
            var by = ParseKeyword(Keyword.BY);
            sequenceOptions.Add(new SequenceOptions.IncrementBy(ParseNumber(), by));
        }

        //[ MINVALUE minvalue | NO MINVALUE ]
        if (ParseKeyword(Keyword.MINVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MinValue(ParseNumber()));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MINVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MinValue(null));
        }

        //[ MAXVALUE maxvalue | NO MAXVALUE ]
        if (ParseKeywordSequence(Keyword.MAXVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MaxValue(ParseNumber()));
        }
        else if (ParseKeywordSequence(Keyword.NO, Keyword.MAXVALUE))
        {
            sequenceOptions.Add(new SequenceOptions.MaxValue(null));
        }

        //[ START [ WITH ] start ]
        if (ParseKeywordSequence(Keyword.START))
        {
            var with = ParseKeyword(Keyword.WITH);
            sequenceOptions.Add(new SequenceOptions.StartWith(ParseNumber(), with));
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
}