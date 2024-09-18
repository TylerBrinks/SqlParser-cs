using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

namespace SqlParser;

public partial class Parser
{
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

    /// <summary>
    /// Get the precedence of the next token
    /// </summary>
    /// <returns>Precedence value</returns>
    public short GetNextPrecedence()
    {
        return _dialect.GetNextPrecedenceDefault(this);
    }

    /// <summary>
    /// Gets the next token in the queue without advancing the current
    /// location.  If an overrun exists, an EOF token is returned.
    /// </summary>
    /// <returns>Returns the next token</returns>
    public Token PeekToken()
    {
        return PeekNthToken(0);
    }

    public Token PeekTokenNoSkip()
    {
        return PeekNthTokenNoSkip(0);
    }

    /// <summary>
    /// Gets a token at the Nth position from the current parser location.
    /// If an overrun exists, an EOF token is returned.
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

    public Token PeekNthTokenNoSkip(int nth)
    {
        if (_index + nth >= _tokens.Count)
        {
            return new EOF();
        }

        return _tokens[_index + nth];
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

    public IList<Token> PeekTokens(int count)
    {
        return PeekTokensWithLocation(count).ToList();
    }

    public IEnumerable<Token> PeekTokensWithLocation(int count)
    {
        var index = _index;
        var iteration = 0;

        while (true)
        {
            if (index >= _tokens.Count)
            {
                yield return new EOF();
                break;
            }

            var token = _tokens[index];
            index++;

            if (token is Whitespace)
            {
                continue;
            }

            yield return token;

            iteration++;
            if (iteration == count)
            {
                break;
            }
        }
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

    // ReSharper disable once GrammarMistakeInComment
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

    public Sequence<T> ParseKeywordSeparated<T>(Keyword keyword, Func<T> action)
    {
        Sequence<T> values = [];

        while (true)
        {
            var value = action();
            values.Add(value);
            if (!ParseKeyword(keyword))
            {
                break;
            }
        }

        return values;
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

    public bool ConsumeToken(Type type)
    {
        if (PeekToken().GetType() == type)
        {
            NextToken();
            return true;
        }

        return false;
    }

    public bool ConsumeTokens(params Type[] tokens)
    {
        var index = _index;
        foreach (var token in tokens)
        {

            if (!ConsumeToken(token))
            {
                _index = index;
                return false;
            }
        }

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

    public T? MaybeParseNullable<T>(Func<T> action) where T : struct
    {
        var index = _index;

        try
        {
            return action();
        }
        catch (ParserException)
        {
            _index = index;
            return null;
        }
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
    /// particular, Rust can break out of a nested context (loop, match, etc.) to a
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
            ShiftLeft when _dialect is PostgreSqlDialect or DuckDbDialect or GenericDialect => BinaryOperator
                .PGBitwiseShiftLeft,
            ShiftRight when _dialect is PostgreSqlDialect or DuckDbDialect or GenericDialect => BinaryOperator
                .PGBitwiseShiftRight,
            Hash when _dialect is PostgreSqlDialect => BinaryOperator.PGBitwiseXor,
            Overlap when _dialect is PostgreSqlDialect or DuckDbDialect => BinaryOperator.PGOverlap,
            CaretAt when _dialect is PostgreSqlDialect or GenericDialect => BinaryOperator.PGStartsWith,
            Tilde => BinaryOperator.PGRegexMatch,
            TildeAsterisk => BinaryOperator.PGRegexIMatch,
            ExclamationMarkTilde => BinaryOperator.PGRegexNotMatch,
            ExclamationMarkTildeAsterisk => BinaryOperator.PGRegexNotIMatch,
            DoubleTilde => BinaryOperator.PGLikeMatch,
            DoubleTildeAsterisk => BinaryOperator.PGILikeMatch,
            ExclamationMarkDoubleTilde => BinaryOperator.PGNotLikeMatch,
            ExclamationMarkDoubleTildeAsterisk => BinaryOperator.PGNotILikeMatch,

            Arrow => BinaryOperator.Arrow,
            LongArrow => BinaryOperator.LongArrow,
            HashArrow => BinaryOperator.HashArrow,
            HashLongArrow => BinaryOperator.HashLongArrow,
            AtArrow => BinaryOperator.AtArrow,
            ArrowAt => BinaryOperator.ArrowAt,
            HashMinus => BinaryOperator.HashMinus,
            AtQuestion => BinaryOperator.AtQuestion,
            AtAt => BinaryOperator.AtAt,
            Question => BinaryOperator.Question,
            QuestionAnd => BinaryOperator.QuestionAnd,
            QuestionPipe => BinaryOperator.QuestionPipe,
            CustomBinaryOperator => BinaryOperator.Custom,

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
                    throw Expected(
                        $"one of [=, >, <, =>, =<, !=] as comparison operator, found: {regularBinaryOperator}");
                }

                return keyword switch
                {
                    Keyword.ALL => new AllOp(expr, regularBinaryOperator, right),
                    Keyword.ANY => new AnyOp(expr, regularBinaryOperator, right),
                    _ => right
                };
            }

            var binaryOperator = new BinaryOp(expr, regularBinaryOperator, ParseSubExpression(precedence))
            {
                PgOptions = pgOptions
            };

            if (regularBinaryOperator is BinaryOperator.Custom)
            {
                binaryOperator.SetCustomOperator(((CustomBinaryOperator)token).Value);
            }

            return binaryOperator;
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
                        ExpectKeywords(Keyword.TIME, Keyword.ZONE);
                        return new AtTimeZone(expr, ParseSubExpression(precedence));
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
                            return new RLike(
                                negated, 
                                expr, 
                                ParseSubExpression(_dialect.GetPrecedence(Precedence.Like)), 
                                regexp);
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
                            return new Like(expr, negated, 
                                ParseSubExpression(_dialect.GetPrecedence(Precedence.Like)), 
                                ParseEscapeChar());
                        }

                        if (ParseKeyword(Keyword.ILIKE))
                        {
                            return new ILike(
                                expr, 
                                negated, 
                                ParseSubExpression(_dialect.GetPrecedence(Precedence.Like)), 
                                ParseEscapeChar());
                        }

                        if (ParseKeywordSequence(Keyword.SIMILAR, Keyword.TO))
                        {
                            return new SimilarTo(
                                expr, 
                                negated, 
                                ParseSubExpression(_dialect.GetPrecedence(Precedence.Like)),
                                ParseEscapeChar());
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
            return new Cast(expr, ParseDataType(), CastKind.DoubleColon);
        }

        if (token is ExclamationMark)
        {
            return new UnaryOp(expr, UnaryOperator.PGPostfixFactorial);
        }

        if (token is LeftBracket)
        {
            switch (_dialect)
            {
                case PostgreSqlDialect or DuckDbDialect or GenericDialect:
                    return ParseSubscript(expr);

                case SnowflakeDialect:
                    PrevToken();
                    return ParseJsonAccess(expr);

                default:
                    return ParseMapAccess(expr);
            }
        }

        if (_dialect is SnowflakeDialect or GenericDialect && token is Colon)
        {
            PrevToken();
            return ParseJsonAccess(expr);
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
            UnicodeStringLiteral s => s.Value,
            _ => throw Expected("literal string", token)
        };
    }

    public DataType ParseDataType()
    {
        var (dataType, trailingBracket) = ParseDataTypeHelper();

        if (trailingBracket)
        {
            throw new ParserException($"Unmatched > after parsing data type {dataType}", PeekToken().Location);
        }

        return dataType;
    }

    public ObjectName ParseObjectName()
    {
        return ParseObjectNameWithClause(false);
    }
    /// <summary>
    ///  Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ParserException"></exception>
    public Ident ParseIdentifier()
    {
        return ParseIdentifierWithClause(false);
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
    /// Parse a comma-separated list of 0+ items accepted by `F`
    /// `end_token` - expected end token for the closure (e.g. [Token::RParen], [Token::RBrace] ...)
    /// </summary>
    public Sequence<T> ParseCommaSeparated0<T>(Func<T> action, Type endTokenType)
    {
        var next = PeekToken();
        if (next.GetType() == endTokenType)
        {
            return [];
        }

        if (_options.TrailingCommas && (next.GetType() == endTokenType || next.GetType() == typeof(Comma)))
        {
            _ = ConsumeToken<Comma>();
            return [];
        }

        return ParseCommaSeparated(action);
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
            if (IsParseCommaSeparatedEnd())
            {
                break;
            }
        }

        return values;
    }
    /// <summary>
    /// Parse the comma of a comma-separated syntax element.
    /// Returns true if there is a next element
    /// </summary>
    /// <returns></returns>
    public bool IsParseCommaSeparatedEnd()
    {
        if (!ConsumeToken<Comma>())
        {
            return true;
        }

        if (_options.TrailingCommas)
        {
            var token = PeekToken();
            switch (token)
            {
                case Word w when Keywords.ReservedForColumnAlias.Contains(w.Keyword):
                case RightParen 
                    or SemiColon 
                    or EOF 
                    or RightBracket 
                    or RightBrace:
                    return true;
            }
        }

        return false;
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
        Fetch? fetch = null;
        Sequence<LockClause>? locks = null;
        Sequence<Expression>? limitBy = null;
        ForClause? forClause = null;
        FormatClause? formatClause = null;

        if (!ParseKeyword(Keyword.INSERT))
        {
            var body = ParseQueryBody(0); //TODO prec_unknown

            var orderBy = ParseOptionalOrderBy();

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

            var settings = ParseSettings();
           
            if (ParseKeyword(Keyword.FETCH))
            {
                fetch = ParseFetch();
            }

            while (ParseKeyword(Keyword.FOR))
            {
                var parsedForClause = ParseForClause();
                if (parsedForClause != null)
                {
                    forClause = parsedForClause;
                    break;
                }

                locks ??= new Sequence<LockClause>();
                locks.Add(ParseLock());
            }

            if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.FORMAT))
            {
                if (ParseKeyword(Keyword.NULL))
                {
                    formatClause = new FormatClause.Null();
                }
                else
                {
                    formatClause = new FormatClause.Identifier(ParseIdentifier());
                }
            }

            return new Statement.Select(new Query(body)
            {
                With = with,
                OrderBy = orderBy,
                Limit = limit,
                Offset = offset,
                Fetch = fetch,
                Locks = locks,
                LimitBy = limitBy,
                ForClause = forClause,
                Settings = settings,
                FormatClause = formatClause
            });
        }

        var insert = ParseInsert();

        return new Statement.Select(new Query(new SetExpression.Insert(insert))
        {
            With = with
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

            var lockFn = ExpectOneOfKeywords(Keyword.UPDATE, Keyword.SHARE);

            lockType = lockFn switch
            {
                Keyword.UPDATE => LockType.Update,
                Keyword.SHARE => LockType.Share,
                _ => lockType
            };

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

        ForClause? ParseForClause()
        {
            if (ParseKeyword(Keyword.XML))
            {
                return ParseForXml();
            }

            if (ParseKeyword(Keyword.JSON))
            {
                return ParseForJson();
            }

            if (ParseKeyword(Keyword.BROWSE))
            {
                return new ForClause.Browse();
            }

            return null;
        }
    }

    private Sequence<Setting>? ParseSettings()
    {
        Sequence<Setting>? settings = null;

        if (_dialect is ClickHouseDialect or GenericDialect && ParseKeyword(Keyword.SETTINGS))
        {
            settings = ParseCommaSeparated(() =>
            {
                var key = ParseIdentifier();
                ExpectToken<Equal>();
                var value = ParseValue();

                return new Setting(key, value);
            });
        }

        return settings;
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
}