using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;
using static SqlParser.Ast.Expression;
using DataType = SqlParser.Ast.DataType;

namespace SqlParser;

public partial class Parser
{
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

                ExtractSyntax syntax;
                if (ParseKeyword(Keyword.FROM))
                {
                    syntax = ExtractSyntax.From;
                }
                else if (_dialect is SnowflakeDialect or GenericDialect && ConsumeToken<Comma>())
                {
                    syntax = ExtractSyntax.Comma;
                }
                else
                {
                    throw Expected("'FROM' or ','");
                }

                var expr = ParseExpr();
                return new Extract(expr, field, syntax);
            });
        }

        Expression ParseCeilFloorExpr(bool isCeiling)
        {
            return ExpectParens<Expression>(() =>
            {
                var expr = ParseExpr();
                // Parse `CEIL/FLOOR(expr)`
                CeilFloorKind field = null!;
                var keywordTo = ParseKeyword(Keyword.TO);

                if (keywordTo)
                {
                    field = new CeilFloorKind.DateTimeFieldKind(ParseDateTimeField());
                }
                else if (ConsumeToken<Comma>())
                {
                    var parsedValue = ParseValue();
                    if (parsedValue is Value.Number n)
                    {
                        field = new CeilFloorKind.Scale(new Value.Number(n.Value, n.Long));
                    }
                    else
                    {
                        ThrowExpected("Scale field can only be of number type", PeekToken());
                    }
                }
                else
                {
                    field = new CeilFloorKind.DateTimeFieldKind(new DateTimeField.NoDateTime());
                }

                return isCeiling
                    ? new Ceil(expr, field)
                    : new Floor(expr, field);
            });
        }

        Expression ParsePositionExpr(Ident ident)
        {
            var betweenPrec = _dialect.GetPrecedence(Precedence.Between);
            var positionExpression = MaybeParse(() =>
            {
                ExpectLeftParen();

                var expr = ParseSubExpression(betweenPrec);

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

            return new UnaryOp(ParseSubExpression(_dialect.GetPrecedence(Precedence.UnaryNot)), UnaryOperator.Not);
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
            //return new Prior(ParseSubExpression(PlusMinusPrecedence));
            return new Prior(ParseSubExpression(_dialect.GetPrecedence(Precedence.PlusMinus)));
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
            var expressions = ParseCommaSeparated0(ParseExpr, typeof(RightBracket));
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

            return new UnaryOp(ParseSubExpression(_dialect.GetPrecedence(Precedence.PlusMinus)), op);
        }

        UnaryOp ParseUnary()
        {
            try
            {
                var op = token is Plus ? UnaryOperator.Plus : UnaryOperator.Minus;
                return new UnaryOp(ParseSubExpression(_dialect.GetPrecedence(Precedence.MulDivModOp)), op);
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
   
    public Expression ParseInterval()
    {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // to match the different flavours of INTERVAL syntax, we only allow expressions
        // if the dialect requires an interval qualifier,
        var value = _dialect.RequireIntervalQualifier ? ParseExpr() : ParsePrefix();

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.

        var token = PeekToken();

        DateTimeField? leadingField = null;

        if (NextTokenIsTemporalUnit())
        {
            leadingField = ParseDateTimeField();
        }
        else if (_dialect.RequireIntervalQualifier)
        {
            throw new ParserException("INTERVAL requires a unit after the literal value");
        }

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

    public Expression ParseSubscript(Expression expression)
    {
        var subscript = ParseSubscriptInner();
        return new Expression.Subscript(expression, subscript);
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
        var precedence = _dialect.GetPrecedence(Precedence.Between);
        var low = ParseSubExpression(precedence);
        ExpectKeyword(Keyword.AND);
        var high = ParseSubExpression(precedence);

        return new Between(expr, negated, low, high);
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

    public Expression ParseNumber()
    {
        var next = NextToken();
        return next switch
        {
            Plus => new UnaryOp(new LiteralValue(ParseNumberValue()), UnaryOperator.Plus),
            Minus => new UnaryOp(new LiteralValue(ParseNumberValue()), UnaryOperator.Minus),
            _ => ParseNumberVal()
        };

        Expression ParseNumberVal()
        {
            PrevToken();
            return new LiteralValue(ParseNumberValue());
        }
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

    public Expression? ParseLimit()
    {
        return ParseKeyword(Keyword.ALL) ? null : ParseExpr();
    }
}