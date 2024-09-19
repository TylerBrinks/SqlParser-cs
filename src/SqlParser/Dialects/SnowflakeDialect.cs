using System.Text;
using SqlParser.Ast;
using SqlParser.Tokens;

namespace SqlParser.Dialects;

/// <summary>
/// Snowflake dialect
///
/// <see href="https://docs.snowflake.com/en/sql-reference/constructs"/>
/// </summary>
public class SnowflakeDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
        => character.IsLetter() || character is Symbols.Underscore;

    public override bool IsIdentifierPart(char character)
        => character.IsAlphaNumeric() || character is Symbols.Dollar or Symbols.Underscore;
    
    public override Statement? ParseStatement(Parser parser)
    {
        if (parser.ParseKeyword(Keyword.CREATE))
        {
            // possibly CREATE STAGE
            //[ OR  REPLACE ]
            var orReplace = parser.ParseKeywordSequence(Keyword.OR, Keyword.REPLACE);

            bool? global = parser.ParseOneOfKeywords(Keyword.OR, Keyword.REPLACE) switch
            {
                Keyword.LOCAL => false,
                Keyword.GLOBAL => true,
                _ => null
            };
            
            var temp = false;
            var @volatile = false;
            var transient = false;

            switch (parser.ParseOneOfKeywords(Keyword.TEMP, Keyword.TEMPORARY, Keyword.VOLATILE, Keyword.TRANSIENT))
            {
                case Keyword.TEMP or Keyword.TEMPORARY:
                    temp = true;
                    break;

                case Keyword.VOLATILE:
                    @volatile = true;
                    break;

                case Keyword.TRANSIENT:
                    transient = true;
                    break;
            }


            if (parser.ParseKeyword(Keyword.STAGE))
            {
                // CREATE STAGE statement
                return ParseCreateStage(orReplace, temp, parser);
            }

            if (parser.ParseKeyword(Keyword.TABLE))
            {
                return ParseCreateTable(orReplace, global, temp, @volatile, transient, parser);
            }

            // Rewind parser
            var back = 1;
            if (orReplace)
            {
                back += 2;
            }

            if (temp)
            {
                back += 1;
            }

            for (var i = 0; i < back; i++)
            {
                parser.PrevToken();
            }
        }

        if (parser.ParseKeywordSequence(Keyword.COPY, Keyword.INTO))
        {
            return ParseCopyInto(parser);
        }

        return null;
    }

    private static Statement.CreateTable ParseCreateTable(bool orReplace, bool? global, bool temp, bool @volatile, bool transient, Parser parser)
    {
        var ifNotExists = parser.ParseIfNotExists();
        var tableName = parser.ParseObjectName(false);

        Sequence<ColumnDef> columns = [];
        bool? copyGrants = null;
        CommentDef? comment = null;
        Query? query = null;
        ObjectName? clone = null;
        ObjectName? like = null;
        WrappedCollection<Ident>? clusterBy = null;
        bool? enableSchemaEvolution = null;
        bool? changeTracking = null;
        long? dataRetentionTimeInDays = null;
        long? maxDataExtensionTimeInDays = null;
        string? defaultDdlCollation = null;
        RowAccessPolicy? withRowAccessPolicy = null;
        ObjectName? aggregationPolicy = null;
        Sequence<Tag>? tags = null;
        Sequence<TableConstraint>? constraints = null;
        
        var loop = true;

        while (loop)
        {
            var next = parser.NextToken();

            switch (next)
            {
                case Word w:
                    {
                        switch (w.Keyword)
                        {
                            case Keyword.COPY:
                                parser.ExpectKeyword(Keyword.GRANTS);
                                copyGrants = true;
                                break;

                            case Keyword.COMMENT:
                                parser.ExpectToken<Equal>();
                                next = parser.NextToken();

                                if (next is SingleQuotedString s)
                                {
                                    comment = new CommentDef.WithEq(s.Value);
                                }
                                else
                                {
                                    throw Parser.Expected("comment", next);
                                }
                                break;

                            case Keyword.AS:
                                query = parser.ParseQuery();
                                loop = false;
                                break;

                            case Keyword.CLONE:
                                clone = parser.ParseObjectName();
                                loop = false;
                                break;

                            case Keyword.LIKE:
                                like = parser.ParseObjectName();
                                loop = false;

                                break;

                            case Keyword.CLUSTER:
                                parser.ExpectKeyword(Keyword.BY);
                                clusterBy = parser.ExpectParens(() =>
                                {
                                    var idents = parser.ParseCommaSeparated(parser.ParseIdentifier);
                                    return new WrappedCollection<Ident>.Parentheses(idents);
                                });

                                break;

                            case Keyword.ENABLE_SCHEMA_EVOLUTION:
                                parser.ExpectToken<Equal>();
                                var kwd = parser.ParseOneOfKeywords(Keyword.TRUE, Keyword.FALSE);
                                enableSchemaEvolution = kwd switch
                                {
                                    Keyword.TRUE => true,
                                    Keyword.FALSE => false,
                                    _ => throw Parser.Expected("TRUE or FALSE")
                                };
                                break;

                            case Keyword.CHANGE_TRACKING:
                                parser.ExpectToken<Equal>();
                                changeTracking = parser.ParseOneOfKeywords(Keyword.TRUE, Keyword.FALSE) switch
                                {
                                    Keyword.TRUE => true,
                                    Keyword.FALSE => false,
                                    _ => throw Parser.Expected("TRUE or FALSE")
                                };
                                break;

                            case Keyword.DATA_RETENTION_TIME_IN_DAYS:
                                parser.ExpectToken<Equal>();
                                dataRetentionTimeInDays = (long)parser.ParseLiteralUnit();
                                break;

                            case Keyword.MAX_DATA_EXTENSION_TIME_IN_DAYS:
                                parser.ExpectToken<Equal>();
                                maxDataExtensionTimeInDays = (long)parser.ParseLiteralUnit();
                                break;

                            case Keyword.DEFAULT_DDL_COLLATION:
                                parser.ExpectToken<Equal>();
                                defaultDdlCollation = parser.ParseLiteralString();
                                break;

                            // WITH is optional, we just verify that next token is one of the expected ones and
                            // fallback to the default match statement
                            case Keyword.WITH:
                                parser.ExpectOneOfKeywords(Keyword.AGGREGATION, Keyword.TAG, Keyword.ROW);
                                parser.PrevToken();
                                break;

                            case Keyword.AGGREGATION:
                                parser.ExpectKeywords(Keyword.POLICY);
                                aggregationPolicy = parser.ParseObjectName();
                                break;

                            case Keyword.ROW:
                                parser.ExpectKeywords(Keyword.ACCESS, Keyword.POLICY);
                                var policy = parser.ParseObjectName();
                                parser.ExpectKeywords(Keyword.ON);
                                var cols = parser.ExpectParens(() => parser.ParseCommaSeparated(parser.ParseIdentifier));
                                withRowAccessPolicy = new RowAccessPolicy(policy, cols);
                                break;

                            case Keyword.TAG:

                                tags = parser.ExpectParens(() => parser.ParseCommaSeparated(ParseTag));
                                break;

                                Tag ParseTag()
                                {
                                    var name = parser.ParseIdentifier();
                                    parser.ExpectToken<Equal>();
                                    var value = parser.ParseLiteralString();
                                    return new Tag(name, value);
                                }

                            default:
                                throw Parser.Expected("end of statement", next);
                        }

                        break;
                    }

                case LeftParen:
                    {
                        parser.PrevToken();
                        (columns, constraints) = parser.ParseColumns();

                        break;
                    }

                case EOF:
                {
                    if (!columns.SafeAny())
                    {
                        throw new ParserException("unexpected end of input");
                    }

                    loop = false;
                    break;
                }

                case SemiColon:
                    {
                        if (!columns.SafeAny())
                        {
                            throw new ParserException("unexpected end of input");
                        }

                        parser.PrevToken();
                        loop = false;
                        break;
                    }

                default:
                    throw Parser.Expected("end of statement", next);
            }
        }

        var create = new CreateTable(tableName, columns)
        {
            OrReplace = orReplace,
            Global = global,
            Temporary = temp,
            Volatile = @volatile,
            Transient = transient,
            IfNotExists = ifNotExists,
            Comment = comment,
            Query = query,
            ChangeTracking = changeTracking,
            CopyGrants = copyGrants,
            CloneClause = clone,
            Like = like,
            ClusterBy = clusterBy,
            EnableSchemaEvolution = enableSchemaEvolution,
            DataRetentionTimeInDays = dataRetentionTimeInDays,
            MaxDataExtensionTimeInDays = maxDataExtensionTimeInDays,
            DefaultDdlCollation = defaultDdlCollation,
            WithRowAccessPolicy = withRowAccessPolicy,
            WithAggregationPolicy = aggregationPolicy,
            WithTags = tags,
            Constraints = constraints,
        };

        return new Statement.CreateTable(create);
    }

    private static Statement ParseCreateStage(bool orReplace, bool temp, Parser parser)
    {
        //[ IF NOT EXISTS ]
        var ifNot = parser.ParseIfNotExists();
        var name = parser.ParseObjectName();
        Sequence<DataLoadingOption>? directoryTableParams = null;
        Sequence<DataLoadingOption>? fileFormat = null;
        Sequence<DataLoadingOption>? copyOptions = null;
        string? comment = null;

        // [ internalStageParams | externalStageParams ]
        var stageParams = ParseStageParams(parser);

        // [ directoryTableParams ]
        if (parser.ParseKeyword(Keyword.DIRECTORY))
        {
            parser.ExpectToken<Equal>();
            directoryTableParams = ParseParenOptions(parser);
        }

        // [ file_format]
        if (parser.ParseKeyword(Keyword.FILE_FORMAT))
        {
            parser.ExpectToken<Equal>();
            fileFormat = ParseParenOptions(parser);
        }

        // [ copy_options ]
        if (parser.ParseKeyword(Keyword.COPY_OPTIONS))
        {
            parser.ExpectToken<Equal>();
            copyOptions = ParseParenOptions(parser);
        }

        if (parser.ParseKeyword(Keyword.COMMENT))
        {
            parser.ExpectToken<Equal>();
            var token = parser.NextToken();

            comment = token switch
            {
                SingleQuotedString s => s.Value,
                _ => throw Parser.Expected("a comment statement", parser.PeekToken())
            };
        }

        return new Statement.CreateStage(name, stageParams)
        {
            OrReplace = orReplace,
            Temporary = temp,
            Comment = comment,
            CopyOptions = copyOptions,
            DirectoryTableParams = directoryTableParams,
            FileFormat = fileFormat,
            IfNotExists = ifNot
        };
    }

    private static StageParams ParseStageParams(Parser parser)
    {
        string? url = null;
        string? storageIntegration = null;
        string? endpoint = null;
        Sequence<DataLoadingOption>? credentials = null;
        Sequence<DataLoadingOption>? encryption = null;

        if (parser.ParseKeyword(Keyword.URL))
        {
            parser.ExpectToken<Equal>();
            var token = parser.NextToken();
            url = token switch
            {
                SingleQuotedString s => s.Value,
                _ => throw Parser.Expected("a URL statement", token)
            };
        }

        if (parser.ParseKeyword(Keyword.STORAGE_INTEGRATION))
        {
            parser.ExpectToken<Equal>();
            var token = parser.NextToken();
            storageIntegration = token.ToString();// <-- not sure
        }

        if (parser.ParseKeyword(Keyword.ENDPOINT))
        {
            parser.ExpectToken<Equal>();
            var token = parser.NextToken();
            endpoint = token switch
            {
                SingleQuotedString s => s.Value,
                _ => throw Parser.Expected("a endpoint statement", token)
            };
        }

        if (parser.ParseKeyword(Keyword.CREDENTIALS))
        {
            parser.ExpectToken<Equal>();
            credentials = ParseParenOptions(parser);
        }

        if (parser.ParseKeyword(Keyword.ENCRYPTION))
        {
            parser.ExpectToken<Equal>();
            encryption = ParseParenOptions(parser);
        }

        return new StageParams
        {
            Credentials = credentials,
            Encryption = encryption,
            Endpoint = endpoint,
            StorageIntegration = storageIntegration,
            Url = url
        };
    }

    private static Sequence<DataLoadingOption>? ParseParenOptions(Parser parser)
    {
        Sequence<DataLoadingOption>? options = null;

        parser.ExpectLeftParen();
        var loop = true;
        while (loop)
        {
            var token = parser.NextToken();

            switch (token)
            {
                case RightParen:
                    loop = false;
                    break;

                case Word w:
                    parser.ExpectToken<Equal>();
                    options ??= new Sequence<DataLoadingOption>();
                    if (parser.ParseKeyword(Keyword.TRUE))
                    {
                        options.Add(new DataLoadingOption(w.Value, DataLoadingOptionType.Boolean, "TRUE"));
                    }
                    else if (parser.ParseKeyword(Keyword.FALSE))
                    {
                        options.Add(new DataLoadingOption(w.Value, DataLoadingOptionType.Boolean, "FALSE"));
                    }
                    else
                    {
                        var next = parser.NextToken();
                        if (next is SingleQuotedString s)
                        {
                            options.Add(new DataLoadingOption(w.Value, DataLoadingOptionType.String, s.Value));
                        }
                        else if (next is Word nw)
                        {
                            options.Add(new DataLoadingOption(w.Value, DataLoadingOptionType.Enum, nw.Value));
                        }
                        else
                        {
                            Parser.ThrowExpected("option value", next);
                        }
                    }
                    break;

                default:
                    Parser.ThrowExpected("another option or ')'", token);
                    break;
            }
        }

        return options;
    }

    private static Statement ParseCopyInto(Parser parser)
    {
        var into = ParseStageName(parser);
        Sequence<string>? files = null;
        Sequence<StageLoadSelectItem>? fromTransformations = null;
        Ident? fromStageAlias = null;
        ObjectName? fromStage;
        StageParams? stageParams;

        parser.ExpectKeyword(Keyword.FROM);

        var next = parser.NextToken();
        // Check if data load transformations are present
        switch (next)
        {
            case LeftParen:
                // Data load with transformations
                parser.ExpectKeyword(Keyword.SELECT);
                fromTransformations = ParseSelectItemsForDataLoad(parser);

                parser.ExpectKeyword(Keyword.FROM);

                fromStage = ParseSnowflakeStageName(parser);
                stageParams = ParseStageParams(parser);

                // As
                if (parser.ParseKeyword(Keyword.AS))
                {
                    var asNext = parser.NextToken();
                    if (asNext is Word word)
                    {
                        fromStageAlias = new Ident(word.Value);
                    }
                    else
                    {
                        throw Parser.Expected("Stage alias", parser.PeekToken());
                    }
                }
                parser.ExpectRightParen();
                break;

            default:
                parser.PrevToken();
                //fromStage = parser.ParseObjectName();
                fromStage = ParseSnowflakeStageName(parser);
                stageParams = ParseStageParams(parser);

                // As
                if (parser.ParseKeyword(Keyword.AS))
                {
                    var asNext = parser.NextToken();
                    if (asNext is Word word)
                    {
                        fromStageAlias = new Ident(word.Value);
                    }
                    else
                    {
                        throw Parser.Expected("Stage alias", parser.PeekToken());
                    }
                }

                break;
        }

        // Files
        if (parser.ParseKeyword(Keyword.FILES))
        {
            parser.ExpectToken<Equal>();
            parser.ExpectLeftParen();
            var loop = true;

            while (loop)
            {
                loop = false;
                if (parser.NextToken() is SingleQuotedString file)
                {
                    files ??= [];
                    files.Add(file.Value);
                }
                else
                {
                    throw Parser.Expected("File token", parser.PeekToken());
                }

                if (parser.NextToken() is Comma)
                {
                    loop = true;
                }
                else
                {
                    parser.PrevToken();
                }
            }
            parser.ExpectRightParen();
        }

        SingleQuotedString? pattern = null;

        // Pattern
        if (parser.ParseKeyword(Keyword.PATTERN))
        {
            parser.ExpectToken<Equal>();

            if (parser.NextToken() is SingleQuotedString p)
            {
                pattern = p;
            }
            else
            {
                throw Parser.Expected("Pattern", parser.PeekToken());
            }
        }

        // File format
        Sequence<DataLoadingOption>? fileFormat = null;
        if (parser.ParseKeyword(Keyword.FILE_FORMAT))
        {
            parser.ExpectToken<Equal>();
            fileFormat = ParseParenthesesOptions(parser);
        }

        // Copy options
        Sequence<DataLoadingOption>? copyOptions = null;
        if (parser.ParseKeyword(Keyword.COPY_OPTIONS))
        {
            parser.ExpectToken<Equal>();
            copyOptions = ParseParenthesesOptions(parser);
        }

        // Validation mode
        string? validationMode = null;
        if (parser.ParseKeyword(Keyword.VALIDATION_MODE))
        {
            parser.ExpectToken<Equal>();
            validationMode = parser.NextToken().ToString();
        }

        return new Statement.CopyIntoSnowflake(into, fromStage, fromStageAlias, stageParams,
            fromTransformations, files, pattern?.Value, fileFormat, copyOptions, validationMode);
    }

    private static ObjectName ParseSnowflakeStageName(Parser parser)
    {
        if (parser.NextToken() is AtSign)
        {
            parser.PrevToken();
            var idents = new List<Ident>();

            while (true)
            {
                idents.Add(ParseStageNameIdentifier(parser));

                if (!parser.ConsumeToken<Period>())
                {
                    break;
                }
            }

            return new ObjectName(idents);
        }

        parser.PrevToken();
        return parser.ParseObjectName();
    }

    private static Sequence<StageLoadSelectItem> ParseSelectItemsForDataLoad(Parser parser)
    {
        var selectItems = new Sequence<StageLoadSelectItem>();

        while (true)
        {
            Ident? alias = null;
            var fileColumnNumber = 0;
            Ident? element = null;
            Ident? itemAs = null;

            var next = parser.NextToken();

            switch (next)
            {
                case Placeholder p:
                    var rightHalf = p.Value[1..];
                    var parsed = int.TryParse(rightHalf, out fileColumnNumber);

                    if (!parsed)
                    {
                        throw new ParserException($"Could not parse '{p}'", p.Location);
                    }

                    break;

                case Word w:
                    alias = new Ident(w.Value);
                    break;

                default:
                    throw Parser.Expected("Alias for file_col_num", next);
            }

            if (alias != null)
            {
                parser.ExpectToken<Period>();

                if (parser.NextToken() is Placeholder p)
                {
                    var rightHalf = p.Value[1..];
                    var parsed = int.TryParse(rightHalf, out fileColumnNumber);

                    if (!parsed)
                    {
                        throw new ParserException($"Could not parse '{p}'", p.Location);
                    }
                }
                else
                {
                    throw Parser.Expected("file_col_num", parser.PeekToken());
                }
            }

            // Try extracting optional element
            next = parser.NextToken();
            if (next is Colon)
            {
                if (parser.NextToken() is Word w)
                {
                    element = new Ident(w.Value);
                }
                else
                {
                    throw Parser.Expected("file_col_num", parser.PeekToken());
                }
            }
            else
            {
                parser.PrevToken();
            }

            // as
            if (parser.ParseKeyword(Keyword.AS))
            {
                if (parser.NextToken() is Word w)
                {
                    itemAs = new Ident(w.Value);
                }
                else
                {
                    throw Parser.Expected("Column item alias", parser.PeekToken());
                }
            }

            selectItems.Add(new StageLoadSelectItem
            {
                Alias = alias,
                FileColumnNumber = fileColumnNumber,
                Element = element,
                ItemAs = itemAs
            });

            if (parser.NextToken() is Comma)
            {
                continue;
            }

            parser.PrevToken();
            break;
        }

        return selectItems;
    }

    private static Sequence<DataLoadingOption> ParseParenthesesOptions(Parser parser)
    {
        var options = new Sequence<DataLoadingOption>();

        parser.ExpectToken<LeftParen>();
        var loop = true;

        while (loop)
        {
            var next = parser.NextToken();

            switch (next)
            {
                case RightParen:
                    loop = false;
                    break;

                case Word key:
                    parser.ExpectToken<Equal>();
                    if (parser.ParseKeyword(Keyword.TRUE))
                    {
                        options.Add(new DataLoadingOption(key.Value, DataLoadingOptionType.Boolean, "TRUE"));
                    }
                    else if (parser.ParseKeyword(Keyword.FALSE))
                    {
                        options.Add(new DataLoadingOption(key.Value, DataLoadingOptionType.Boolean, "FALSE"));
                    }
                    else
                    {
                        var wordNext = parser.NextToken();
                        switch (wordNext)
                        {
                            case SingleQuotedString s:
                                options.Add(new DataLoadingOption(key.Value, DataLoadingOptionType.String, s.Value));
                                break;
                            case Word w:
                                options.Add(new DataLoadingOption(key.Value, DataLoadingOptionType.Enum, w.Value));
                                break;
                            default:
                                throw Parser.Expected("expected option value", parser.PeekToken());
                        }
                    }
                    break;

                default:
                    throw Parser.Expected("another option or ')'", parser.PeekToken());
            }
        }

        return options;
    }
    /// <summary>
    /// Parses options provided within parenthesis
    ///  (
    ///     ENABLE = { TRUE | FALSE }
    ///     REFRESH_ON_CREATE =  { TRUE | FALSE }
    ///  )
    /// </summary>
    /// <param name="parser"></param>
    /// <returns></returns>
    private static ObjectName ParseStageName(Parser parser)
    {
        var next = parser.NextToken();

        if (next is AtSign)
        {
            parser.PrevToken();
            var idents = new List<Ident>();
            while (true)
            {
                idents.Add(ParseStageNameIdentifier(parser));
                if (!parser.ConsumeToken<Period>())
                {
                    break;
                }
            }
            return new ObjectName(idents);
        }

        parser.PrevToken();
        return parser.ParseObjectName();
    }

    private static Ident ParseStageNameIdentifier(Parser parser)
    {
        var ident = new StringBuilder();
        var loop = true;

        while (loop && parser.NextTokenNoSkip() is { } next)
        {
            switch (next)
            {
                case Whitespace:
                    loop = false;
                    break;

                case Period:
                case RightParen:
                    parser.PrevToken();
                    loop = false;
                    break;

                case AtSign:
                    ident.Append(Symbols.At);
                    break;

                case Tilde:
                    ident.Append(Symbols.Tilde);
                    break;

                case Modulo:
                    ident.Append(Symbols.Percent);
                    break;

                case Divide:
                    ident.Append(Symbols.Divide);
                    break;

                case Word w:
                    foreach (var c in w.Value)
                    {
                        ident.Append(c);
                    }

                    break;

                default:
                    throw Parser.Expected("Stage name identifier", parser.PeekToken());
            }
        }

        return new Ident(ident.ToString());
    }

    public override short? GetNextPrecedence(Parser parser)
    {
        var token = parser.PeekToken();

        if (token is Colon)
        {
            return GetPrecedence(Precedence.DoubleColon);
        }

        return null;
    }

    public override bool SupportsFilterDuringAggregation => true;
    public override bool SupportsStringLiteralBackslashEscape => true;
    public override bool SupportsParenthesizedSetVariables => true;
    public override bool SupportsProjectionTrailingCommas => true;
    public override bool SupportsMatchRecognize => true;
    public override bool SupportsDictionarySyntax => true;
    public override bool SupportsConnectBy => true;
    public override bool SupportsWindowFunctionNullTreatmentArg => true;
    public override bool DescribeRequiresTableKeyword => true;
    public override bool AllowExtractCustom => true;
    public override bool AllowExtractSingleQuotes => true;
}