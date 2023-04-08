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
    {
        return character.IsLetter() || character is Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return character.IsAlphaNumeric() || character is Symbols.Dollar or Symbols.Underscore;
    }

    public override bool SupportsFilterDuringAggregation()
    {
        return true;
    }

    public override bool SupportsWithinAfterArrayAggregation()
    {
        return true;
    }

    public override Statement? ParseStatement(Parser parser)
    {
        if (parser.ParseKeyword(Keyword.CREATE))
        {
            // possibly CREATE STAGE
            //[ OR  REPLACE ]
            var orReplace = parser.ParseKeywordSequence(Keyword.OR, Keyword.REPLACE);
            var temp = parser.ParseKeyword(Keyword.TEMPORARY);

            if (parser.ParseKeyword(Keyword.STAGE))
            {
                // CREATE STAGE statement
                return ParseCreateStage(orReplace, temp, parser);
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

        return null;
    }

    private Statement ParseCreateStage(bool orReplace, bool temp, Parser parser)
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

    private StageParams ParseStageParams(Parser parser)
    {
        string? url=null;
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

    private Sequence<DataLoadingOption>? ParseParenOptions(Parser parser)
    {
        Sequence<DataLoadingOption>? options = null;

        parser.ExpectLeftParen();
        var loop = true;
        while (loop)
        {
            var token = parser.NextToken();

            switch(token)
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
}