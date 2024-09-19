using SqlParser.Ast;
using SqlParser.Dialects;
using SqlParser.Tokens;

namespace SqlParser.Tests.Dialects;

public class BogusCounterDialect : GenericDialect
    {
        public override Statement ParseStatement(Parser parser)
        {
            var token = parser.NextToken();

            if (token is Word {Value: "custom" } word)
            {
                var value = word.Value;

                while ((token = parser.NextToken()) is not EOF)
                {
                    value += token switch
                    {
                        Word t => $" {t.Value}",
                        _ => token.ToString()
                    };
                }

                return new SchemaName.Simple(value);
            }
            else
            {
                while ((parser.NextToken()) is not EOF)
                {
                    // Advance to the end of the stream
                }

                return new SchemaName.Simple("Totally Custom SQL");
            }
        }
    }