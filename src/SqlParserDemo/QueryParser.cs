using Newtonsoft.Json;
using Spectre.Console;
using Spectre.Console.Json;
using SqlParser;
using SqlParser.Dialects;

namespace SqlParserDemo;

public class QueryParser
{
    private bool _prompt = true;
    private bool _repeat;
    private string? _sql;

    public void Run()
    {
        while (_prompt)
        {
            if (_repeat && _sql != null)
            {
                AnsiConsole.Markup("[green]Repeating query[/]");
                AnsiConsole.WriteLine("");
                AnsiConsole.Markup($"[blue]{_sql}[/]");
                AnsiConsole.WriteLine("");
                AnsiConsole.WriteLine("");
                _repeat = false;
            }
            else
            {
                _sql = AnsiConsole.Prompt(new TextPrompt<string>("Enter a SQL Query:").PromptStyle("lime"));
            }

            try
            {
                var statements = new Parser().ParseSql(_sql, new SQLiteDialect());
                var choices = new List<string> { "Default (AST)", "SQL", "JSON" };
                var format = AnsiConsole.Prompt(new SelectionPrompt<string>()
                    .Title("[green]Format the output as[/]?")
                    .AddChoices(choices));

                var formatted = format switch
                {
                    { } when format == choices[0] => statements.First().ToString(),
                    { } when format == choices[1] => statements.First().ToSql(),
                    { } when format == choices[2] => JsonConvert.SerializeObject(statements.First(), Formatting.Indented,
                        new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Ignore
                        }),
                    _ => ""
                };

                if (format == choices[2])
                {
                    AnsiConsole.Write(new JsonText(formatted));
                }
                else
                {
                    AnsiConsole.Markup(formatted.EscapeMarkup());
                }
            }
            catch (ParserException ex)
            {
                AnsiConsole.Markup("[red]Invalid SQL.[/]");
                AnsiConsole.WriteLine("");
                AnsiConsole.Write(ex.Message);
            }
            catch (TokenizeException ex)
            {
                AnsiConsole.MarkupLine("[red]Invalid Query Format.[/]");
                AnsiConsole.WriteLine("");
                AnsiConsole.MarkupLine(ex.Message);
            }

            AnsiConsole.WriteLine("");
            AnsiConsole.WriteLine("");

            var continueOptions = new List<string> {"Enter another query", "Repeat the last query", "Back to demo options"};
            var option = AnsiConsole.Prompt(new SelectionPrompt<string>()
                .Title("[yellow]Continue[/]?")
                .AddChoices(continueOptions));

            if (option == continueOptions[0])
            {
                _sql = null;
            }
            else if (option == continueOptions[1])
            {
                _repeat = true;
            }

            if (option == continueOptions[2])
            {
                AnsiConsole.Clear();
                _prompt = false;
            }

            AnsiConsole.Clear();
        }
    }
}