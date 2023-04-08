using Spectre.Console;
using SqlParserDemo;

AnsiConsole.Write(new Rule("SQL Parser") { Justification = Justify.Left });
AnsiConsole.WriteLine("");

var loop = true;

while (loop)
{
    var demoChoices = new List<string> {"Parse & format a SQL query you type in", "Use SQL to query sample CSV files (using pre-build queries)", "Exit"};
    var demo = AnsiConsole.Prompt(new SelectionPrompt<string>()
        .Title("[yellow]Select a feature to demo[/]")
        .AddChoices(demoChoices));

    Action demoChoice = demo switch
    {
        { } when demo == demoChoices[0] => () => new QueryParser().Run(),
        { } when demo == demoChoices[1] => () => new CsvQuery().Run(),
        _ => () => { loop = false; Console.Clear(); }
    };
    
    demoChoice();
}