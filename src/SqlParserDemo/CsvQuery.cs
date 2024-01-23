using Spectre.Console;
using SqlParser;
using SqlParser.Ast;
using Action = System.Action;
using Table = Spectre.Console.Table;

namespace SqlParserDemo;

public class CsvQuery
{

    public void Run()
    {
        var loop = true;

        while (loop)
        {
            var choices = new List<string> {"SELECT * FROM demo_file1.csv", "SELECT TOP 5 b, d from  demo_file2.csv", "Back to demo options" };

            var op = AnsiConsole.Prompt(new SelectionPrompt<string>()
                .Title("[green]Choose a query to run against a CSV file[/]?")
                .AddChoices(choices));

            Action operation = op switch
            {
                {} when op == choices[0] => () => Select(op),
                {} when op == choices[1] => () => Select(op),
                _ => () => { AnsiConsole.Clear(); loop = false; }
            };

            operation();
        }
    }

    public void Select(string query)
    {
        var ast = new Parser().ParseSql(query);

        var plan = BuildLogicalPlan(ast);

        plan.Render(ast.ToSql());
    }

    private CsvQueryPlan BuildLogicalPlan(Sequence<Statement> ast)
    {
        // There will only be 1 statement even though the parser
        // supports multiple statements in a single query.
        var select = ast.First().AsQuery()!.Body.AsSelect();

        // The select clause has the name of the CSV file as the table name.
        // Extract the name from the query
        var csvFile = select.From!.First().Relation!.AsTable().Name.ToString();
        // Find the CSV file in the build output directory
        var path = Path.IsPathRooted(csvFile) ? csvFile : Path.GetRelativePath(Directory.GetCurrentDirectory(), csvFile);
        
        var text = File.ReadAllText(path);
        // Primitive CSV parsing; demo purposes only
        var rows = text.Split("\r\n").Where(r => !string.IsNullOrEmpty(r)).ToList();
        var header = rows[0].Split(",").ToList();

        // Building a pseudo query plan; simplistic, demo purposes only.
        // Inform the plan about the headers (row 0) and the raw row values (rows 1+)
        var plan = new CsvQueryPlan(rows[0].Split(",").ToList(), rows.Skip(1).ToList());
        
        // Check what kind of query is executing.  Wildcard will select all
        // otherwise specific columns are being selected.
        if (select.Projection is [SelectItem.Wildcard])
        {
            // Select *, so use all columns
            var indices = header.Select((_, index) => index).ToList();
            plan.ColumnIndices.AddRange(indices);
        }
        else
        {
            // Unnamed columns, so select specific columns by their index positions
            var columnNames = select.Projection.Select(col => col.AsUnnamed().Expression.AsIdentifier().Ident.Value).ToList();
            var indices = columnNames.Select(c => header.IndexOf(c));
            plan.ColumnIndices.AddRange(indices);
        }

        // Filter the Top N records if 'TOP N' is in the query
        if (select.Top != null)
        {
            var top = (TopQuantity.Constant)select.Top.Quantity!;
            plan.Top = (int)top.Quantity;
        }
        
        return plan;
    }

    public class CsvQueryPlan
    {
        public CsvQueryPlan(List<string> headers, List<string> rows)
        {
            Headers = headers;
            Rows = rows;
        }

        public List<int> ColumnIndices = new();
        public List<string> Headers { get; init; }
        public List<string> Rows { get; init; }
        public int? Top { get; set; }

        public void Render(string sql)
        {
            var table = new Table();
            // Add the columns to the console output
            foreach (var index in ColumnIndices)
            {
                table.AddColumn(Headers[index]);
            }

            // Find the number of records to query
            var count = Math.Min(Top ?? int.MaxValue, Rows.Count);

            // Add each row in the query taking only the columns
            // relevant to the output
            foreach (var row in Rows.Take(count))
            {
                var cols = row.Split(",").ToList();

                table.AddRow(ColumnIndices.Select(index => cols[index]).ToArray());
            }

            AnsiConsole.MarkupLine("[yellow]The table below rendered by parsing the AST for this query:[/]");
            AnsiConsole.MarkupLine($"[green]\"{sql}\"[/]");
            AnsiConsole.Write(table);
            AnsiConsole.WriteLine("");
        }
    }
}