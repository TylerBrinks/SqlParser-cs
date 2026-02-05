using System.Reflection;
using Newtonsoft.Json;
using SqlParser.Dialects;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace SqlParser.Tests;

public class ParserRegressionTests(ITestOutputHelper output) : ParserTestBase
{
    [Theory]
    [SqlFile("1.sql")]
    [SqlFile("2.sql")]
    [SqlFile("3.sql")]
    [SqlFile("4.sql")]
    [SqlFile("5.sql")]
    [SqlFile("6.sql")]
    [SqlFile("7.sql")]
    [SqlFile("8.sql")]
    [SqlFile("9.sql")]
    [SqlFile("10.sql")]
    [SqlFile("11.sql")]
    [SqlFile("12.sql")]
    [SqlFile("13.sql")]
    [SqlFile("14.sql")]
    [SqlFile("15.sql")]
    [SqlFile("16.sql")]
    [SqlFile("17.sql")]
    [SqlFile("18.sql")]
    [SqlFile("19.sql")]
    [SqlFile("20.sql")]
    [SqlFile("21.sql")]
    [SqlFile("22.sql")]
    public void Test_Query_Files(string sql)
    {
        var ast = new Parser().ParseSql(sql);
        DefaultDialects = [new GenericDialect()];
        if (ast.Count == 1)
        {
            OneStatementParsesTo(sql, ast.ToSql());
        }

        Assert.NotNull(ast);
        output.WriteLine(ast.ToSql());
        output.WriteLine(new string('-', 50));
        output.WriteLine(JsonConvert.SerializeObject(ast));
    }
}

public class SqlFileAttribute : DataAttribute
{
    private readonly string _filePath;

    public SqlFileAttribute(string filePath)
    {
        _filePath = $"Queries/{filePath}";
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        if (testMethod == null) { throw new ArgumentNullException(nameof(testMethod)); }

        // Get the absolute path to the JSON file
        var path = Path.IsPathRooted(_filePath)
            ? _filePath
            : Path.GetRelativePath(Directory.GetCurrentDirectory(), _filePath);

        if (!File.Exists(path))
        {
            throw new ArgumentException($"Could not find file at path: {path}");
        }

        return new List<object[]> { new object[] { File.ReadAllText(path) } };
    }
}