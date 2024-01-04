using SqlParser.Dialects;
using SqlParser.Ast;
using static SqlParser.Ast.SetExpression;

namespace SqlParser.Tests.Dialects;

public class DuckDbDialectTests : ParserTestBase
{
    public DuckDbDialectTests()
    {
        DefaultDialects = new[] { new DuckDbDialect() };
    }

    [Fact]
    public void Test_Select_Wildcard_With_Exclude()
    {
        var select = VerifiedOnlySelect("SELECT * EXCLUDE (col_a) FROM data");
        SelectItem expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(new Sequence<Ident>{"col_a"})
        });
        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT name.* EXCLUDE department_id FROM employee_table");
        expected = new SelectItem.QualifiedWildcard(new ObjectName("name"), new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Single("department_id")
        });
        Assert.Equal(expected, select.Projection[0]);

        select = VerifiedOnlySelect("SELECT * EXCLUDE (department_id, employee_id) FROM employee_table");
        expected = new SelectItem.Wildcard(new WildcardAdditionalOptions
        {
            ExcludeOption = new ExcludeSelectItem.Multiple(new Sequence<Ident> {"department_id", "employee_id"})
        });
        Assert.Equal(expected, select.Projection[0]);
    }

    [Fact]
    public void Parse_Div_Infix()
    {
        VerifiedStatement("SELECT 5 / 2", new Dialect[] {new DuckDbDialect(), new GenericDialect()});
    }

    [Fact]
    public void Create_Macro()
    {
        var macro = VerifiedStatement("CREATE MACRO schema.add(a, b) AS a + b");
        var expected = new Statement.CreateMacro(false, false, new ObjectName(new Ident[] {"schema", "add"}),
            new Sequence<MacroArg> { new ("a"), new ("b") },
            new MacroDefinition.MacroExpression(new Expression.BinaryOp(
                new Expression.Identifier("a"),
                BinaryOperator.Plus,
                new Expression.Identifier("b"))));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Create_Macro_Default_Args()
    {
        var macro = VerifiedStatement("CREATE MACRO add_default(a, b := 5) AS a + b");

        var expected = new Statement.CreateMacro(false, false, new ObjectName(new Ident[] { "add_default" }),
            new Sequence<MacroArg>
            {
                new("a"),
                new("b", new Expression.LiteralValue(new Value.Number("5")))
            },
            new MacroDefinition.MacroExpression(new Expression.BinaryOp(
                new Expression.Identifier("a"),
                BinaryOperator.Plus,
                new Expression.Identifier("b"))));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Create_Table_Macro()
    {
        var query = "SELECT col1_value AS column1, col2_value AS column2 UNION ALL SELECT 'Hello' AS col1_value, 456 AS col2_value";
        var sql = "CREATE OR REPLACE TEMPORARY MACRO dynamic_table(col1_value, col2_value) AS TABLE " + query;

        var macro = (Statement.CreateMacro)VerifiedStatement(sql);

        var subquery = VerifiedQuery(query);
        var expected = new Statement.CreateMacro(true, true, new ObjectName(new Ident[] {"dynamic_table"}),
            new Sequence<MacroArg>
            {
                new("col1_value"),
                new("col2_value")
            },
            new MacroDefinition.MacroTable(subquery));

        Assert.Equal(expected, macro);
    }

    [Fact]
    public void Select_Union_By_Name()
    {
        var select = VerifiedQuery("SELECT * FROM capitals UNION BY NAME SELECT * FROM weather");

        var left = new SelectExpression(new Select(new Sequence<SelectItem>
        {
            new SelectItem.Wildcard(new WildcardAdditionalOptions())
        })
        {
            From = new Sequence<TableWithJoins>
            {
                new(new TableFactor.Table("capitals"))
            }
        });
        var right = new SelectExpression(new Select(new Sequence<SelectItem>
        {
            new SelectItem.Wildcard(new WildcardAdditionalOptions())
        })
        {
            From = new Sequence<TableWithJoins>
            {
                new(new TableFactor.Table("weather"))
            }
        });

        SetExpression expected = new SetOperation(left, SetOperator.Union, right, SetQuantifier.ByName);

        Assert.Equal(expected, select.Body);

        select = VerifiedQuery("SELECT * FROM capitals UNION ALL BY NAME SELECT * FROM weather");
        expected = new SetOperation(left, SetOperator.Union, right, SetQuantifier.ByName)
        {
            SetQuantifier = SetQuantifier.AllByName
        };

        Assert.Equal(expected, select.Body);
    }
}
