using SqlParser.Dialects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SqlParser.Ast;

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
}
