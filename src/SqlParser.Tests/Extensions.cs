using SqlParser.Ast;
using SqlParser.Dialects;
using static SqlParser.Ast.SelectItem;

namespace SqlParser.Tests;

public static class Extensions
{
    public static void RunParserMethod(this IEnumerable<Dialect> dialects, string sql, Action<Parser> action)
    {
        foreach (var dialect in dialects)
        {
            var parser = new Parser();
            parser.TryWithSql(sql, dialect);
            action(parser);
        }
    }

    public static Expression AsExpr(this SelectItem item)
    {
        return item switch
        {
            UnnamedExpression u => u.Expression,
            SelectItem.ExpressionWithAlias e => e.Expression,
            _ => throw new NotImplementedException("AsExpr extension method needs a new Increment match added")
        };
    }
}