
namespace SqlParser.Ast;

public record TableEngine(string Name, Sequence<Ident>? Parameters = null) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}");

        if (Parameters.SafeAny())
        {
            writer.WriteSql($"({Parameters.ToSqlDelimited()})");
        }
    }
}
