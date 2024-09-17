namespace SqlParser.Ast;

/// <summary>
/// Was there an explicit ROWs keyword (MySQL)?
/// <see href="https://dev.mysql.com/doc/refman/8.0/en/values.html"/>
/// </summary>
public record Values(Sequence<Sequence<Expression>> Rows, bool ExplicitRow = false) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("VALUES ");

        var prefix = ExplicitRow ? "ROW" : null;

        for (var i = 0; i < Rows.Count; i++)
        {
            if (i > 0)
            {
                writer.WriteCommaSpaced();
            }

            writer.WriteSql($"{prefix}({Rows[i]})");
        }
    }
}