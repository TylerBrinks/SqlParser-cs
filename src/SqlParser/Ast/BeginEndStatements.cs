namespace SqlParser.Ast;

/// <summary>
/// Represents a list of statements enclosed within BEGIN and END keywords.
/// Example:
/// BEGIN
///     SELECT 1;
///     SELECT 2;
/// END
/// </summary>
public record BeginEndStatements(Sequence<Statement> Statements) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write("BEGIN");
        if (Statements.SafeAny())
        {
            writer.Write(" ");
            for (var i = 0; i < Statements.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write(" ");
                }
                Statements[i].ToSql(writer);
                writer.Write(";");
            }
        }
        writer.Write(" END");
    }
}
