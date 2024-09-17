namespace SqlParser.Ast;

public abstract record FromTable(Sequence<TableWithJoins> From) : IWriteSql, IElement
{
    /// An explicit `FROM` keyword was specified.
    public record WithFromKeyword(Sequence<TableWithJoins> From) : FromTable(From);

    /// BigQuery: `FROM` keyword was omitted.
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#delete_statement
    public record WithoutKeyword(Sequence<TableWithJoins> From) : FromTable(From);

    public void ToSql(SqlTextWriter writer)
    {
        if (this is WithFromKeyword)
        {
            writer.Write("FROM ");
        }

        writer.WriteDelimited(From, Constants.SpacedComma);
    }
}
