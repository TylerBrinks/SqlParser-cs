namespace SqlParser.Ast;

public abstract record CreateTableOptions : IWriteSql
{
    public record None : CreateTableOptions;
    public record With(Sequence<SqlOption> OptionsList) : CreateTableOptions;
    public record Options(Sequence<SqlOption> OptionsList) : CreateTableOptions;


    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case With w:
                writer.Write($"WITH ({w.OptionsList.ToSqlDelimited()})");
                break;

            case Options o:
                writer.Write($"OPTIONS ({o.OptionsList.ToSqlDelimited()})");
                break;
        }
    }
}