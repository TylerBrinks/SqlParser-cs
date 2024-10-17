namespace SqlParser.Ast;

public abstract record WrappedCollection<T>(Sequence<T> Items) : IWriteSql where T : IWriteSql
{
    public record NoWrapping(Sequence<T> Items) : WrappedCollection<T>(Items);
    public record Parentheses(Sequence<T> Items) : WrappedCollection<T>(Items);

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case NoWrapping:
                writer.WriteSql($"{Items.ToSqlDelimited()}");
                break;

            case Parentheses:
                writer.WriteSql($"({Items.ToSqlDelimited()})");
                break;
        }
    }
}