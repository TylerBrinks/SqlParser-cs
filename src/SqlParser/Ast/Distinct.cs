namespace SqlParser.Ast;

public abstract record DistinctFilter : IWriteSql
{
    public record Distinct : DistinctFilter
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DISTINCT");
        }
    }

    public record On(Sequence<Expression> ColumnNames) : DistinctFilter
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"DISTINCT ON ({ColumnNames.ToSqlDelimited()})");
        }
    }

    public virtual void ToSql(SqlTextWriter writer) { }
}
