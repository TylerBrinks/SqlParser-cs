namespace SqlParser.Ast;

public abstract record Subscript : IWriteSql
{
    public record Index(Expression IndexExpression) : Subscript;
    public record Slice(Expression? LowerBound, Expression? UpperBound, Expression? Stride) : Subscript;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Index i:
                writer.WriteSql($"{i.IndexExpression}");
                break;

            case Slice s:
            {
                if (s.LowerBound != null)
                {
                    writer.WriteAsync($"{s.LowerBound}");
                }
                writer.Write(':');

                if (s.UpperBound != null)
                {
                    writer.WriteAsync($"{s.UpperBound}");
                }
           
                if (s.Stride != null)
                {
                    writer.WriteAsync($":{s.Stride}");
                }

                break;
            }
        }
    }
}
