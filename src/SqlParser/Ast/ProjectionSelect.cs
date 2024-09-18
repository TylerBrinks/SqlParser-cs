namespace SqlParser.Ast;

public record ProjectionSelect(Sequence<SelectItem> Projection, OrderBy? OrderBy, GroupByExpression? GroupBy) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
       writer.WriteSql($"SELECT {Projection.ToSqlDelimited()}");

       if (GroupBy != null)
       {
           writer.WriteSql($" {GroupBy}");
       }

       if (OrderBy != null)
       {
           writer.WriteSql($" {OrderBy}");
       }
    }
}
