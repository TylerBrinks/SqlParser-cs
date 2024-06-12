namespace SqlParser.Ast;

/// <summary>
/// Top query qualifier
/// </summary>
/// <param name="Quantity">Quantity expression</param>
/// <param name="WithTies">True if with ties</param>
/// <param name="Percent">True if percentage</param>
public record Top(TopQuantity? Quantity, bool WithTies, bool Percent) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var extension = WithTies ? " WITH TIES" : null;

        var percent = Percent ? " PERCENT" : null;

        if (Quantity != null)
        {
            switch (Quantity)
            {
                case TopQuantity.TopExpression te:
                    writer.WriteSql($"TOP ({te.Expression}){percent}{extension}");
                    break;

                case TopQuantity.Constant c:
                    writer.WriteSql($"TOP {c.Quantity}{percent}{extension}");
                    break;
            }
        }
        else
        {
            writer.WriteSql($"TOP{extension}");
        }
    }
}

public abstract record TopQuantity : IElement
{
    public record TopExpression(Expression Expression) : TopQuantity;
    public record Constant(long Quantity) : TopQuantity;
}