namespace SqlParser.Ast;

/// <summary>
/// Top query qualifier
/// </summary>
/// <param name="Quantity">Quantity expression</param>
/// <param name="WithTies">True if with ties</param>
/// <param name="Percent">True if percentage</param>
public record Top(Expression? Quantity, bool WithTies, bool Percent) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var extension = WithTies ? " WITH TIES" : null;

        if (Quantity != null)
        {
            var percent = Percent ? " PERCENT" : null;
            writer.WriteSql($"TOP ({Quantity}){percent}{extension}");
        }
        else
        {
            writer.WriteSql($"TOP{extension}");
        }
    }
}