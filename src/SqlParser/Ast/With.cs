namespace SqlParser.Ast;

/// <summary>
/// With statement
/// </summary>
/// <param name="Recursive">True if recursive</param>
/// <param name="CteTables">Common expression tables</param>
public record With(bool Recursive, Sequence<CommonTableExpression> CteTables) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var recursive = Recursive ? "RECURSIVE " : null;
        writer.WriteSql($"WITH {recursive}{CteTables}");
    }
}