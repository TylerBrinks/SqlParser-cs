namespace SqlParser.Ast;

/// <summary>
/// Operate function argument
/// </summary>
/// <param name="Mode">Argument mode</param>
public record OperateFunctionArg(ArgMode Mode) : IWriteSql, IElement
{
    public Ident? Name { get; init; }
    public DataType? DataType { get; init; }
    public Expression? DefaultExpr { get; init; }

    public void ToSql(SqlTextWriter writer)
    {
        if (Mode != ArgMode.None)
        {
            writer.WriteSql($"{Mode} ");
        }

        if (Name != null)
        {
            writer.WriteSql($"{Name} ");
        }

        if (DataType != null)
        {
            writer.WriteSql($"{DataType}");
        }

        if (DefaultExpr != null)
        {
            writer.WriteSql($" = {DefaultExpr}");
        }
    }
}