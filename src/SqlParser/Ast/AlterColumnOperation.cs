namespace SqlParser.Ast;

/// <summary>
/// Alter column SQL operations
/// </summary>
public abstract record AlterColumnOperation : IWriteSql
{
    /// <summary>
    /// Set not null column operation
    /// <exmaple>
    /// <c>
    /// SET NOT NULL
    /// </c>
    /// </exmaple>
    /// </summary>
    public record SetNotNull : AlterColumnOperation;
    /// <summary>
    /// Drop not null column operation
    /// <exmaple>
    /// <c>
    /// DROP NOT NULL
    /// </c>
    /// </exmaple>
    /// </summary>
    public record DropNotNull : AlterColumnOperation;
    /// <summary>
    /// Set default column operation
    /// <exmaple>
    /// <c>
    /// SET DEFAULT
    /// </c>
    /// </exmaple>
    /// </summary>
    /// <param name="Value">Expression value</param>
    public record SetDefault(Expression Value) : AlterColumnOperation;
    /// <summary>
    /// Drop default column operation
    /// <exmaple>
    /// <c>
    /// DROP DEFAULT
    /// </c>
    /// </exmaple>
    /// </summary>
    public record DropDefault : AlterColumnOperation;
    /// <summary>
    /// Set data type column operation
    /// <exmaple>
    /// <c>
    /// [SET DATA] TYPE data_type [USING expr]
    /// </c>
    /// </exmaple>
    /// </summary>
    /// <param name="DataType"></param>
    public record SetDataType(DataType DataType, Expression? Using = null) : AlterColumnOperation, IElement;

    public record AddGenerated(GeneratedAs? GeneratedAs, Sequence<SequenceOptions> SequenceOptions) : AlterColumnOperation, IElement;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case SetNotNull:
                writer.Write("SET NOT NULL");
                break;

            case DropNotNull:
                writer.Write("DROP NOT NULL");
                break;

            case SetDefault sd:
                writer.WriteSql($"SET DEFAULT {sd.Value}");
                break;

            case DropDefault:
                writer.Write("DROP DEFAULT");
                break;

            case SetDataType sdt:

                writer.WriteSql($"SET DATA TYPE {sdt.DataType}");

                if (sdt.Using != null)
                {
                    writer.WriteSql($" USING {sdt.Using}");
                }
               
                break;

            case AddGenerated ag:
                var genAs = ag.GeneratedAs switch
                {
                    GeneratedAs.Always => " ALWAYS",
                    GeneratedAs.ByDefault => " BY DEFAULT",
                    _ => string.Empty
                };
                writer.Write($"ADD GENERATED{genAs} AS IDENTITY");
                if (ag.SequenceOptions.SafeAny())
                {
                    writer.Write(" (");
                    writer.WriteDelimited(ag.SequenceOptions, "");
                    writer.Write(" )");
                }
                break;

        }
    }
}