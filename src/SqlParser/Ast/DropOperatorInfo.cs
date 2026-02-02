namespace SqlParser.Ast;

/// <summary>
/// Information about an operator to drop
/// DROP OPERATOR name ( { left_type | NONE } , right_type )
/// </summary>
public record DropOperatorInfo(ObjectName Name, DataType? LeftType, DataType RightType) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name}(");

        if (LeftType != null)
        {
            writer.WriteSql($"{LeftType}");
        }
        else
        {
            writer.Write("NONE");
        }

        writer.WriteSql($", {RightType})");
    }
}
