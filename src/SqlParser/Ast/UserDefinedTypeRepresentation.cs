
namespace SqlParser.Ast;
public abstract record UserDefinedTypeRepresentation : IWriteSql
{
    public record Composite(Sequence<UserDefinedTypeCompositeAttributeDef> Attributes) : UserDefinedTypeRepresentation;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is Composite c)
        {
            writer.Write("(");
            writer.WriteDelimited(c.Attributes, ", ");
            writer.Write(")");
        }
    }
}
