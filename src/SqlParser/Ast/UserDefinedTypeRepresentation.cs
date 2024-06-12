
namespace SqlParser.Ast;
public abstract record UserDefinedTypeRepresentation : IWriteSql, IElement
{
    public record Composite(Sequence<UserDefinedTypeCompositeAttributeDef> Attributes) : UserDefinedTypeRepresentation;

    public void ToSql(SqlTextWriter writer)
    {
        if (this is Composite c)
        {
            writer.Write($"({c.Attributes.ToSqlDelimited()})");
        }
    }
}
