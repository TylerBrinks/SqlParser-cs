
namespace SqlParser.Ast;
public abstract record UserDefinedTypeRepresentation : IWriteSql, IElement
{
    public record Composite(Sequence<UserDefinedTypeCompositeAttributeDef> Attributes) : UserDefinedTypeRepresentation;
    public record Enum(Sequence<Ident> Labels) : UserDefinedTypeRepresentation;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Composite c:
                writer.Write($"({c.Attributes.ToSqlDelimited()})");
                break;
            case Enum e:
                writer.Write($"ENUM ({e.Labels.ToSqlDelimited()})");
                break;
        }
    }
}
