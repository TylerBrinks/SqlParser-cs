namespace SqlParser.Ast;

[AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Property)]
public class VisitAttribute(int order) : Attribute
{
    public int Order { get; init; } = order;
}