namespace SqlParser.Ast;

[AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Property)]
public class VisitAttribute : Attribute
{
    public int Order { get; init; }

    public VisitAttribute(int order)
    {
        Order = order;
    }
}