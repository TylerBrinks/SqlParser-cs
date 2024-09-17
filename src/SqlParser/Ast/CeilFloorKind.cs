namespace SqlParser.Ast;

public abstract record CeilFloorKind : IElement
{
    public record DateTimeFieldKind(DateTimeField Field) : CeilFloorKind;
    public record Scale(Value Field) : CeilFloorKind;
}
