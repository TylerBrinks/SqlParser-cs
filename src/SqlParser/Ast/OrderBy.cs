namespace SqlParser.Ast;

public record OrderBy(Sequence<OrderByExpression>? Expressions, Interpolate? Interpolate) : IElement;