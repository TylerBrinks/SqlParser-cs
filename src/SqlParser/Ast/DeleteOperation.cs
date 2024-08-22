
namespace SqlParser.Ast;

public record DeleteOperation(
    Sequence<ObjectName>? Tables,
    Sequence<SelectItem>? Output,
    FromTable From,
    Sequence<OrderByExpression>? OrderBy = null,
    TableFactor? Using = null,
    Expression? Selection = null,
    Sequence<SelectItem>? Returning = null,
    Expression? Limit = null);
