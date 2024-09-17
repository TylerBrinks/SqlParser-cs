namespace SqlParser.Ast;

/// <summary>
/// A table name or a parenthesized subquery with an optional alias
/// </summary>
public record TableFunctionArgs(Sequence<FunctionArg> Arguments, Sequence<Setting>? Settings = null) : IElement;