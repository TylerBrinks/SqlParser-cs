namespace SqlParser.Ast;

public record InsertAliases(ObjectName RowAlias, Sequence<Ident>? ColumnAliases) : IElement;