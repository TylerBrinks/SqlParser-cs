namespace SqlParser.Tokens;

/// <summary>
/// jsonb @? jsonpath -> boolean: Does JSON path return any item for the specified
/// JSON value?
/// </summary>
public class AtQuestion() : StringToken("@?");