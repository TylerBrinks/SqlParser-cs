namespace SqlParser.Tokens;

/// <summary>
/// jsonb @@ jsonpath → boolean: Returns the result of a JSON path predicate check
/// for the specified JSON value. Only the first item of the result is taken into
/// account. If the result is not Boolean then NULL is returned.
/// </summary>
public class AtAt() : StringToken("@@");