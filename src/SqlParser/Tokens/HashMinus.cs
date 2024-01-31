namespace SqlParser.Tokens;

/// <summary>
/// jsonb #- text[] -> jsonb: Deletes the field or array element at the specified
/// path where path elements can be either field keys or array indexes.
/// </summary>
public class HashMinus() : StringToken("#-");