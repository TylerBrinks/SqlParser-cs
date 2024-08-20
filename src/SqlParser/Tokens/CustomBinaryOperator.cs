namespace SqlParser.Tokens;

/// <summary>
/// Custom binary operator
/// This is used to represent any custom binary operator that is not part of the SQL standard.
/// PostgreSQL allows defining custom binary operators using CREATE OPERATOR.
/// </summary>
/// <param name="binaryOperator">Operator</param>
public class CustomBinaryOperator(string binaryOperator) : StringToken(binaryOperator);