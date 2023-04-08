namespace SqlParser.Dialects;

/// <summary>
/// ClickHouse SQL dialect
///
/// <see href="https://clickhouse.com/docs/en/sql-reference/ansi/"/>
/// </summary>
public class ClickHouseDialect : Dialect
{
    public override bool IsIdentifierStart(char character)
    {
        return character.IsLetter() || character == Symbols.Underscore;
    }

    public override bool IsIdentifierPart(char character)
    {
        return IsIdentifierStart(character) || character.IsDigit();
    }
}