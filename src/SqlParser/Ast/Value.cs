namespace SqlParser.Ast;

/// <summary>
/// Primitive SQL values such as number and string
/// </summary>
public abstract record Value : IWriteSql, IElement
{
    public abstract record StringBasedValue(string Value) : Value;

    /// <summary>
    /// Boolean value true or false
    /// </summary>
    /// <param name="Value">True or false</param>
    public record Boolean(bool Value) : Value
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Value);
        }
    }
    /// <summary>
    /// $tag_name$string value$tag_name$ - Postgres syntax
    /// </summary>
    /// <param name="Value">Quoted value</param>
    public record DollarQuotedString(DollarQuotedStringValue Value) : Value
    {
        public override void ToSql(SqlTextWriter writer)
        {
            Value.ToSql(writer);
        }
    }
    /// <summary>
    /// B"string value"
    /// </summary>
    /// <param name="Value">String value</param>
    public record DoubleQuotedString(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"\"{Value.EscapeDoubleQuoteString()}\"");
        }
    }
    /// <summary>
    /// e'string value' - Postgres extension
    /// <see href="https://www.postgresql.org/docs/8.3/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS"/>
    /// for more details.
    /// </summary>
    /// <param name="Value">String value</param>
    public record EscapedStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"E'{Value.EscapeEscapedString()}'");
        }
    }
    /// <summary>
    /// X'hex value'
    /// </summary>
    /// <param name="Value">String value</param>
    public record HexStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"X'{Value}'");
        }
    }
    /// <summary>
    /// N'string value'
    /// </summary>
    /// <param name="Value">String value</param>
    public record NationalStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"N'{Value}'");
        }
    }
    /// <summary>
    /// NULL value
    /// </summary>
    public record Null : Value
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"NULL");
        }
    }
    /// <summary>
    /// Numeric literal
    /// </summary>
    /// <param name="Value">String value</param>
    /// <param name="Long">True if long value</param>
    public record Number(string Value, bool Long = false) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Value}");
            writer.Write(Long ? "L" : null);
        }

        public int? AsInt()
        {
            if (int.TryParse(Value, out var val))
            {
                return val;
            }

            return null;
        }
    }
    /// <summary>
    /// `?` or `$` Prepared statement arg placeholder
    /// </summary>
    /// <param name="Value">String value</param>
    public record Placeholder(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Value);
        }
    }
    /// <summary>
    /// R'string value' or r'string value' or r"string value"
    /// <see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals"/>
    /// </summary>
    /// <param name="Value">String value</param>
    public record RawStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"R'{Value}'");
        }
    }
    /// <summary>
    /// Single quoted string value
    /// </summary>
    /// <param name="Value">String value</param>
    public record SingleQuotedString(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"'{Value.EscapeSingleQuoteString()}'");
        }
    }
    /// <summary>
    /// B'string value'
    /// </summary>
    /// <param name="Value">String value</param>
    public record SingleQuotedByteStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"B'{Value}'");
        }
    }
    /// <summary>
    /// B"string value"
    /// </summary>
    /// <param name="Value">String value</param>
    public record DoubleQuotedByteStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"B\"{Value}\"");
        }
    }
    /// <summary>
    /// Add support of snowflake field:key - key should be a value
    /// </summary>
    /// <param name="Value">String value</param>
    public record UnQuotedString(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Value);
        }
    }

    public abstract void ToSql(SqlTextWriter writer);

    public T As<T>() where T : Value
    {
        return (T) this;
    }

    public Number AsNumber()
    {
        return As<Number>();
    }
}
/// <summary>
/// Dollar quoted string value
/// </summary>
/// <param name="Value">String value</param>
/// <param name="Tag">Tag value</param>
public record DollarQuotedStringValue(string Value, string? Tag = null) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.Write(Tag != null 
            ? $"${Tag}${Value}${Tag}$" 
            : $"$${Value}$$");
    }
}