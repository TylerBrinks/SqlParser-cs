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
    /// Double-quoted literal with raw string prefix. Example `R"abc"`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String value</param>
    public record DoubleQuotedRawStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"R\"{Value}\"");
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
    /// Single quoted literal with raw string prefix. Example `R'abc'`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value"></param>
    public record SingleQuotedRawStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"R'{Value}'");
        }
    }
    /// <summary>
    /// Triple double quoted strings: Example """abc"""
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String value</param>
    public record TripleSingleQuotedString(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"'''{Value}'''");
        }
    }
    /// <summary>
    /// Triple double quoted strings: Example """abc"""
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String Value</param>
    public record TripleDoubleQuotedString(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"\"\"\"{Value}\"\"\"");
        }
    }
    /// <summary>
    /// Triple single quoted literal with byte string prefix. Example `B'''abc'''`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String Value</param>
    public record TripleSingleQuotedByteStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"B'''{Value}'''");
        }
    }
    /// <summary>
    /// Triple double-quoted literal with byte string prefix. Example `B"""abc"""`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String Value</param>
    public record TripleDoubleQuotedByteStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"B\"\"\"{Value}\"\"\"");
        }
    }
    /// <summary>
    /// Triple single quoted literal with raw string prefix. Example `R'''abc'''`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value">String Value</param>
    public record TripleSingleQuotedRawStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"R'''{Value}'''");
        }
    }
    /// <summary>
    /// Triple double-quoted literal with raw string prefix. Example `R"""abc"""`
    /// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#quoted_literals
    /// </summary>
    /// <param name="Value"></param>
    public record TripleDoubleQuotedRawStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"R\"\"\"{Value}\"\"\"");
        }
    }

    public record UnicodeStringLiteral(string Value) : StringBasedValue(Value)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"U&'{Value.EscapeUnicodeString()}'");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);

    public T As<T>() where T : Value
    {
        return (T)this;
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