namespace SqlParser.Ast;

/// <summary>
/// SQL data types
/// </summary>
public abstract record DataType : IWriteSql, IElement
{
    /// <summary>
    /// Data type with character length specificity
    /// </summary>
    public abstract record CharacterLengthDataType(CharacterLength? CharacterLength) : DataType
    {
        protected CharacterLength? CharLength = CharacterLength;

        protected ulong? IntegerLength => CharLength is CharacterLength.IntegerLength length
            ? length.Length
            : null;

        protected void FormatCharacterStringType(SqlTextWriter writer, string sqlType, ulong? length)
        {
            writer.Write(sqlType);

            if (length != null)
            {
                writer.Write($"({length})");
            }
        }
    }

    /// <summary>
    /// Data type with length specificity
    /// </summary>
    /// <param name="Length">Data type length</param>
    public abstract record LengthDataType(ulong? Length = null) : DataType
    {
        protected void FormatTypeWithOptionalLength(SqlTextWriter writer, string sqlType, ulong? length, bool unsigned = false)
        {
            writer.Write($"{sqlType}");

            if (length != null)
            {
                writer.Write($"({length})");
            }
            if (unsigned)
            {
                writer.Write(" UNSIGNED");
            }
        }
    }
    /// <summary>
    /// Data type with exact number specificity
    /// </summary>
    /// <param name="ExactNumberInfo"></param>
    public abstract record ExactNumberDataType(ExactNumberInfo? ExactNumberInfo) : DataType;
    /// <summary>
    /// Data type with time zone information
    /// </summary>
    /// <param name="TimezoneInfo">Time zone info</param>
    /// <param name="Length"></param>
    public abstract record TimeZoneDataType(TimezoneInfo TimezoneInfo, ulong? Length = null) : DataType
    {
        protected void FormattedDatetimePrecisionAndTz(SqlTextWriter writer, string sqlType)
        {
            writer.Write($"{sqlType}");
            string? length = null;

            if (Length != null)
            {
                length = $"({Length})";
            }

            if (TimezoneInfo == TimezoneInfo.Tz)
            {
                writer.WriteSql($"{TimezoneInfo}{length}");
            }
            else if (TimezoneInfo != TimezoneInfo.None)
            {
                writer.WriteSql($"{length} {TimezoneInfo}");
            }
        }
    }

    /// <summary>
    /// Array data type
    /// </summary>
    /// <param name="DataType"></param>
    public record Array(ArrayElementTypeDef DataType) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            switch (DataType)
            {
                case ArrayElementTypeDef.None:
                    writer.Write("ARRAY");
                    break;

                case ArrayElementTypeDef.SquareBracket sb:
                    writer.WriteSql($"{DataType}[{sb.Size}]");
                    break;

                case ArrayElementTypeDef.AngleBracket:
                    writer.WriteSql($"ARRAY<{DataType}>");
                    break;
            }
        }
    }
    /// <summary>
    /// Big integer with optional display width e.g. BIGINT or BIGINT(20)
    /// </summary>
    /// <param name="Length">Length</param>
    public record BigInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "BIGINT", Length);
        }
    }
    /// <summary>
    /// This is alias for `BigNumeric` type used in BigQuery
    ///
    /// <see href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types"/>
    /// </summary>
    /// <param ExactNumberInfo="Exact number"></param>
    public record BigNumeric(ExactNumberInfo ExactNumberInfo) : ExactNumberDataType(ExactNumberInfo)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once StringLiteralTypo
            writer.WriteSql($"BIGNUMERIC{ExactNumberInfo}");
        }
    }
    /// <summary>
    /// Fixed-length binary type with optional length e.g.
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type"/>
    /// <see href="https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16"/>
    /// </summary>
    /// <param name="Length">Length</param>
    public record Binary(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "BINARY", Length);
        }
    }
    /// <summary>
    /// Large binary object with optional length e.g. BLOB, BLOB(1000)
    /// 
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-large-object-string-type"/>
    /// <see href="https://docs.oracle.com/javadb/10.8.3.0/ref/rrefblob.html"/>
    /// </summary>
    public record Blob(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "BLOB", Length);
        }
    }
    /// <summary>
    /// Boolean data type
    /// </summary>
    public record Bool : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("BOOL");
        }
    }
    public record Boolean : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("BOOLEAN");
        }
    }
    /// <summary>
    /// Binary string data type
    /// </summary>
    // ReSharper disable IdentifierTypo
    public record Bytea : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // ReSharper disable once StringLiteralTypo
            writer.Write("BYTEA");
        }
    }
    /// Variable-length binary data with optional length.
    ///
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#bytes_type
    public record Bytes(ulong? Length) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            //writer.Write("BYTES");
            FormatTypeWithOptionalLength(writer, "BYTES", Length);
        }
    }
    /// <summary>
    /// Fixed-length char type e.g. CHAR(10)
    /// </summary>
    public record Char(CharacterLength? CharacterLength = null) : CharacterLengthDataType(CharacterLength)
    {
        public override void ToSql(SqlTextWriter writer)
        {

            FormatCharacterStringType(writer, "CHAR", IntegerLength);
        }
    }
    /// <summary>
    /// Fixed-length character type e.g. CHARACTER(10)
    /// </summary>
    public record Character(CharacterLength? CharacterLength = null) : CharacterLengthDataType(CharacterLength)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatCharacterStringType(writer, "CHARACTER", IntegerLength);
        }
    }
    /// <summary>
    /// Large character object with optional length e.g. CHARACTER LARGE OBJECT, CHARACTER LARGE OBJECT(1000)
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type"/>
    /// </summary>
    /// <param name="Length">Length</param>
    public record CharacterLargeObject(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "CHARACTER", Length);
        }
    }
    /// <summary>
    /// Character varying type e.g. CHARACTER VARYING(10)
    /// </summary>
    public record CharacterVarying(CharacterLength? CharacterLength = null) : CharacterLengthDataType(CharacterLength)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (CharacterLength != null)
            {
                FormatCharacterStringType(writer, "CHARACTER VARYING", IntegerLength);
            }
        }
    }
    /// <summary>
    /// Large character object with optional length e.g. CHAR LARGE OBJECT, CHAR LARGE OBJECT(1000)
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type"/>
    /// </summary>
    /// <param name="Length">Length</param>
    public record CharLargeObject(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "CHARACTER LARGE OBJECT", Length);
        }
    }
    /// <summary>
    /// Char varying type e.g. CHAR VARYING(10)
    /// </summary>
    public record CharVarying(CharacterLength? CharacterLength = null) : CharacterLengthDataType(CharacterLength)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (CharacterLength != null)
            {
                FormatCharacterStringType(writer, "CHAR VARYING", IntegerLength);
            }
        }
    }
    /// <summary>
    /// Large character object with optional length e.g. CLOB, CLOB(1000)
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#character-large-object-type"/>
    /// <see href="https://docs.oracle.com/javadb/10.10.1.2/ref/rrefclob.html"/>
    /// </summary>
    public record Clob(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "CLOB", Length);
        }
    }
    /// <summary>
    /// Custom type such as enums
    /// </summary>
    public record Custom(ObjectName Name, Sequence<string>? Values = null) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Values.SafeAny())
            {
                writer.WriteSql($"{Name}({Values})");
            }
            else
            {
                Name.ToSql(writer);
            }
        }
    }
    /// <summary>
    /// Date data type
    /// </summary>
    public record Date : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DATE");
        }
    }
    /// <summary>
    /// Date32 with the same range as Datetime64
    /// </summary>
    public record Date32 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DATE32");
        }
    }
    /// <summary>
    /// Datetime with optional time precision e.g. MySQL
    /// 
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/datetime.html"/>
    /// </summary>
    public record Datetime(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "DATETIME", Length);
        }
    }
    /// <summary>
    /// Datetime with time precision and optional timezone e.g. ClickHouse.
    /// https://dev.mysql.com/doc/refman/8.0/en/datetime.html
    /// </summary>
    public record Datetime64(ulong Precision, string? TimeZone = null) : LengthDataType(Precision)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DateTime64({Precision}");

            if (TimeZone != null)
            {
                writer.WriteSql($", '{TimeZone}'");
            }

            writer.Write(')');
        }
    }
    /// <summary>
    /// Dec data type with optional precision and scale e.g. DEC(10,2): DataType
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type"/>
    /// </summary>
    public record Dec(ExactNumberInfo ExactNumberInfo) : ExactNumberDataType(ExactNumberInfo)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DEC{ExactNumberInfo}");
        }
    }
    /// <summary>
    /// Decimal type with optional precision and scale e.g. DECIMAL(10,2)
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type"/>
    /// </summary>
    public record Decimal(ExactNumberInfo ExactNumberInfo) : ExactNumberDataType(ExactNumberInfo)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DECIMAL{ExactNumberInfo}");
        }
    }
    /// <summary>
    /// Double data type
    /// </summary>
    public record Double : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DOUBLE");
        }
    }
    /// <summary>
    /// Double PRECISION e.g. standard, PostgreSql
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#approximate-numeric-type"/>
    /// <see href="https://www.postgresql.org/docs/current/datatype-numeric.html"/>
    /// </summary>
    public record DoublePrecision : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("DOUBLE PRECISION");
        }
    }
    /// <summary>
    /// Enum data types 
    /// </summary>
    /// <param name="Values"></param>
    public record Enum(Sequence<string> Values) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("ENUM(");
            for (var i = 0; i < Values.Count; i++)
            {
                if (i > 0)
                {
                    writer.WriteCommaSpaced();
                }
                writer.Write($"'{Values[i].EscapeSingleQuoteString()}'");
            }
            writer.Write(')');
        }
    }
    /// <summary>
    /// Fixed string
    /// </summary>
    /// <param name="Length"></param>
    public record FixedString(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "FixedString", Length);
        }
    }
    /// <summary>
    /// Floating point with optional precision e.g. FLOAT(8)
    /// </summary>
    public record Float(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "FLOAT", Length);
        }
    }
    /// <summary>
    /// FLOAT4 as alias for Real in postgresql
    /// </summary>
    public record Float4 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FLOAT4");
        }
    }
    /// <summary>
    /// FLOAT8 as alias for Double in postgresql
    /// </summary>
    public record Float8 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FLOAT8");
        }
    }
    /// <summary>
    /// Floating point in Clickhouse
    /// </summary>
    public record Float32 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FLOAT32");
        }
    }
    /// <summary>
    /// FLOAT64
    /// </summary>
    public record Float64 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FLOAT64");
        }
    }
    /// <summary>
    /// Integer with optional display width e.g. INT or INT(11)
    /// <param name="Length">Length</param>
    /// </summary>
    public record Int(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT", Length);
        }
    }
    /// <summary>
    /// Integer with optional display width e.g. INTEGER or INTEGER(11)
    /// </summary>
    public record Integer(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INTEGER", Length);
        }
    }
    /// <summary>
    /// Interval data type
    /// </summary>
    public record Interval : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INTERVAL");
        }
    }
    /// <summary>
    /// Int2 as alias for SmallInt in [postgresql]
    /// Note: Int2 mean 2 bytes in postgres (not 2 bits)
    /// Int2 with optional display width e.g. INT2 or INT2(5)
    /// </summary>
    /// <param name="Length">Length</param>
    public record Int2(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT2", Length);
        }
    }
    /// <summary>
    /// Int4 as alias for Integer in [postgresql]
    /// Note: Int4 mean 4 bytes in postgres (not 4 bits)
    /// Int4 with optional display width e.g. Int4 or Int4(11)
    /// </summary>
    /// <param name="Length">Length</param>
    public record Int4(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT4", Length);
        }
    }
    /// <summary>
    /// Int8 as alias for Bigint in [postgresql]
    /// Note: Int8 mean 8 bytes in postgres (not 8 bits)
    /// Int8 with optional display width e.g. INT8 or INT8(11)
    /// </summary>
    /// <param name="Length">Length</param>
    public record Int8(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT8", Length);
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: Int16 mean 16 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record Int16 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INT16");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse, BigQuery
    /// </summary>
    public record Int32 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INT32");
        }
    }
    /// Integer type in [bigquery]
    ///
    /// [bigquery]: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#integer_types
    public record Int64 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INT64");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: Int128 mean 128 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record Int128 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INT128");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: Int256 mean 256 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record Int256 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("INT256");
        }
    }
    /// <summary>
    /// Json data type
    /// </summary>
    public record Json : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("JSON");
        }
    }
    /// <summary>
    /// Binary Json data type
    /// </summary>
    public record JsonB : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("JSONB");
        }
    }
    /// <summary>
    /// LowCardinality - changes the internal representation of other data types to be dictionary-encoded.
    /// </summary>
    public record LowCardinality(DataType DataType) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"LowCardinality({DataType})");
        }
    }
    /// <summary>
    ///
    /// Map Clickhouse: https://clickhouse.com/docs/en/sql-reference/data-types/map
    /// </summary>
    /// <param name="KeyDataType"></param>
    /// <param name="ValueDataType"></param>
    public record Map(DataType KeyDataType, DataType ValueDataType) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"Map({KeyDataType}, {ValueDataType})");
        }
    }
    /// <summary>
    /// MySQL medium integer ([1]) with optional display width e.g. MEDIUMINT or MEDIUMINT(5)
    ///
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/integer-types.html"/>
    /// </summary>
    public record MediumInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "MEDIUMINT", Length);
        }
    }
    /// <summary>
    /// Nested Clickhouse:https://clickhouse.com/docs/en/sql-reference/data-types/nested-data-structures/nested
    /// </summary>
    /// <param name="Columns"></param>
    public record Nested(Sequence<ColumnDef> Columns) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"Tuple({Columns.ToSqlDelimited()})");
        }
    }
    /// <summary>
    /// Empty data type
    /// </summary>
    public record None : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
        }
    }
    /// <summary>
    /// Nullable - special marker NULL represents in ClickHouse as a data type.
    /// </summary>
    /// <param name="DataType"></param>
    public record Nullable(DataType DataType) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"Nullable({DataType})");
        }
    }
    /// <summary>
    /// Numeric type with optional precision and scale e.g. NUMERIC(10,2) 
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#exact-numeric-type"/>
    /// </summary>
    public record Numeric(ExactNumberInfo ExactNumberInfo) : ExactNumberDataType(ExactNumberInfo)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"NUMERIC{ExactNumberInfo}");
        }
    }
    /// <summary>
    /// Variable-length character type e.g. NVARCHAR(10)
    /// </summary>
    public record Nvarchar(CharacterLength? Length = null) : CharacterLengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatCharacterStringType(writer, "NVARCHAR", IntegerLength);
        }
    }
    /// <summary>
    /// Floating point e.g. REAL
    /// </summary>
    public record Real : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("REAL");
        }
    }
    /// <summary>
    /// Regclass used in postgresql serial
    /// </summary>
    public record Regclass : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("REGCLASS");
        }
    }
    /// <summary>
    /// Set data type
    /// </summary>
    public record Set(Sequence<string> Values) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SET(");
            for (var i = 0; i < Values.Count; i++)
            {
                if (i > 0)
                {
                    writer.WriteCommaSpaced();
                }

                writer.Write($"'{Values[i].EscapeSingleQuoteString()}'");
            }
            writer.Write(')');
        }
    }
    /// <summary>
    /// Small integer with optional display width e.g. SMALLINT or SMALLINT(5)
    /// </summary>
    public record SmallInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "SMALLINT", Length);
        }
    }
    /// <summary>
    /// String data type
    /// </summary>
    public record StringType(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "STRING", Length);
        }
    }
    /// Struct
    ///
    /// Hive: https://docs.cloudera.com/cdw-runtime/cloud/impala-sql-reference/topics/impala-struct.html
    /// BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    public record Struct(Sequence<StructField> Fields, StructBracketKind Bracket) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (Fields.SafeAny())
            {
                switch (Bracket)
                {
                    case StructBracketKind.Parentheses:
                        writer.WriteSql($"STRUCT({Fields.ToSqlDelimited()})");
                        break;

                    case StructBracketKind.AngleBrackets:
                        writer.Write($"STRUCT<{Fields.ToSqlDelimited()}>");
                        break;
                }
            }
            else
            {
                writer.Write("STRUCT");
            }
        }
    }
    /// <summary>
    /// Text data type
    /// </summary>
    public record Text : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TEXT");
        }
    }
    /// <summary>
    /// Time with optional time precision and time zone information
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type"/>
    /// </summary>
    public record Time(TimezoneInfo TimezoneInfo, ulong? When = null) : TimeZoneDataType(TimezoneInfo, When)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormattedDatetimePrecisionAndTz(writer, "TIME");
        }
    }
    /// <summary>
    /// Timestamp with optional time precision and time zone information
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#datetime-type"/>
    /// </summary>
    public record Timestamp(TimezoneInfo TimezoneInfo, ulong? When = null) : TimeZoneDataType(TimezoneInfo, When)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormattedDatetimePrecisionAndTz(writer, "TIMESTAMP");
        }
    }
    /// <summary>
    /// Tiny integer with optional display width e.g. TINYINT or TINYINT(3)
    /// </summary>
    public record TinyInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "TINYINT", Length);
        }
    }
    /// <summary>
    /// Trigger data type, returned by functions associated with triggers
    /// </summary>
    public record Trigger : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TRIGGER");
        }
    }
    /// <summary>
    /// Tuple Clickhouse: https://clickhouse.com/docs/en/sql-reference/data-types/tuple
    /// </summary>
    /// <param name="Fields"></param>
    public record Tuple(Sequence<StructField> Fields) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"Tuple({Fields.ToSqlDelimited()})");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt8 mean 8 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt8 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT8");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt16 mean 16 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt16 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT16");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt32 mean 32 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt32 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT32");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt64 mean 64 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt64 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT64");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt128 mean 128 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt128 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT128");
        }
    }
    /// <summary>
    /// Integer type in Clickhouse
    /// Note: UInt256 mean 256 bits in Clickhouse
    /// https://clickhouse.com/docs/en/sql-reference/data-types/int-uint
    /// </summary>
    public record UInt256 : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UINT256");
        }
    }
    /// <summary>
    /// Union
    ///
    /// DuckDb https://duckdb.org/docs/sql/data_types/union.html
    /// </summary>
    /// <param name="Values"></param>
    public record Union(Sequence<UnionField> Fields) : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"UNION({Fields.ToSqlDelimited()})");
        }
    }
    /// <summary>
    /// Unsigned big integer with optional display width e.g. BIGINT UNSIGNED or BIGINT(20) UNSIGNED
    /// </summary>
    public record UnsignedBigInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "BIGINT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned integer with optional display width e.g. INT UNSIGNED or INT(11) UNSIGNED
    /// </summary>
    public record UnsignedInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned integer with optional display width e.g. INTEGER UNSIGNED or INTEGER(11) UNSIGNED
    /// </summary>
    public record UnsignedInteger(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned medium integer ([1]) with optional display width e.g. MEDIUMINT UNSIGNED or MEDIUMINT(5) UNSIGNED
    ///
    /// <see href="https://dev.mysql.com/doc/refman/8.0/en/integer-types.html"/>
    /// </summary>
    public record UnsignedMediumInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "MEDIUMINT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned small integer with optional display width e.g. SMALLINT UNSIGNED or SMALLINT(5) UNSIGNED
    /// </summary>
    public record UnsignedSmallInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "SMALLINT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned tiny integer with optional display width e.g. TINYINT UNSIGNED or TINYINT(3) UNSIGNED
    /// </summary>
    public record UnsignedTinyInt(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "TINYINT", Length, true);
        }
    }
    /// <summary>
    /// Unsigned Int2 with optional display width e.g. INT2 Unsigned or INT2(5) Unsigned
    /// </summary>
    /// <param name="Length"></param>
    public record UnsignedInt2(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT2", Length, true);
        }
    }
    /// <summary>
    /// Unsigned Int4 with optional display width e.g. INT4 Unsigned or INT4(5) Unsigned
    /// </summary>
    /// <param name="Length"></param>
    public record UnsignedInt4(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT4", Length, true);
        }
    }
    /// <summary>
    /// Unsigned Int8 with optional display width e.g. INT8 Unsigned or INT8(5) Unsigned
    /// </summary>
    /// <param name="Length"></param>
    public record UnsignedInt8(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "INT8", Length, true);
        }
    }
    /// <summary>
    /// No type specified - only used with SQLite
    /// </summary>
    public record Unspecified : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            // No text written
        }
    }
    /// <summary>
    /// UUID data ype
    /// </summary>
    public record Uuid : DataType
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("UUID");
        }
    }
    /// <summary>
    /// Variable-length binary with optional length type
    ///
    /// <see href="https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html#binary-string-type"/>
    /// <see href="https://learn.microsoft.com/pt-br/sql/t-sql/data-types/binary-and-varbinary-transact-sql?view=sql-server-ver16"/>
    /// </summary>
    public record Varbinary(ulong? Length = null) : LengthDataType(Length)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatTypeWithOptionalLength(writer, "VARBINARY", Length);
        }
    }
    /// <summary>
    /// Variable-length character type e.g. VARCHAR(10)
    /// </summary>
    public record Varchar(CharacterLength? CharacterLength = null) : CharacterLengthDataType(CharacterLength)
    {
        public override void ToSql(SqlTextWriter writer)
        {
            FormatCharacterStringType(writer, "VARCHAR", IntegerLength);
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}