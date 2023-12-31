using SqlParser.Ast;

namespace SqlParser;

/// <summary>
/// Converts enum values to strings.  The default behavior is to
/// convert the enum name to an upper case string.  Otherwise each
/// case returns the expected string equivalent
/// </summary>
public static class EnumWriter
{
    public static string? Write(Enum e)
    {
        return e switch
        {
            AddDropSync.Add => "ADD PARTITIONS",
            AddDropSync.Drop => "DROP PARTITIONS",
            AddDropSync.Sync => "SYNC PARTITIONS",

            BinaryOperator.Plus => "+",
            BinaryOperator.Minus => "-",
            BinaryOperator.Multiply => "*",
            BinaryOperator.Divide => "/",
            BinaryOperator.Modulo => "%",
            BinaryOperator.StringConcat => "||",
            BinaryOperator.Gt => ">",
            BinaryOperator.Lt => "<",
            BinaryOperator.GtEq => ">=",
            BinaryOperator.LtEq => "<=",
            BinaryOperator.Spaceship => "<=>",
            BinaryOperator.Eq => "=",
            BinaryOperator.NotEq => "<>",
            BinaryOperator.And => "AND",
            BinaryOperator.Or => "OR",
            BinaryOperator.Xor => "XOR",
            BinaryOperator.BitwiseOr => "|",
            BinaryOperator.BitwiseAnd => "&",
            BinaryOperator.BitwiseXor => "^",
            BinaryOperator.DuckIntegerDivide => "//",
            BinaryOperator.MyIntegerDivide => "DIV",
            BinaryOperator.PGBitwiseXor => "#",
            BinaryOperator.PGBitwiseShiftLeft => "<<",
            BinaryOperator.PGBitwiseShiftRight => ">>",
            BinaryOperator.PGExp => "^",
            BinaryOperator.PGRegexMatch => "~",
            BinaryOperator.PGRegexIMatch => "~*",
            BinaryOperator.PGRegexNotMatch => "!~",
            BinaryOperator.PGRegexNotIMatch => "!~*",

            DateTimeField.TimezoneMinute => "TIMEZONE_MINUTE",
            DateTimeField.TimezoneHour => "TIMEZONE_HOUR",

            FileFormat.None => null,

            JsonOperator.Arrow => "->",
            JsonOperator.LongArrow => "->>",
            JsonOperator.HashArrow => "#>",
            JsonOperator.HashLongArrow => "#>>",
            JsonOperator.Colon => ":",
            JsonOperator.AtArrow => "@>",
            JsonOperator.ArrowAt => "<@",
            JsonOperator.HashMinus => "#-",
            JsonOperator.AtQuestion => "@?",
            JsonOperator.AtAt => "@@",

            NonBlock.SkipLocked => "SKIP LOCKED",

            ReferentialAction.NoAction => "NO ACTION",
            ReferentialAction.SetDefault => "SET DEFAULT",
            ReferentialAction.SetNull => "SET NULL",

            SearchModifier.InBooleanMode => "IN BOOLEAN MODE",
            SearchModifier.InNaturalLanguageMode => "IN NATURAL LANGUAGE MODE",
            SearchModifier.InNaturalLanguageModeWithQueryExpansion => "IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION",
            SearchModifier.WithQueryExpansion => "WITH QUERY EXPANSION",

            TimezoneInfo.WithTimeZone => "WITH TIME ZONE",
            TimezoneInfo.WithoutTimeZone => "WITHOUT TIME ZONE",

            TransactionIsolationLevel.ReadCommitted => "READ COMMITTED",
            TransactionIsolationLevel.ReadUncommitted => "READ UNCOMMITTED",
            TransactionIsolationLevel.RepeatableRead => "REPEATABLE READ",
            TransactionAccessMode.ReadOnly => "READ ONLY",
            TransactionAccessMode.ReadWrite => "READ WRITE",

            UnaryOperator.Plus => "+",
            UnaryOperator.Minus => "-",
            UnaryOperator.Not => "NOT",
            UnaryOperator.PGBitwiseNot => "~",
            UnaryOperator.PGSquareRoot => "|/",
            UnaryOperator.PGCubeRoot => "||/",
            UnaryOperator.PGPostfixFactorial => "!",
            UnaryOperator.PGPrefixFactorial => "!!",
            UnaryOperator.PGAbs => "@",

            _ => e.ToString().ToUpperInvariant()
        };
    }
}