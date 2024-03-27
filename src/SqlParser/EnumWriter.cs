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
            BinaryOperator.PGOverlap => "&&",
            BinaryOperator.PGRegexMatch => "~",
            BinaryOperator.PGRegexIMatch => "~*",
            BinaryOperator.PGRegexNotMatch => "!~",
            BinaryOperator.PGRegexNotIMatch => "!~*",
            BinaryOperator.PGStartsWith => "^@",
            BinaryOperator.PGLikeMatch => "~~",
            BinaryOperator.PGILikeMatch => "~~*",
            BinaryOperator.PGNotLikeMatch => "!~~",
            BinaryOperator.PGNotILikeMatch => "!~~*",
            
            CteAsMaterialized.NotMaterialized => "NOT MATERIALIZED",

            DateTimeField.TimezoneMinute => "TIMEZONE_MINUTE",
            DateTimeField.TimezoneHour => "TIMEZONE_HOUR",
            DateTimeField.TimezoneAbbr => "TIMEZONE_ABBR",
            DateTimeField.TimezoneRegion => "TIMEZONE_REGION",
            
            //DeclareType.Cursor => "CURSOR",
            //DeclareType.ResultSet => "RESULTSET",
            //DeclareType.Exception => "EXCEPTION",

            FileFormat.None => null,

            FlushType.BinaryLogs => "BINARY LOGS",
            FlushType.EngineLogs => "ENGINE LOGS",
            FlushType.ErrorLogs => "ERROR LOGS",
            FlushType.GeneralLogs => "GENERAL LOGS",
            FlushType.Hosts => "HOSTS",
            FlushType.Privileges => "PRIVILEGES",
            FlushType.OptimizerCosts => "OPTIMIZER_COSTS",
            FlushType.RelayLogs => "RELAY LOGS",
            FlushType.SlowLogs => "SLOW LOGS",
            FlushType.Status => "STATUS",
            FlushType.UserResources => "USER_RESOURCES",
            FlushType.Tables => "TABLES",

            HiveDelimiter.FieldsTerminatedBy => "FIELDS TERMINATED BY",
            HiveDelimiter.FieldsEscapedBy => "ESCAPED BY",
            HiveDelimiter.CollectionItemsTerminatedBy => "COLLECTION ITEMS TERMINATED BY",
            HiveDelimiter.MapKeysTerminatedBy => "MAP KEYS TERMINATED BY",
            HiveDelimiter.LinesTerminatedBy => "LINES TERMINATED BY",
            HiveDelimiter.NullDefinedAs => "NULL DEFINED AS",

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

            MySqlInsertPriority.LowPriority => "LOW_PRIORITY",
            MySqlInsertPriority.Delayed => "DELAYED",
            MySqlInsertPriority.HighPriority => "HIGH_PRIORITY",

            SetQuantifier.ByName => "BY NAME",
            SetQuantifier.AllByName => "ALL BY NAME",
            SetQuantifier.DistinctByName => "DISTINCT BY NAME",

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