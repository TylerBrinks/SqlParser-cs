namespace SqlParser.Ast;

/// <summary>
/// Option for CREATE/ALTER OPERATOR statements
/// e.g., LEFTARG = box, RIGHTARG = box, FUNCTION = area_equal_function
/// </summary>
public record OperatorOption(Ident Name, Expression Value) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"{Name} = {Value}");
    }
}

/// <summary>
/// Item in CREATE OPERATOR CLASS AS clause
/// </summary>
public abstract record OperatorClassItem : IWriteSql, IElement
{
    /// <summary>
    /// OPERATOR strategy_number operator_name [ ( op_type, op_type ) ] [ FOR SEARCH | FOR ORDER BY sort_family_name ]
    /// </summary>
    public record Operator(int StrategyNumber, ObjectName Name) : OperatorClassItem
    {
        public DataType? LeftType { get; init; }
        public DataType? RightType { get; init; }
        public OperatorClassItemPurpose? Purpose { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OPERATOR {StrategyNumber} {Name}");
            if (LeftType != null || RightType != null)
            {
                writer.Write(" (");
                if (LeftType != null)
                {
                    writer.WriteSql($"{LeftType}");
                }
                else
                {
                    writer.Write("NONE");
                }
                writer.Write(", ");
                if (RightType != null)
                {
                    writer.WriteSql($"{RightType}");
                }
                else
                {
                    writer.Write("NONE");
                }
                writer.Write(")");
            }
            if (Purpose != null)
            {
                writer.WriteSql($" {Purpose}");
            }
        }
    }

    /// <summary>
    /// FUNCTION support_number [ ( op_type [ , op_type ] ) ] function_name ( argument_type [, ...] )
    /// </summary>
    public record Function(int SupportNumber, ObjectName Name) : OperatorClassItem
    {
        public Sequence<DataType>? ArgumentTypes { get; init; }

        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FUNCTION {SupportNumber} {Name}");
            if (ArgumentTypes.SafeAny())
            {
                writer.WriteSql($"({ArgumentTypes.ToSqlDelimited()})");
            }
        }
    }

    /// <summary>
    /// STORAGE storage_type
    /// </summary>
    public record Storage(DataType StorageType) : OperatorClassItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"STORAGE {StorageType}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Purpose for operator in operator class
/// </summary>
public abstract record OperatorClassItemPurpose : IWriteSql, IElement
{
    public record ForSearch : OperatorClassItemPurpose
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FOR SEARCH");
        }
    }

    public record ForOrderBy(ObjectName SortFamilyName) : OperatorClassItemPurpose
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FOR ORDER BY {SortFamilyName}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Operation for ALTER OPERATOR CLASS
/// </summary>
public abstract record AlterOperatorClassOperation : IWriteSql, IElement
{
    public record RenameTo(ObjectName NewName) : AlterOperatorClassOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {NewName}");
        }
    }

    public record OwnerTo(Ident NewOwner) : AlterOperatorClassOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OWNER TO {NewOwner}");
        }
    }

    public record SetSchema(ObjectName NewSchema) : AlterOperatorClassOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET SCHEMA {NewSchema}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Operation for ALTER OPERATOR FAMILY
/// </summary>
public abstract record AlterOperatorFamilyOperation : IWriteSql, IElement
{
    public record Add(Sequence<OperatorClassItem> Items) : AlterOperatorFamilyOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ADD {Items.ToSqlDelimited()}");
        }
    }

    public record Drop(Sequence<OperatorFamilyDropItem> Items) : AlterOperatorFamilyOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DROP {Items.ToSqlDelimited()}");
        }
    }

    public record RenameTo(ObjectName NewName) : AlterOperatorFamilyOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {NewName}");
        }
    }

    public record OwnerTo(Ident NewOwner) : AlterOperatorFamilyOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"OWNER TO {NewOwner}");
        }
    }

    public record SetSchema(ObjectName NewSchema) : AlterOperatorFamilyOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"SET SCHEMA {NewSchema}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

/// <summary>
/// Item to drop from operator family
/// </summary>
public abstract record OperatorFamilyDropItem : IWriteSql, IElement
{
    public record Operator(int StrategyNumber, DataType? LeftType, DataType? RightType) : OperatorFamilyDropItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"OPERATOR {StrategyNumber} (");
            if (LeftType != null)
            {
                writer.WriteSql($"{LeftType}");
            }
            else
            {
                writer.Write("NONE");
            }
            writer.Write(", ");
            if (RightType != null)
            {
                writer.WriteSql($"{RightType}");
            }
            else
            {
                writer.Write("NONE");
            }
            writer.Write(")");
        }
    }

    public record Function(int SupportNumber, DataType? LeftType, DataType? RightType) : OperatorFamilyDropItem
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"FUNCTION {SupportNumber} (");
            if (LeftType != null)
            {
                writer.WriteSql($"{LeftType}");
            }
            else
            {
                writer.Write("NONE");
            }
            writer.Write(", ");
            if (RightType != null)
            {
                writer.WriteSql($"{RightType}");
            }
            else
            {
                writer.Write("NONE");
            }
            writer.Write(")");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}
