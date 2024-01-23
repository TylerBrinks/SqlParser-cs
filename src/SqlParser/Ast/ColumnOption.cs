using SqlParser.Tokens;

namespace SqlParser.Ast;

/// <summary>
/// ColumnOptions are modifiers that follow a column definition in a CREATE TABLE statement.
/// </summary>
public abstract record ColumnOption : IWriteSql
{
    /// <summary>
    /// Null column option
    /// <example>
    /// <c>NULL</c>
    /// </example>
    /// </summary>
    public record Null : ColumnOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("NULL");
        }
    }
    /// <summary>
    /// Not Null column option
    /// <example>
    /// <c>NOT NULL</c>
    /// </example>
    /// </summary>
    public record NotNull : ColumnOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("NOT NULL");
        }
    }
    /// <summary>
    /// Default column option
    /// <example>
    /// <c>{ PRIMARY KEY | UNIQUE }</c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record Default(Expression Expression) : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DEFAULT {Expression}");
        }
    }
    /// <summary>
    /// Unique column option
    /// <example>
    /// <c>{ PRIMARY KEY | UNIQUE }</c>
    /// </example>
    /// </summary>
    /// <param name="IsPrimary">True if primary</param>
    public record Unique(bool IsPrimary) : ColumnOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(IsPrimary ? "PRIMARY KEY" : "UNIQUE");
        }
    }
    /// <summary>
    /// Referential integrity constraint column options
    /// <example>
    /// <c>
    /// ([FOREIGN KEY REFERENCES
    /// foreign_table (referred_columns)
    /// { [ON DELETE referential_action] [ON UPDATE referential_action] |
    ///   [ON UPDATE referential_action] [ON DELETE referential_action]
    /// })
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name">Name</param>
    /// <param name="ReferredColumns">Referred Columns</param>
    /// <param name="OnDeleteAction">On Delete Action</param>
    /// <param name="OnUpdateAction">On DoUpdate Action</param>
    public record ForeignKey(ObjectName Name, 
        Sequence<Ident>? ReferredColumns = null, 
        ReferentialAction OnDeleteAction = ReferentialAction.None, 
        ReferentialAction OnUpdateAction = ReferentialAction.None)
        : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"REFERENCES {Name}");
            if (ReferredColumns.SafeAny())
            {
                writer.WriteSql($" ({ReferredColumns})");
            }

            if (OnDeleteAction != ReferentialAction.None)
            {
                writer.WriteSql($" ON DELETE {OnDeleteAction}");
            }

            if (OnUpdateAction != ReferentialAction.None)
            {
                writer.WriteSql($" ON UPDATE {OnUpdateAction}");
            }
        }
    }
    /// <summary>
    /// Check expression column options
    /// <example>
    /// <c>
    /// CHECK (expr)
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record Check(Expression Expression) : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CHECK ({Expression})");
        }
    }
    /// <summary>
    ///  Dialect-specific options, such as:
    ///  MySQL's AUTO_INCREMENT or SQLite's `AUTOINCREMENT`
    /// </summary>
    /// <param name="Tokens">Tokens</param>
    public record DialectSpecific(Sequence<Token> Tokens) : ColumnOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            for (var i = 0; i < Tokens.Count; i++)
            {
                if (i > 0)
                {
                    writer.Write(" ");
                }

                writer.Write(Tokens[i]);
            }
        }
    }
    /// <summary>
    /// Character set options
    /// </summary>
    /// <param name="Name"></param>
    public record CharacterSet(ObjectName Name) : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CHARACTER SET {Name}");
        }
    }
    /// <summary>
    /// Comment column option
    /// </summary>
    /// <param name="Value">Comment value</param>
    public record Comment(string Value) : ColumnOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"COMMENT '{Value.EscapeSingleQuoteString()}'");
        }
    }
    /// <summary>
    /// On Update column options
    /// </summary>
    /// <param name="Expression">Expression</param>
    public record OnUpdate(Expression Expression) : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ON UPDATE {Expression}");
        }
    }
    /// <summary>
    /// Generated are modifiers that follow a column definition in a CREATE TABLE statement.
    /// </summary>
    /// <param name="GeneratedAs">Generated as</param>
    /// <param name="SequenceOptions">Sequence options</param>
    /// <param name="GenerationExpr">Generation expression</param>
    public record Generated(GeneratedAs GeneratedAs,
        bool GeneratedKeyword,
        Sequence<SequenceOptions>? SequenceOptions = null,
        Expression? GenerationExpr = null,
        GeneratedExpressionMode? GenerationExpressionMode = null) : ColumnOption, IElement
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (GenerationExpr != null)
            {
                var modifier = GenerationExpressionMode switch
                {
                    GeneratedExpressionMode.Virtual => " VIRTUAL",
                    GeneratedExpressionMode.Sorted => " STORED",
                    _ => null
                };

                if (GeneratedKeyword)
                {
                    writer.WriteSql($"GENERATED ALWAYS AS ({GenerationExpr}){modifier}");
                }
                else
                {
                    writer.WriteSql($"AS ({GenerationExpr}){modifier}");
                }
            }
            else
            {
                var when = GeneratedAs switch
                {
                    GeneratedAs.Always => "ALWAYS",
                    GeneratedAs.ByDefault => "BY DEFAULT",
                    _ => throw new ParserException("SORTED not valid")
                };
                writer.Write($"GENERATED {when} AS IDENTITY");

                if (SequenceOptions!=null)
                {
                    if (SequenceOptions.Any())
                    {
                        writer.Write(" (");
                        writer.WriteDelimited(SequenceOptions, string.Empty);
                        writer.Write(" )");
                    }
                }
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}