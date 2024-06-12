namespace SqlParser.Ast;

/// <summary>
/// Copy targets
/// </summary>
public abstract record CopyTarget : IWriteSql, IElement
{
    /// <summary>
    /// Stdin copy target
    /// </summary>
    public record Stdin : CopyTarget;
    /// <summary>
    /// Stdin copy target
    /// </summary>
    public record Stdout : CopyTarget;
    /// <summary>
    /// File copy target
    /// </summary>
    /// <param name="FileName">File name</param>
    public record File(string FileName) : CopyTarget;
    /// <summary>
    /// Program copy target
    /// </summary>
    /// <param name="Comment">Comment value</param>
    public record Program(string Comment) : CopyTarget;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Stdin:
                writer.Write("STDIN");
                break;

            case Stdout:
                writer.Write("STDOUT");
                break;

            case File f:
                writer.Write($"'{f.FileName.EscapeSingleQuoteString()}'");
                break;

            case Program p:
                writer.Write($"PROGRAM '{p.Comment.EscapeSingleQuoteString()}'");
                break;
        }
    }
}
/// <summary>
/// Copy source
/// </summary>
public abstract record CopySource : IElement
{
    public record Table(ObjectName TableName, Sequence<Ident> Columns) : CopySource;

    public record CopySourceQuery(Query? Query) : CopySource;
}
/// <summary>
/// Copy options
/// </summary>
public abstract record CopyOption : IWriteSql, IElement
{
    /// <summary>
    /// <example>
    /// <c>
    /// FORMAT format_name 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name"></param>
    public record Format(Ident Name) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// FREEZE [ boolean ] 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Frozen"></param>
    public record Freeze(bool Frozen) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// DELIMITER 'delimiter_character'
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Character"></param>
    public record Delimiter(char Character) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// NULL 'null_string'
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Value"></param>
    public record Null(string Value) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// HEADER [ boolean ] 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="HeaderValue"></param>
    public record Header(bool HeaderValue) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// QUOTE 'quote_character' 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Character"></param>
    public record Quote(char Character) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// ESCAPE 'escape_character'
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Character"></param>
    public record Escape(char Character) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// FORCE_QUOTE { ( column_name [, ...] ) | * }
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Names"></param>
    public record ForceQuote(Sequence<Ident> Names) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// FORCE_NOT_NULL ( column_name [, ...] ) 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Names"></param>
    public record ForceNotNull(Sequence<Ident> Names) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// FORCE_NULL ( column_name [, ...] ) 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Names"></param>
    public record ForceNull(Sequence<Ident> Names) : CopyOption;
    /// <summary>
    /// <example>
    /// <c>
    /// ENCODING 'encoding_name' 
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Name"></param>
    public record Encoding(string Name) : CopyOption;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Format f:
                writer.WriteSql($"FORMAT {f.Name}");
                break;

            case Freeze { Frozen: true }:
                writer.Write("FREEZE TRUE");
                break;

            case Freeze { Frozen: false }:
                writer.Write("FREEZE FALSE");
                break;

            case Delimiter d:
                writer.WriteSql($"DELIMITER '{d.Character}'");
                break;

            case Null n:
                writer.Write($"NULL '{n.Value.EscapeSingleQuoteString()}'");
                break;

            case Header { HeaderValue: true }:
                writer.Write("HEADER");
                break;

            case Header { HeaderValue: false }:
                writer.Write("HEADER FALSE");
                break;

            case Quote q:
                writer.Write($"QUOTE '{q.Character}'");
                break;

            case Escape e:
                writer.Write($"ESCAPE '{e.Character}'");
                break;

            case ForceQuote fq:
                writer.WriteSql($"FORCE_QUOTE ({fq.Names})");
                break;

            case ForceNotNull fnn:
                writer.WriteSql($"FORCE_NOT_NULL ({fnn.Names})");
                break;

            case ForceNull fn:
                writer.WriteSql($"FORCE_NULL ({fn.Names})");
                break;

            case Encoding en:
                writer.Write($"ENCODING '{en.Name.EscapeSingleQuoteString()}'");
                break;
        }
    }
}
/// <summary>
/// Copy legacy options
/// </summary>
public abstract record CopyLegacyOption : IWriteSql, IElement
{
    /// <summary>
    /// Binary copy option
    /// <example>
    /// <c>
    /// BINARY
    /// </c>
    /// </example>
    /// </summary>
    public record Binary : CopyLegacyOption;

    /// <summary>
    /// Delimiter copy option
    /// <example>
    /// <c>
    /// DELIMITER [ AS ] 'delimiter_character'
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Character">Character delimiter</param>
    public record Delimiter(char Character) : CopyLegacyOption;

    /// <summary>
    /// Null copy option
    /// <example>
    /// <c>
    /// NULL [ AS ] 'null_string'
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Value">String value</param>
    public record Null(string Value) : CopyLegacyOption;

    /// <summary>
    /// CSV copy option
    /// <example>
    /// <c>
    /// CSV ...
    /// </c>
    /// </example>
    /// </summary>
    /// <param name="Options">Legacy copy options</param>
    public record Csv(Sequence<CopyLegacyCsvOption> Options) : CopyLegacyOption;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Binary:
                writer.Write("BINARY");
                break;

            case Delimiter d:
                writer.Write($"DELIMITER '{d.Character}'");
                break;

            case Null n:
                writer.Write($"NULL '{n.Value.EscapeSingleQuoteString()}'");
                break;

            case Csv c:
                writer.Write($"CSV {c.Options.ToSqlDelimited(Symbols.Space)}");
                break;
        }
    }
}

public abstract record CopyLegacyCsvOption : IWriteSql, IElement
{
    /// <summary>
    /// HEADER
    /// </summary>
    public record Header : CopyLegacyCsvOption;
    /// <summary>
    /// QUOTE [ AS ] 'quote_character'
    /// </summary>
    /// <param name="Character"></param>
    public record Quote(char Character) : CopyLegacyCsvOption;
    /// <summary>
    /// ESCAPE [ AS ] 'escape_character'
    /// </summary>
    /// <param name="Character"></param>
    public record Escape(char Character) : CopyLegacyCsvOption;
    /// <summary>
    /// FORCE QUOTE { column_name [, ...] | * }
    /// </summary>
    /// <param name="Names"></param>
    public record ForceQuote(Sequence<Ident> Names) : CopyLegacyCsvOption;
    /// <summary>
    /// FORCE NOT NULL column_name [, ...]
    /// </summary>
    /// <param name="Names"></param>
    public record ForceNotNull(Sequence<Ident> Names) : CopyLegacyCsvOption;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Header:
                writer.Write("HEADER");
                break;

            case Quote q:
                writer.Write($"QUOTE '{q.Character}'");
                break;

            case Escape e:
                writer.Write($"ESCAPE '{e.Character}'");
                break;

            case ForceQuote fq:
                writer.WriteSql($"FORCE QUOTE {fq.Names}");
                break;

            case ForceNotNull fn:
                writer.WriteSql($"FORCE NOT NULL {fn.Names}");
                break;
        }
    }
}
