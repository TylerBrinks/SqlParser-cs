namespace SqlParser.Ast;

public abstract record ForClause : IWriteSql
{
    public record Browse : ForClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("FOR BROWSE");
        }
    }
    public record Json(ForJson ForJson, string? Root, bool IncludeNullValues, bool WithoutArrayWrapper ) : ForClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FOR JSON {ForJson}");

            if (Root != null)
            {
                writer.WriteSql($", ROOT('{Root}')");
            }

            if (IncludeNullValues)
            {
                writer.Write(", INCLUDE_NULL_VALUES");
            }

            if (WithoutArrayWrapper)
            {
                writer.Write(", WITHOUT_ARRAY_WRAPPER");
            }
        }
    }
    public record Xml(ForXml ForXml, bool Elements, bool BinaryBase64, string? Root, bool Type) : ForClause
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"FOR XML {ForXml}");
            
            if (BinaryBase64)
            {
                writer.Write(", BINARY BASE64");
            }
            if (Type)
            {
                writer.Write(", TYPE");
            }

            if (Root != null)
            {
                writer.WriteSql($", ROOT('{Root}')");
            }

            if (Elements)
            {
                writer.Write(", ELEMENTS");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

public abstract record ForJson : IWriteSql
{
    public record Auto : ForJson
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("AUTO");
        }
    }
    public record Path : ForJson
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("PATH");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

public abstract record ForXml :IWriteSql
{
    public record Raw(string? Value) : ForXml
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("RAW");

            if (Value != null)
            {
                writer.Write($"('{Value}')");
            }
        }
    }

    public record Auto : ForXml
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("AUTO");
        }
    }

    public record Explicit : ForXml
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("EXPLICIT");
        }
    }

    public record Path(string? Value) : ForXml
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("PATH");

            if (Value != null)
            {
                writer.Write($"('{Value}')");
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}