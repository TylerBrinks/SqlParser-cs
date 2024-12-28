using SqlParser;
using SqlParser.Ast;

namespace SqlParser.Ast;

public abstract record JsonTableColumn : IWriteSql, IElement
{

    public record Named(JsonTableNamedColumn Column) : JsonTableColumn
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Column}");
        }
    }

    public record ForOrdinality(Ident Name) : JsonTableColumn
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Name} FOR ORDINALITY");
        }
    }

    public record Nested(JsonTableNestedColumn Column) : JsonTableColumn
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{Column}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}

public record JsonTableNestedColumn(Value Path, Sequence<JsonTableColumn> Columns) : IWriteSql
{
    public void ToSql(SqlTextWriter writer)
    {
        writer.WriteSql($"NESTED PATH {Path} COLUMNS ({Columns.ToSqlDelimited()})");
    }
}

public record JsonTableNamedColumn(Ident Name, DataType Type, Value Path, bool Exists,
    JsonTableColumnErrorHandling? OnEmpty, JsonTableColumnErrorHandling? OnError) : IWriteSql, IElement
{
    public void ToSql(SqlTextWriter writer)
    {
        var exists = Exists ? " EXISTS" : null;

        writer.WriteSql($"{Name} {Type}{exists} PATH {Path}");

        if (OnEmpty != null)
        {
            writer.WriteSql($" {OnEmpty} ON EMPTY");
        }

        if (OnError != null)
        {
            writer.WriteSql($" {OnError} ON ERROR");
        }
    }
}

public abstract record JsonTableColumnErrorHandling : IWriteSql, IElement
{
    public record Null : JsonTableColumnErrorHandling;
    public record Default(Value Value) : JsonTableColumnErrorHandling;
    public record Error : JsonTableColumnErrorHandling;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Null:
                writer.Write("NULL");
                break;

            case Default d:
                writer.WriteSql($"DEFAULT {d.Value}");
                break;

            case Error:
                writer.Write("ERROR");
                break;
        }
    }
}