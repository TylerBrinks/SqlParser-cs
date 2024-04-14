namespace SqlParser.Ast;

public abstract record AttachDuckDbDatabaseOption : IWriteSql
{
    public record ReadOnly(bool? IsReadOnly) : AttachDuckDbDatabaseOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            switch (IsReadOnly)
            {
                case true:
                    writer.Write("READ_ONLY true");
                    break;

                case false:
                    writer.Write("READ_ONLY false");
                    break;

                case null:
                    writer.Write("READ_ONLY");
                    break;
            }
        }
    }

    public record Type(Ident AttachType) : AttachDuckDbDatabaseOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"TYPE {AttachType}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}