namespace SqlParser.Ast;

public abstract record TableVersion : IWriteSql
{
    public record ForSystemTimeAsOf(Expression Expression) : TableVersion
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($" FOR SYSTEM_TIME AS OF {Expression}");
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}