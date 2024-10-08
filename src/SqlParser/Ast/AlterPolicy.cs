namespace SqlParser.Ast;

public abstract record AlterPolicyOperation : IWriteSql, IElement
{
    public record Rename(Ident NewName) : AlterPolicyOperation;
    public record Apply(Sequence<Owner>? To, Expression? Using, Expression? WithCheck) : AlterPolicyOperation;

    public void ToSql(SqlTextWriter writer)
    {
        switch (this)
        {
            case Rename r:
                writer.WriteSql($" RENAME TO {r.NewName}");
                break;

            case Apply a:
                if (a.To != null)
                {
                    writer.WriteSql($" TO {a.To.ToSqlDelimited()}");
                }

                if (a.Using != null)
                {
                    writer.WriteSql($" USING ({a.Using})");
                }

                if (a.WithCheck != null)
                {
                    writer.WriteSql($" WITH CHECK ({a.WithCheck})");
                }
                break;
        }
    }
}
