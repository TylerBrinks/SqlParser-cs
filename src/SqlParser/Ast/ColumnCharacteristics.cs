namespace SqlParser.Ast;

public record ConstraintCharacteristics : IWriteSql
{
    public bool? Deferrable { get; set; }
    public DeferrableInitial? Initially { get; set; }
    public bool? Enforced { get; set; }

    public void ToSql(SqlTextWriter writer)
    {
        var deferrable = Deferrable is true ? "DEFERRABLE" : "NOT DEFERRABLE";
        var initial = Initially is DeferrableInitial.Deferred ? "INITIALLY DEFERRED" : "INITIALLY IMMEDIATE";
        var enforced = Enforced is true ? "ENFORCED" : "NOT ENFORCED";

        switch (Deferrable)
        {
            case null when !Initially.HasValue && Enforced.HasValue:
                writer.Write($"{enforced}");
                break;
            case null when Initially.HasValue && !Enforced.HasValue:
                writer.Write($"{initial}");
                break;
            case null when Initially.HasValue && Enforced.HasValue:
                writer.Write($"{initial} {enforced}");
                break;
            default:
            {
                if (Deferrable.HasValue && !Initially.HasValue && !Enforced.HasValue)
                {
                    writer.Write($"{deferrable}");
                }
                else if (Deferrable.HasValue && !Initially.HasValue && Enforced.HasValue)
                {
                    writer.Write($"{deferrable} {enforced}");
                }
                else if (Deferrable.HasValue && Initially.HasValue && !Enforced.HasValue)
                {
                    writer.Write($"{deferrable} {initial}");
                }
                else if (Deferrable.HasValue && Initially.HasValue && Enforced.HasValue)
                {
                    writer.Write($"{deferrable} {initial} {enforced}");
                }

                break;
            }
        }
    }
}