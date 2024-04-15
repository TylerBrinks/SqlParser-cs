namespace SqlParser.Ast;

public abstract record DateTimeField : IWriteSql
{
    public record None : DateTimeField;

    public record Year : DateTimeField;

    public record Month : DateTimeField;

    public record Week(Ident? Weekday = null) : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("WEEK");
            if (Weekday != null)
            {
                writer.WriteSql($"({Weekday})");
            }
        }
    }

    public record Day : DateTimeField;

    public record DayOfWeek : DateTimeField;

    public record DayOfYear : DateTimeField;

    public record Date : DateTimeField;

    public record DateTime : DateTimeField;

    public record Hour : DateTimeField;

    public record Minute : DateTimeField;

    public record Second : DateTimeField;

    public record Century : DateTimeField;

    public record Decade : DateTimeField;

    public record Dow : DateTimeField;

    public record Doy : DateTimeField;

    public record Epoch : DateTimeField;

    public record Isodow : DateTimeField;

    public record IsoWeek : DateTimeField;

    public record Isoyear : DateTimeField;

    public record Julian : DateTimeField;

    public record Microsecond : DateTimeField;

    public record Microseconds : DateTimeField;
    // ReSharper disable once IdentifierTypo
    public record Millenium : DateTimeField;

    public record Millennium : DateTimeField;

    public record Millisecond : DateTimeField;

    public record Milliseconds : DateTimeField;

    public record Nanosecond : DateTimeField;

    public record Nanoseconds : DateTimeField;

    public record Quarter : DateTimeField;

    public record Time : DateTimeField;

    public record Timezone : DateTimeField;

    public record TimezoneAbbr : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TIMEZONE_ABBR");
        }
    }

    public record TimezoneHour : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TIMEZONE_HOUR");
        }
    }

    public record TimezoneMinute : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TIMEZONE_MINUTE");
        }
    }

    public record TimezoneRegion : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("TIMEZONE_REGION");
        }
    }

    public record NoDateTime : DateTimeField;

    public record Custom(Ident CustomDate) : DateTimeField
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"{CustomDate}");
        }
    }


    public virtual void ToSql(SqlTextWriter writer)
    {
        writer.Write(GetType().Name.ToUpper());
    }
}