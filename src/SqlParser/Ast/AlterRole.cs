using static SqlParser.Ast.ResetConfig;

namespace SqlParser.Ast;

public abstract record RoleOption : IWriteSql, IElement
{
    public record BypassRls(bool Bypass) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Bypass ? "BYPASSRLS" : "NOBYPASSRLS");
        }
    }

    public record ConnectionLimit(Expression Expression) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"CONNECTION LIMIT {Expression}");
        }
    }

    public record CreateDb(bool Create) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Create ? "CREATEDB" : "NOCREATEDB");
        }
    }

    public record CreateRole(bool Create) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Create ? "CREATEROLE" : "NOCREATEROLE");
        }
    }

    public record Inherit(bool InheritValue) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(InheritValue ? "INHERIT" : "NOINHERIT");
        }
    }

    public record Login(bool LoginValue) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(LoginValue ? "LOGIN" : "NOLOGIN");
        }
    }

    public record PasswordOption(Password Password) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            switch (Password)
            {
                case Password.ValidPassword v:
                    writer.WriteSql($"PASSWORD {v.Expression}");
                    break;
                case Password.NullPassword:
                    writer.Write("PASSWORD NULL");
                    break;
            }
        }
    }

    public record Replication(bool Replicate) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(Replicate ? "REPLICATION" : "NOREPLICATION");
        }
    }
   
    public record SuperUser(bool IsSuperUser) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write(IsSuperUser ? "SUPERUSER" : "NOSUPERUSER");
        }
    }

    public record ValidUntil(Expression Expression) : RoleOption
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"VALID UNTIL {Expression}");
        }
    }
    public abstract void ToSql(SqlTextWriter writer);
}

public abstract record SetConfigValue : IElement
{
    public record Default : SetConfigValue;
    public record FromCurrent : SetConfigValue;
    public record Value(Expression Expression) : SetConfigValue;
}

public abstract record ResetConfig : IElement
{
    public record All : ResetConfig;

    public record ConfigName(ObjectName Name) : ResetConfig;
}

public abstract record AlterRoleOperation : IWriteSql, IElement
{
    public record RenameRole(Ident RoleName) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"RENAME TO {RoleName}");
        }
    }

    public record AddMember(Ident MemberName) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"ADD MEMBER {MemberName}");
        }
    }

    public record DropMember(Ident MemberName) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.WriteSql($"DROP MEMBER {MemberName}");
        }
    }

    public record WithOptions(Sequence<RoleOption> Options) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write($"WITH {Options.ToSqlDelimited(Symbols.Space)}");
        }
    }

    public record Set(ObjectName ConfigName, SetConfigValue ConfigValue, ObjectName? InDatabase) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (InDatabase != null)
            {
                writer.WriteSql($"IN DATABASE {InDatabase} ");
            }

            switch (ConfigValue)
            {
                case SetConfigValue.Default:
                    writer.WriteSql($"SET {ConfigName} TO DEFAULT");
                    break;
                case SetConfigValue.FromCurrent:
                    writer.WriteSql($"SET {ConfigName} FROM CURRENT");
                    break;
                case SetConfigValue.Value value:
                    writer.WriteSql($"SET {ConfigName} TO {value.Expression}");
                    break;
            }
        }
    }

    public record Reset(ResetConfig ConfigName, ObjectName? InDatabase) : AlterRoleOperation
    {
        public override void ToSql(SqlTextWriter writer)
        {
            if (InDatabase != null)
            {
                writer.WriteSql($"IN DATABASE {InDatabase} ");
            }

            switch (ConfigName)
            {
                case All:
                    writer.Write("RESET ALL");
                    break;

                case ConfigName c:
                    writer.WriteSql($"RESET {c.Name}");
                    break;
            }
        }
    }

    public abstract void ToSql(SqlTextWriter writer);
}