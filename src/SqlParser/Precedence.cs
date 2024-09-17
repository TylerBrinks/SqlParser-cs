namespace SqlParser;

public enum Precedence
{
    DoubleColon,
    AtTz,
    MulDivModOp,
    PlusMinus,
    Xor,
    Ampersand,
    Caret,
    Pipe,
    Between,
    Eq,
    Like,
    Is,
    PgOther,
    UnaryNot,
    And,
    Or,
}