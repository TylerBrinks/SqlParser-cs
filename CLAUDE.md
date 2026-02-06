# Agent Instructions for SqlParser-CS

## Project Overview

SqlParser-CS is a .NET port of the Rust [sqlparser-rs project](https://github.com/sqlparser-rs/sqlparser-rs). It's a SQL lexer and parser that generates an Abstract Syntax Tree (AST) conforming to ANSI/ISO SQL standards and various SQL dialects.

### Key Characteristics
- **Port from Rust**: Maintains functional parity with the original Rust implementation
- **Syntax-only parsing**: Focuses on syntactic analysis, not semantic validation
- **Dialect-extensible**: Supports multiple SQL dialects with customization hooks
- **Target frameworks**: .NET 8, .NET 9, .NET 10

## Architecture and Design Patterns

### Core Design Patterns

1. **Pratt Parser (TDOP)**
   - The expression parser uses Top-Down Operator Precedence parsing
   - Defined in `Parser.Expressions.cs`
   - Handles operator precedence and associativity declaratively

2. **Recursive Descent Parser**
   - Statement parsing uses traditional hand-written recursive descent
   - Defined in `Parser.Statements.cs`
   - Favored over parser generators for simplicity and debuggability

3. **Visitor Pattern**
   - Base implementation in `Ast/Visitor.cs`
   - Traverses the entire AST hierarchy
   - Provides pre/post visit hooks for Statements, Expressions, and Relations

### Project Structure

```
src/
├── SqlParser/              # Core library
│   ├── Ast/               # Abstract Syntax Tree types
│   ├── Dialects/          # SQL dialect implementations
│   ├── Tokens/            # Token definitions
│   ├── Parser.cs          # Main parser logic
│   ├── Parser.Base.cs     # Parser base methods
│   ├── Parser.Expressions.cs  # Expression parsing
│   ├── Parser.Statements.cs   # Statement parsing
│   ├── Tokenizer.cs       # Lexical analysis
│   └── SqlQueryParser.cs  # Public API entry point
├── SqlParser.Tests/       # Test suite (xUnit)
├── SqlParser.Benchmarks/  # Performance benchmarks
└── SqlParserDemo/         # Console demo application
```

## C# Conventions Specific to This Project

### Abstract Base Records with Nested Subtypes

This project uses an **unconventional but intentional pattern** to emulate Rust's discriminated unions:

```csharp
// Abstract base record
public abstract record Statement : IWriteSql, IElement
{
    // Nested concrete implementations
    public record Select(Query Query) : Statement { ... }
    public record Insert(InsertStatement Insert) : Statement { ... }
    public record Delete(DeleteStatement Delete) : Statement { ... }
}

// Usage
var statement = new Statement.Select(query);
```

**Rationalle**
- Mimics Rust's enum variants and unions as much as C# can
- Provides pseudo-namespacing for related types
- Enables pattern matching with type tests
- Maintains close alignment with Rust source

**Key types using this pattern:**
- `Statement` - Top-level SQL statements
- `Expression` - SQL expressions
- `DataType` - SQL data types
- `TableFactor` - Table references
- `SetExpression` - Set operations (UNION, INTERSECT, etc.)

### Code Style

1. **Record Types Everywhere**
   - Use `record` for immutable AST nodes
   - Use `record struct` for small value types
   - Leverage positional syntax for concise definitions

2. **File Organization**
   - One primary type per file
   - Nested types stay with their parent
   - Large types split into partial classes (e.g., `Dialect.cs` + `Dialect.Props.cs`)

3. **Naming Conventions**
   - PascalCase for types, properties, and methods
   - Avoid abbreviations except well-known ones (SQL, AST, etc.)
   - Match Rust naming where possible for maintainability

4. **ReSharper Suppressions**
   - Common suppressions at file level:
     ```csharp
     // ReSharper disable StringLiteralTypo
     // ReSharper disable CommentTypo
     ```
   - Used to handle SQL keywords and dialect-specific terms

## Working with Dialects

### Creating Custom Dialects

All dialects inherit from `Dialect` abstract class:

```csharp
public class MyCustomDialect : Dialect
{
    public override bool IsIdentifierStart(char ch) => ...;
    public override bool IsIdentifierPart(char ch) => ...;
    
    // Optional: Override parsing behavior
    public override Statement? ParseStatement(Parser parser) => ...;
    public override Expression? ParsePrefix(Parser parser) => ...;
    public override Expression? ParseInfix(Parser parser, Expression expr, int precedence) => ...;
}
```

**Key extension points:**
- `IsIdentifierStart/Part` - Define valid identifier characters
- `IsDelimitedIdentifierStart` - Define quote characters
- `ParseStatement` - Custom statement syntax
- `ParsePrefix/Infix` - Custom expression operators
- `GetNextPrecedence` - Custom operator precedence

### Supported Dialects

Located in `src/SqlParser/Dialects/`:
- GenericDialect (baseline)
- AnsiDialect
- BigQueryDialect
- ClickHouseDialect
- DatabricksDialect
- DuckDbDialect
- HiveDialect
- MsSqlDialect
- MySqlDialect
- OracleDialect
- PostgreSqlDialect
- RedshiftDialect
- SnowflakeDialect
- SQLiteDialect

## Testing Guidelines

### Test Framework: xUnit

All tests inherit from `ParserTestBase`:

```csharp
public class MyDialectTests : ParserTestBase
{
    [Fact]
    public void Parse_Custom_Syntax()
    {
        var sql = "SELECT * FROM table";
        var statement = VerifiedStatement(sql);
        // Assertions...
    }
}
```

### Helper Methods

- `VerifiedStatement(sql)` - Parse and verify single statement
- `VerifiedStatement<T>(sql)` - Parse and cast to specific type
- `VerifiedQuery(sql)` - Parse and return Query
- `VerifiedOnlySelect(sql)` - Parse and return Select
- `VerifiedExpr(sql)` - Parse expression
- `OneStatementParsesTo(sql, canonical)` - Verify normalization

### Test Organization

```
SqlParser.Tests/
├── Dialects/              # Dialect-specific tests
│   ├── PostgresDialectTests.cs
│   ├── MsSqlDialectTests.cs
│   └── ...
├── ParserCommonTests.cs   # General parser tests
├── TokenizerTests.cs      # Lexer tests
└── ParserTestBase.cs      # Base class with helpers
```

### Writing Tests

**DO:**
- Test across multiple dialects using `AllDialects`
- Verify SQL round-tripping: parse → ToSql() → parse again
- Test both valid syntax and expected parse errors
- Use xUnit's `[Fact]` and `[Theory]` appropriately

**DON'T:**
- Test semantic validation (e.g., duplicate column names)
- Assume specific error messages
- Create tests that depend on execution order

## Common Patterns and Utilities

### IWriteSql Interface

All AST nodes implement `IWriteSql`:

```csharp
public interface IWriteSql
{
    void ToSql(SqlTextWriter writer);
}

// Usage
var sql = statement.ToSql(); // Extension method
```

### SqlTextWriter

Specialized writer for SQL output with interpolation support:

```csharp
writer.WriteSql($"SELECT {projection} FROM {table}");
```

### Sequence<T>

Immutable list wrapper used throughout AST:

```csharp
public record Select
{
    public Sequence<SelectItem> Projection { get; init; }
    // ...
}
```

### ObjectName

Represents multi-part identifiers:

```csharp
var name = new ObjectName("schema", "table");
// Renders as: schema.table
```

### Parser State Management

- `DepthGuard` - Prevents stack overflow during deep recursion
- `ParserState` - Tracks parsing context
- `Location` - Tracks line/column for error reporting

## Extending the Parser

### Adding New Statement Types

1. Define the record in `Ast/Statement.cs` or new file:
   ```csharp
   public record MyStatement(ObjectName Name, Expression Value) : Statement
   {
       public override void ToSql(SqlTextWriter writer)
       {
           writer.WriteSql($"MY STATEMENT {Name} VALUE {Value}");
       }
   }
   ```

2. Add parsing logic in dialect or `Parser.Statements.cs`:
   ```csharp
   public override Statement? ParseStatement(Parser parser)
   {
       if (parser.ParseKeyword(Keyword.MY))
       {
           // Parse the custom syntax
           return new Statement.MyStatement(...);
       }
       return null;
   }
   ```

3. Add tests in `SqlParser.Tests/`:
   ```csharp
   [Fact]
   public void Parse_My_Statement()
   {
       var stmt = VerifiedStatement<Statement.MyStatement>("MY STATEMENT...");
       Assert.Equal(...);
   }
   ```

### Adding New Expression Types

Follow the same pattern in `Ast/Expression.cs` and `Parser.Expressions.cs`.

## Performance Considerations

- **StringBuilderPool** - Pooled StringBuilder instances for ToSql()
- **Span<T>** - Used for tokenization where applicable
- **Lazy evaluation** - AST nodes created on-demand
- **Benchmarks** - Located in `SqlParser.Benchmarks/` using BenchmarkDotNet

## Alignment with Rust Source

### Maintaining Parity

When making changes:

1. **Check Rust source first** - Verify the feature exists in sqlparser-rs
2. **Match structure** - Keep class/method organization similar
3. **Port tests** - Translate Rust tests to C# xUnit tests
4. **Coordinate PRs** - Ideally submit to both repositories

### Differences from Rust

**Acceptable differences:**
- C# naming conventions (PascalCase vs snake_case)
- Use of properties instead of fields
- Extension methods for convenience
- Additional helper methods for .NET ergonomics

**Must match:**
- AST structure and hierarchy
- Parsing logic and output
- Supported syntax and dialects
- Test coverage

## Common Tasks

### Debugging Parse Issues

1. Check tokenization first:
   ```csharp
   var tokens = new Tokenizer().Tokenize(sql);
   ```

2. Use demo project for interactive testing
3. Add breakpoints in `Parser.ParseStatement()` or `Parser.ParseExpr()`
4. Check dialect-specific overrides

### Adding Keyword Support

1. Add to `Keywords.cs` enum
2. Update `Keyword.TryParse()` if needed
3. Use `parser.ParseKeyword(Keyword.NEW_KEYWORD)` in parsing logic

### Handling Whitespace and Comments

- Tokenizer handles whitespace/comments automatically
- Use `parser.PrevToken` to access comments if needed
- Preserve formatting with `ParserOptions.PreserveFormatting`

## What NOT to Do

❌ **Don't add semantic validation**
- Example: Don't reject `CREATE TABLE(x int, x int)` (duplicate columns)
- Reason: Semantic rules vary by dialect and implementation

❌ **Don't break from Rust structure without good reason**
- Changes should align with sqlparser-rs when possible
- Document divergences clearly

❌ **Don't use mutable state in AST nodes**
- Records should be immutable
- Use `with` expressions for modifications

❌ **Don't add dependencies without discussion**
- Keep dependencies minimal
- Prefer BCL types where possible

❌ **Don't commit without tests**
- Every parse feature needs corresponding tests
- Verify with multiple dialects where applicable

## Build and Development

### Required Tools
- .NET 8+ SDK
- Visual Studio 2022+ or Rider
- Optional: BenchmarkDotNet for performance testing

### Build Commands
```bash
# Build entire solution
dotnet build

# Run tests
dotnet test

# Run specific test class
dotnet test --filter "FullyQualifiedName~PostgresDialectTests"

# Run benchmarks
dotnet run -c Release --project src/SqlParser.Benchmarks
```

### CI/CD
- GitHub Actions workflow in `.github/workflows/cd.yml`
- Runs on all PRs and main branch commits
- Includes build, test, and NuGet package creation

## Contributing Back to Rust

When implementing features that should exist in both projects:

1. Implement in C# first OR port from Rust
2. Create PR for SqlParser-CS
3. If new feature, create corresponding Rust PR
4. Link the PRs in descriptions
5. Coordinate reviews and merges

## Resources

- **Original Rust Project**: https://github.com/sqlparser-rs/sqlparser-rs
- **SQL:2016 Grammar**: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html
- **Pratt Parsing**: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing

## Questions?

When in doubt:
1. Check the Rust implementation
2. Look for similar existing patterns in the codebase
3. Refer to existing tests for examples
4. Keep changes minimal and focused
5. Maintain the syntax-only parsing philosophy
