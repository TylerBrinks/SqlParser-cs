# Extensible SQL Lexer and Parser for .NET

[![NuGet Status](https://img.shields.io/nuget/v/sqlparsercs.svg)](https://www.nuget.org/packages/sqlparsercs/)  [![CD](https://github.com/TylerBrinks/SqlParser-cs/actions/workflows/cd.yml/badge.svg)](https://github.com/TylerBrinks/SqlParser-cs/actions/workflows/cd.yml) 

This project is .NET port from the original Rust [sqlparser-rs project](https://github.com/sqlparser-rs/sqlparser-rs).

It's a .NET library capable of parsing SQL statements into an abstract syntax tree (AST).  It contains a lexer and parser for SQL that conforms with the
[ANSI/ISO SQL standard][sql-standard] and other dialects.  


## Getting Started

Installation
Install [SqlParser](https://www.nuget.org/packages/sqlparsercs/) from the 
.NET CLI as:
```
dotnet add package SqlParserCS
```
Or from the package manager console:
```
PM> Install-Package SqlParserCS
```

## Parsing SQL Queries

Parse a simple `SELECT` statement:

```cs
using SqlParser;

var sql = """
		select top 10 *
        from my_table
        where abc > 99
        order by xyz desc
		""";

var ast =  new Parser().ParseSql(sql);
```
SqlParser reads the output into a token stream and parses it into a SQL syntax tree. 

## Demo Project
Need more than just a code snippet?  The solution includes a demo console app where you can test out parsing SQL as well as look at few 
simple examples of how to use the AST to query an arbitrary data source like a CSV file.  Download or clone the project, set the demo
project as startup, and run it.  The console app will prompt you through the examples.

## AST Object Model
Once the syntax tree has been parsed, you can query and traverse the object model.  The example below is (highly) manual way of parsing the output. 
The library contains numerous convenience methods to help traverse the tree gracefully.

```cs
// Get the query statement since parsed results can have multiple
// statements (separated by a semicolon).
var select = statements.First() as Statement.Select;
// Set a variable to the select statement's body
var body = select.Query.Body as SetExpression.SelectExpression;
// Find the table contained within the select
var table = body.Select.From.First().Relation as TableFactor.Table;
// Write out the table's name
Console.WriteLine(table.Name); // <-- writes "my_table"
```

### C# Records
While C# and Rust programming languages have high feature parity, there are some things that (at present), C# cannot duplicate. In particular, Rust uses discriminated unions.  Simply put, Rust allows enums to carry class-like (or record-like) implementations.  [C# can come close](https://github.com/dotnet/csharplang/blob/main/proposals/discriminated-unions.md) with creative use of record types, particularly due to their ByValue equality.  However they still fall short of the Rust counterpart.  

In this project, every effort has been made to duplicate the object hierarchy by using a combination of abstract base records with enclosed sub-class implementations.  While somewhat unconventional, it allows each major syntax tree type to have a pseudo namespace within the containing record type.

For example, a `Statement` is a high level type that describes `SELECT`, `INSERT`, `DELETE` statements, etc.  Each of those specific implementations is a child of the `Statement` abstract base record.

```cs
var statement = new Statement.Select(...)
```
<br/>

## Text Output
The library supports multiple SQL dialects (Postgres, MsSql, etc.) as well as multiple output modes.  The code and can be extended to display the syntax tree
in custom formats.

This example calls `ToString()` and redners the AST's default format showing the  object hierarchy

```cs
var sql = "select top 10 * from mytable where abc > 99 order by xyz desc";
var ast =  new Parser().ParseSql(sql);

Console.Write!($"{ast}");
// Elided and multi-lines for readability
Select { 
  Query = Query { 
    Body = SelectExpression { 
      Select = Select { 
        Projection = [ Wildcard { }],
          Distinct = False, 
          ...
        }, 
        From = [
          TableWithJoins { 
            Relation = Table { 
              Name = mytable
            }
          }
        ]  
    ...
  }
}
```

The same query can be output the built-in `ToSql()` method which produces a SQL string representing the original query after going through a process of normalizing the text (e.g. upper case keywords).

```cs
Console.WriteLine($"SQL: {ast.ToSql()}");

SQL: SELECT TOP 10 * FROM mytable WHERE abc > 10 ORDER BY xyz DESC
```

The syntax tree can also be formatted as JSON, for example.  This example uses Newtonsoft to render the AST hierarchy.

```cs
JsonConvert.SerializeObject(statements.First(), Formatting.Indented)
// Elided for readability
{
   "Query": {
      "Body": {
         "Select": {
            "Projection": [
               {
                  "Expression": {
                     "Ident": {
                        "Value": "a",
                        "QuoteStyle": null
                     }
                  }
               }
	...
```
<br/>

## Features
### SQL Dialects
This library supports the following SQL dialects.  You can also create your own dialect with support for parsing 
SQL at every level of the syntax tree hierarchy.  

-  Generic SQL dialect
-  ANSI SQL dialect
-  BigQuery SQL dialect
-  ClickHouse SQL dialect
-  Hive SQL dialect
-  MS SQL dialect
-  My SQL dialect
-  PostgreSQL dialect
-  Redshift SQL dialect
-  SQLite dialect

### Supporting custom SQL dialects
The library supports a number of inbuilt dialects, but it is not comprehensive.  As such, you can create your own dialect with hooks into the parser that allow you to customize the AST as it's being constructed.

Please see the included examples for reference.

### Visitors
The library also contains a base class visitor implementation that allows you to walk the entire syntax tree.  The visitor only traverses non-null elements in the tree and resports all Statements, Expressions, and Relations before and after each element is visited.  The base class is a jumping-off point for traversing the AST, and is only one of several ways the AST can be navigated.

```cs
var visitor = new YourCustomVisitor();
ast.Visit(visitor);
```

## Syntax vs Semantics

Like the Rust counterpart, this library provides only a syntax parser, and tries to avoid applying any SQL semantics, and accepts queries that specific databases would
reject, even when using that Database's specific `Dialect`. 

For example, `CREATE TABLE(x int, x int)` is accepted by this crate, even
though most SQL engines will reject this statement due to the repeated
column name `x`.

This library avoids semantic analysis because it varies drastically
between dialects and implementations. If you want to do semantic
analysis, feel free to use this project as a base.

## Design

The core expression parser uses the [Pratt Parser] design, which is a top-down
operator-precedence (TDOP) parser, while the surrounding SQL statement parser is
a traditional, hand-written recursive descent parser. Eli Bendersky has a good
[tutorial on TDOP parsers][tdop-tutorial], if you are interested in learning
more about the technique.

Like the Rust project author's,  we're are a fan of this design pattern over parser generators for the following due to simplicity, performancee, ease of debugging, and extensibility.

If you like Lexing & Parsing, I have a similar example of a hand-written [CSS lexer/parser](https://github.com/TylerBrinks/ExCSS).



## Contributions are welcome!

Contributions are highly encouraged! Please keep in mind that this project
is a port of the Rust SQL parser, and every effort will be made to keep 
the projects functionally in sync.  Therefore pull requests will 
generally follow the spirit of the oriignal project.  Ideally, pull requests that
affect the parser or a dialect specifically will be submitted back to the original
Rust project as well.

Pull requests that add support for or fix a bug in a feature in the
SQL standard, or a feature in a popular RDBMS, like Microsoft SQL
Server or PostgreSQL, will likely be accepted after a brief
review.

PRs without tests will not be reviewed or merged. Please run all tests prior to any pull request

If you are unable to submit a patch, feel free to file an issue instead. Please
try to include:

  * some representative examples of the syntax you wish to support or fix;
  * the relevant bits of the [SQL grammar][sql-2016-grammar], if the syntax is
    part of SQL:2016; and
  * links to documentation for the feature for a few of the most popular
    databases that support it.

If you need support for a feature, you will likely need to implement
it yourself. Our goal as maintainers is to facilitate the integration
of various features from various contributors, but not to provide the
implementations ourselves, as we simply don't have the resources.


## SQL compliance

This project adops the same compliance position as the Rust project:

SQL was first standardized in 1987, and revisions of the standard have been
published regularly since. Most revisions have added significant new features to
the language, and as a result no database claims to support the full breadth of
features. This parser currently supports most of the SQL-92 syntax, plus some
syntax from newer versions that have been explicitly requested, plus some MSSQL,
PostgreSQL, and other dialect-specific syntax. Whenever possible, the [online
SQL:2016 grammar][sql-2016-grammar] is used to guide what syntax to accept.

Unfortunately, stating anything more specific about compliance is difficult.
There is no publicly available test suite that can assess compliance
automatically, and doing so manually would strain the project's limited
resources. Still, we are interested in eventually supporting the full SQL
dialect, and we are slowly building out our own test suite.

If you are assessing whether this project will be suitable for your needs,
you'll likely need to experimentally verify whether it supports the subset of
SQL that you need. Please file issues about any unsupported queries that you
discover. Doing so helps us prioritize support for the portions of the standard
that are actually used. Note that if you urgently need support for a feature,
you will likely need to write the implementation yourself. See the
[Contributing](#Contributing) section for details.

### License, etc.
SqlParser is based on the work originally developed by [Andy Grove](https://github.com/andygrove) and other contributors 

SqlParser is Copyright &copy; 2023 Tyler Brinks and other contributors under the [MIT license](LICENSE.txt).


[tdop-tutorial]: https://eli.thegreenplace.net/2010/01/02/top-down-operator-precedence-parsing
[sql-2016-grammar]: https://jakewheat.github.io/sql-overview/sql-2016-foundation-grammar.html
[sql-standard]: https://en.wikipedia.org/wiki/ISO/IEC_9075