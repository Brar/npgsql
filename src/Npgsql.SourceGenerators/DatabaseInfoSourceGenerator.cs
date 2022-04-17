using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using Scriban;
using Scriban.Runtime;

namespace Npgsql.SourceGenerators;

[Generator]
sealed class DatabaseInfoSourceGenerator : ISourceGenerator
{
    // Warning: Do not change to target typing
    // See: https://github.com/dotnet/roslyn-analyzers/issues/5890#issuecomment-1046043775
    // ReSharper disable once ArrangeObjectCreationWhenTypeEvident
    static readonly DiagnosticDescriptor DatFileParserError = new DiagnosticDescriptor(
        id: "PG0001",
        title: "Failed to parse .dat file",
        messageFormat: "Syntax error in .dat file",
        category: "Internal",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public void Initialize(GeneratorInitializationContext context) { }

    public void Execute(GeneratorExecutionContext context)
    {

        List<Dictionary<string,string>>? pgType = null;
        List<Dictionary<string,string>>? pgRange = null;
        var noDatFile = true;
        foreach (var file in context.AdditionalFiles)
        {
            if (Path.GetExtension(file.Path) != ".dat")
                continue;

            noDatFile = false;
            var fileName = Path.GetFileName(file.Path);
            try
            {
                switch (fileName)
                {
                case "pg_type.dat":
                    pgType = ParseDatFile(file.GetText());
                    break;
                case "pg_range.dat":
                    pgRange = ParseDatFile(file.GetText());
                    break;
                }
            }
            catch (DatFileFormatException e)
            {
                context.ReportDiagnostic(Diagnostic.Create(DatFileParserError,
                    Location.Create(file.Path, new(e.Position, 1),
                        new(new(e.LineNumber - 1, e.LinePosition - 1), new(e.LineNumber - 1, e.LinePosition)))));

                // We have detected and reported a syntax error in a .dat file so we just return
                return;
            }
        }

        // There is no .dat file in the additional files for this project.
        // This probably means that the project just isn't interested in this source generator
        if (noDatFile)
            return;

        if (pgType == null)
            throw new("This source generator needs at least a valid pg_type.dat file in AdditionalFiles to perform it's work");

        var compilation = context.Compilation;
        var postgresMinimalDatabaseInfoSymbol = compilation.GetTypeByMetadataName("Npgsql.PostgresMinimalDatabaseInfo");
        if (postgresMinimalDatabaseInfoSymbol is null)
            throw new("Could not find PostgresMinimalDatabaseInfo");

        var template = Template.Parse(EmbeddedResource.GetContent("PostgresMinimalDatabaseInfo.snbtxt"), "PostgresMinimalDatabaseInfo.snbtxt");

        var rangeTypes = new List<object>();
        var multiRangeTypes = new List<object>();
        if (pgRange != null)
        {
            foreach (var range in pgRange)
            {
                var rangeSubTypeName = range["rngsubtype"];
                var rangeTypeName = range["rngtypid"];
                var rangeMultiTypeName = range["rngmultitypid"];
                var rangeTypeOid = pgType.Where(t => t["typname"] == rangeTypeName).Select(t => t["oid"]).First();
                var rangeMultiTypeOid = pgType.Where(t => t["typname"] == rangeMultiTypeName).Select(t => t["oid"]).First();
                rangeTypes.Add(new
                {
                    Name = rangeTypeName,
                    Oid = rangeTypeOid,
                    ElementName = rangeSubTypeName,
                });
                multiRangeTypes.Add(new
                {
                    Name = rangeMultiTypeName,
                    Oid = rangeMultiTypeOid,
                    ElementName = rangeTypeName,
                });
            }
        }

        // There is information about 4 composite types in pg_type.dat (pg_type, pg_attribute, pg_proc, pg_class)
        // We don't bother adding them for now since there is no pg_attribute.dat and we'd have to parse the
        // C header file pg_attribute.h to acquire the field information we need to make them work
        var obj = new ScriptObject();
        obj.Import(new
        {
            UnreferencedBaseTypes = pgType.Where(d =>
                !d.ContainsKey("typtype") && !d.ContainsKey("array_type_oid") &&
                (pgRange == null || pgRange.All(r => r["rngsubtype"] != d["typname"]))).Select(t => new
            {
                Name = t["typname"],
                Oid = t["oid"]
            }).ToArray(),
            ReferencedBaseTypes = pgType.Where(d =>
                !d.ContainsKey("typtype") &&
                (d.ContainsKey("array_type_oid") || (pgRange?.Any(r => r["rngsubtype"] == d["typname"]) ?? false))).Select(t => new
            {
                Name = t["typname"],
                Oid = t["oid"]
            }).ToArray(),
            UnreferencedPseudoTypes = pgType.Where(d =>
                d.TryGetValue("typtype", out var value) && value == "p" && !d["typname"].StartsWith("_") &&
                !d.ContainsKey("array_type_oid") && (pgRange == null || pgRange.All(r => r["rngsubtype"] != d["typname"]))
                && pgType.All(p => p.TryGetValue("typtype", out var v) && v == "p" && p["typname"] != "_" + d["typname"])
                ).Select(t => new
            {
                Name = t["typname"],
                Oid = t["oid"]
            }).ToArray(),
            ReferencedPseudoTypes = pgType.Where(d =>
                d.TryGetValue("typtype", out var value) && value == "p" && !d["typname"].StartsWith("_") &&
                (d.ContainsKey("array_type_oid") || (pgRange?.Any(r => r["rngsubtype"] == d["typname"]) ?? false)
                                                 || pgType.Any(p => p.TryGetValue("typtype", out var v) && v == "p" && p["typname"] == "_" + d["typname"])
                    )).Select(t => new
            {
                Name = t["typname"],
                Oid = t["oid"]
            }).ToArray(),
            RangeTypes = rangeTypes,
            MultirangeTypes = multiRangeTypes,
            ArrayTypes = pgType
                .Where(d => (d.ContainsKey("array_type_oid") || d["typname"].StartsWith("_"))
                            && (!d.TryGetValue("typtype", out var value) ||
                                value != "c") // Exclude arrays of composite types. See comment above
                            && (pgRange != null ||
                                value != "r" && value != "m") // Exclude arrays of range types if pg_range.dat was not included
                )
                .Select(t => t["typname"].StartsWith("_")
                    ? new
                    {
                        Name = t["typname"],
                        Oid = t["oid"],
                        ElementName = t["typname"].Substring(1),
                    }
                    : new
                    {
                        Name = "_" + t["typname"],
                        Oid = t["array_type_oid"],
                        ElementName = t["typname"],
                    }).ToArray(),
        });
        var tc = new TemplateContext(obj);
        tc.AutoIndent = false;
        var output = template.Render(tc);
        context.AddSource(postgresMinimalDatabaseInfoSymbol.Name + ".Generated.cs", SourceText.From(output, Encoding.UTF8));
    }

    static List<Dictionary<string,string>>? ParseDatFile(SourceText? content)
    {
        if (content == null)
            return null;

        var list = new List<Dictionary<string,string>>();
        var dict = new Dictionary<string, string>();
        var key = new StringBuilder();
        var value = new StringBuilder();
        var lineNo = 1;
        var posInLine = 1;
        var state = ParseState.Initial;
        var outerState = ParseState.Initial;
        for (var i = 0; i < content.Length; i++)
        {
            var c = content[i];
            switch (state)
            {
            case ParseState.Initial:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.Initial;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.Initial;
                    state = ParseState.Comment;
                    break;
                case '[':
                    state = ParseState.InArray;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.InArray:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.InArray;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.InArray;
                    state = ParseState.Comment;
                    break;
                case '{':
                    state = ParseState.InHash;
                    break;
                case ']':
                    state = ParseState.AfterArray;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.InHash:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.InHash;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.InHash;
                    state = ParseState.Comment;
                    break;
                case '}':
                    list.Add(new(dict));
                    dict = new();
                    state = ParseState.AfterHash;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    if (IsValidKeyChar(c))
                    {
                        key.Clear();
                        key.Append(c);
                        state = ParseState.Key;
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.Key:
            {
                switch (c)
                {
                    case '\r':
                        lineNo++;
                        posInLine = 0;
                        outerState = ParseState.AfterKey;
                        state = ParseState.Cr;
                        break;
                    case '\n':
                        lineNo++;
                        posInLine = 0;
                        state = ParseState.AfterKey;
                        break;
                    case '#':
                        outerState = ParseState.AfterKey;
                        state = ParseState.Comment;
                        break;
                    case '=':
                    {
                        state = ParseState.FatComma;
                        break;
                    }
                    default:
                    {
                        if (char.IsWhiteSpace(c))
                        {
                            state = ParseState.AfterKey;
                            break;
                        }

                        if (IsValidKeyChar(c))
                        {
                            key.Append(c);
                            break;
                        }

                        throw new DatFileFormatException(i, lineNo, posInLine);
                    }
                }
                break;
            }
            case ParseState.AfterKey:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.AfterKey;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.AfterKey;
                    state = ParseState.Comment;
                    break;
                case '=':
                {
                    state = ParseState.FatComma;
                    break;
                }
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.FatComma:
            {
                if (c == '>')
                {
                    state = ParseState.AfterFatComma;
                    break;
                }

                throw new DatFileFormatException(i, lineNo, posInLine);

            }
            case ParseState.AfterFatComma:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.AfterFatComma;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.AfterFatComma;
                    state = ParseState.Comment;
                    break;
                case '\'':
                    state = ParseState.Value;
                    value.Clear();
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.Value:
            {
                switch (c)
                {
                    case '\\':
                        state = ParseState.ValueEscape;
                        break;
                    case '\'':
                        state = ParseState.AfterValue;
                        dict.Add(key.ToString(), value.ToString());
                        break;
                    default:
                        value.Append(c);
                        break;
                }
                break;
            }
            case ParseState.ValueEscape:
            {
                value.Append(c);
                state = ParseState.Value;
                break;
            }
            case ParseState.AfterValue:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.AfterValue;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.AfterValue;
                    state = ParseState.Comment;
                    break;
                case ',':
                    state = ParseState.InHash;
                    break;
                case '}':
                    list.Add(new(dict));
                    dict = new();
                    state = ParseState.AfterHash;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.AfterHash:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.AfterHash;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.AfterHash;
                    state = ParseState.Comment;
                    break;
                case ',':
                    state = ParseState.InArray;
                    break;
                case ']':
                    state = ParseState.AfterArray;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.AfterArray:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    outerState = ParseState.AfterArray;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    break;
                case '#':
                    outerState = ParseState.AfterArray;
                    state = ParseState.Comment;
                    break;
                default:
                {
                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    throw new DatFileFormatException(i, lineNo, posInLine);
                }
                }
                break;
            }
            case ParseState.Comment:
            {
                switch (c)
                {
                case '\r':
                    lineNo++;
                    posInLine = 0;
                    state = ParseState.Cr;
                    break;
                case '\n':
                    lineNo++;
                    posInLine = 0;
                    state = outerState;
                    break;
                }
                break;
            }
            case ParseState.Cr:
            {
                if (c == '\n')
                {
                    state = outerState;
                    break;
                }

                // undo reading this character
                // don't increment line position
                i--;
                continue;
            }
            // ReSharper disable once UnreachableSwitchCaseDueToIntegerAnalysis
            default:
            {
                throw new ArgumentOutOfRangeException();
            }

            static bool IsValidKeyChar(char c)
                => c == '_' || char.IsLetter(c);
            }

            posInLine++;
        }
            
        return list;
    }

    enum ParseState
    {
        Initial,
        Comment,
        Cr,
        InArray,
        InHash,
        Key,
        AfterKey,
        FatComma,
        AfterFatComma,
        Value,
        ValueEscape,
        AfterValue,
        AfterHash,
        AfterArray,
    }

    public sealed class DatFileFormatException : FormatException
    {
        public int Position { get; }
        public int LineNumber { get; }
        public int LinePosition { get; }

        public DatFileFormatException(int position, int lineNumber, int linePosition) : base(
            $"Syntax error in dat file at line {lineNumber}, position {linePosition}")
        {
            Position = position;
            LineNumber = lineNumber;
            LinePosition = linePosition;
        }
    }
}
