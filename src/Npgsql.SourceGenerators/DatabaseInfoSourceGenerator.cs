﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using Scriban;
using Scriban.Runtime;

namespace Npgsql.SourceGenerators;

[Generator(LanguageNames.CSharp)]
sealed class DatabaseInfoSourceGenerator : IIncrementalGenerator
{
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var postgresMinimalDatabaseInfoSymbolName = context.CompilationProvider
            .Select(static (c, _) => c.GetTypeByMetadataName("Npgsql.PostgresMinimalDatabaseInfo")?.Name
                                     ?? throw new("Could not find PostgresMinimalDatabaseInfo"));

        var sourceCode = context.AdditionalTextsProvider
            .Where(static file => Path.GetFileName(file.Path) == "catalogs.json")
            .Select(CreateSourceCodeFromJson);

        var combined = sourceCode.Combine(postgresMinimalDatabaseInfoSymbolName);

        context.RegisterSourceOutput(combined, (spc, pair) =>
        {
            spc.AddSource(pair.Right + ".Generated.cs",  SourceText.From(pair.Left, Encoding.UTF8));
        });
    }

    static string CreateSourceCodeFromJson(AdditionalText file, CancellationToken cancellationToken)
    {
        var catalogs = JsonSerializer.Deserialize<CatalogInfo>(
                           file.GetText(cancellationToken)?.ToString()
                           ?? throw new Exception($"An error occurred when reading the additional build file '{file.Path}'"))
                       ?? throw new("Failed to deserialize json.");

        var template = Template.Parse(EmbeddedResource.GetContent("PostgresMinimalDatabaseInfo.snbtxt"), "PostgresMinimalDatabaseInfo.snbtxt");

        var rangeTypes = new List<object>();
        var multiRangeTypes = new List<object>();
        foreach (var range in catalogs.pg_range)
        {
            var rangeSubTypeName = range.rngsubtype;
            var rangeTypeName = range.rngtypid;
            var rangeMultiTypeName = range.rngmultitypid;
            var rangeTypeOid = catalogs.pg_type.Where(t => t.typname == rangeTypeName).Select(t => t.oid).First();
            var rangeMultiTypeOid = catalogs.pg_type.Where(t => t.typname == rangeMultiTypeName).Select(t => t.oid).First();
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

        // There currently is information about 4 composite types from pg_type.dat in catalogs.json:
        // pg_type, pg_attribute, pg_proc, pg_class.
        // We don't bother adding them for now since there is no pg_attribute.dat and we'd have to parse the
        // C header file pg_attribute.h to acquire the field information we need to make them work
        var obj = new ScriptObject();
        obj.Import(new
        {
            UnreferencedBaseTypes = catalogs.pg_type.Where(d =>
                d.typtype == null && d.array_type_oid == null &&
                catalogs.pg_range.All(r => r.rngsubtype != d.typname)).Select(t => new
            {
                Name = t.typname,
                Oid = t.oid
            }).ToArray(),
            ReferencedBaseTypes = catalogs.pg_type.Where(d =>
                d.typtype == null &&
                (d.array_type_oid != null || catalogs.pg_range.Any(r => r.rngsubtype == d.typname))).Select(t => new
            {
                Name = t.typname,
                Oid = t.oid
            }).ToArray(),
            UnreferencedPseudoTypes = catalogs.pg_type.Where(d =>
                d.typtype is "p" && !d.typname.StartsWith("_") &&
                d.array_type_oid == null &&
                catalogs.pg_range.All(r => r.rngsubtype != d.typname) &&
                catalogs.pg_type.All(p => p.typtype is "p" && p.typname != "_" + d.typname)
                ).Select(t => new
            {
                Name = t.typname,
                Oid = t.oid
            }).ToArray(),
            ReferencedPseudoTypes = catalogs.pg_type.Where(d =>
                d.typtype is "p" && !d.typname.StartsWith("_") && (
                    d.array_type_oid != null ||
                    catalogs.pg_range.Any(r => r.rngsubtype == d.typname) ||
                    catalogs.pg_type.Any(p => p.typtype is "p" && p.typname == "_" + d.typname)
                ))
                .Select(t => new
                {
                    Name = t.typname,
                    Oid = t.oid
                }).ToArray(),
            RangeTypes = rangeTypes,
            MultirangeTypes = multiRangeTypes,
            ArrayTypes = catalogs.pg_type
                .Where(d => (d.array_type_oid!= null || d.typname.StartsWith("_"))
                            && (d.typtype == null ||
                                d.typtype != "c") // Exclude arrays of composite types. See comment above
                )
                .Select(t => t.typname.StartsWith("_")
                    ? new
                    {
                        Name = t.typname,
                        Oid = t.oid,
                        ElementName = t.typname.Substring(1),
                    }
                    : new
                    {
                        Name = "_" + t.typname,
                        Oid = t.array_type_oid!,
                        ElementName = t.typname,
                    }).ToArray(),
            Version = catalogs.version,
        });
        var tc = new TemplateContext(obj);
        tc.AutoIndent = false;
        var output = template.Render(tc);
        return output!;
    }

    // ReSharper disable InconsistentNaming
    // ReSharper disable IdentifierTypo
    // ReSharper disable MemberHidesStaticFromOuterClass
    class pg_type
    {
        public string oid { get; set; } = null!;
        public string typname { get; set; } = null!;
        public string? array_type_oid { get; set; }
        public string? typtype { get; set; }
    }

    class pg_range
    {
        public string rngtypid { get; set; } = null!;
        public string rngsubtype { get; set; } = null!;
        public string rngmultitypid { get; set; } = null!;
    }

    class CatalogInfo
    {
        public int version { get; set; }
        public List<pg_type> pg_type { get; set; } = null!;
        public List<pg_range> pg_range { get; set; } = null!;
    }
}
