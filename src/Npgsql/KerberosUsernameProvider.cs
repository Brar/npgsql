﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Npgsql;

/// <summary>
/// Launches MIT Kerberos klist and parses out the default principal from it.
/// Caches the result.
/// </summary>
class KerberosUsernameProvider
{
    static bool _performedDetection;
    static string? _principalWithRealm;
    static string? _principalWithoutRealm;

    internal static string? GetUsername(bool includeRealm, ILogger connectionLogger)
        => _performedDetection
            ? includeRealm ? _principalWithRealm : _principalWithoutRealm
#if NET5_0_OR_GREATER
            : DetectUsername(includeRealm, connectionLogger, false).GetAwaiter().GetResult();
#else
            : DetectUsername(includeRealm, connectionLogger);
#endif

#if NET5_0_OR_GREATER
    internal static ValueTask<string?> GetUsernameAsync(bool includeRealm, ILogger connectionLogger)
        => _performedDetection
            ? ValueTask.FromResult(includeRealm ? _principalWithRealm : _principalWithoutRealm)
            : DetectUsername(includeRealm, connectionLogger, true);

    static async ValueTask<string?> DetectUsername(bool includeRealm, ILogger connectionLogger, bool async)
#else
    static string? DetectUsername(bool includeRealm, ILogger connectionLogger)
#endif
    {
        var klistPath = FindInPath("klist");
        if (klistPath == null)
        {
            connectionLogger.LogDebug("klist not found in PATH, skipping Kerberos username detection");
            return null;
        }

        var processStartInfo = new ProcessStartInfo
        {
            FileName = klistPath,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        var process = Process.Start(processStartInfo);
        if (process is null)
        {
            connectionLogger.LogDebug("klist process could not be started");
            return null;
        }

#if NET5_0_OR_GREATER
        if (async)
            await process.WaitForExitAsync();
        else
#else
            // ReSharper disable once MethodHasAsyncOverload
            process.WaitForExit();
#endif

        if (process.ExitCode != 0)
        {
            connectionLogger.LogDebug($"klist exited with code {process.ExitCode}: {process.StandardError.ReadToEnd()}");
            return null;
        }

        var line = default(string);
        for (var i = 0; i < 2; i++)
            // ReSharper disable once MethodHasAsyncOverload
#if NET5_0_OR_GREATER
            if ((line = async ? await process.StandardOutput.ReadLineAsync() : process.StandardOutput.ReadLine()) == null)
#else
            if ((line = process.StandardOutput.ReadLine()) == null)
#endif
            {
                connectionLogger.LogDebug("Unexpected output from klist, aborting Kerberos username detection");
                return null;
            }

        return ParseKListOutput(line!, includeRealm, connectionLogger);
    }

    static string? ParseKListOutput(string line, bool includeRealm, ILogger connectionLogger)
    {
        var colonIndex = line.IndexOf(':');
        var colonLastIndex = line.LastIndexOf(':');
        if (colonIndex == -1 || colonIndex != colonLastIndex)
        {
            connectionLogger.LogDebug("Unexpected output from klist, aborting Kerberos username detection");
            return null;
        }
        var secondPart = line.AsSpan(1 + line.IndexOf(':'));

        var principalWithRealm = secondPart.Trim();
        var atIndex = principalWithRealm.IndexOf('@');
        var atLastIndex = principalWithRealm.LastIndexOf('@');
        if (atIndex == -1 || atIndex != atLastIndex)
        {
            connectionLogger.LogDebug(
                $"Badly-formed default principal {principalWithRealm.ToString()} from klist, aborting Kerberos username detection");
            return null;
        }

        _principalWithRealm = principalWithRealm.ToString();
        _principalWithoutRealm = principalWithRealm.Slice(9, atIndex).ToString();
        _performedDetection = true;
        return includeRealm ? _principalWithRealm : _principalWithoutRealm;
    }

    static string? FindInPath(string name) => Environment.GetEnvironmentVariable("PATH")
        ?.Split(Path.PathSeparator)
        .Select(p => Path.Combine(p, name))
        .FirstOrDefault(File.Exists);
}
