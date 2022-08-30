using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Npgsql;

/// <summary>
/// Provides lookup for a pool based on a connection string.
/// </summary>
/// <remarks>
/// Note that pools created directly as <see cref="NpgsqlDataSource" /> are referenced directly by users, and aren't managed here.
/// </remarks>
static class PoolManager
{
    internal static ConcurrentDictionary<string, NpgsqlDataSource> Pools { get; } = new();
    static Dictionary<NpgsqlDataSource, Timer> ClearedPools { get; } = new();

    internal static void Clear(string connString)
    {
        NpgsqlDataSource? pool;
        while (!Pools.TryRemove(connString, out pool) && Pools.ContainsKey(connString)) { }

        if (pool == null)
            return;
        
        pool.Clear();
        var timer = new Timer(state =>
        {
            lock (ClearedPools)
            {
                var ds = (NpgsqlDataSource)state!;
#if NET5_0_OR_GREATER
                ClearedPools.Remove(ds, out var timer);
#else
                var timer = ClearedPools[ds];
                ClearedPools.Remove(ds);
#endif
                ds.Dispose();
                timer!.Dispose();
            }
        }, pool, TimeSpan.FromMinutes(1) /* ToDo: Put in some serious value here, probably reading it from the connection string */, Timeout.InfiniteTimeSpan);
        ClearedPools.Add(pool, timer);
    }

    internal static void ClearAll()
    {
        // Clear the cleared pools again since they may have been used (and by that refilled) after removing them from the pool
        lock (ClearedPools)
        {
            foreach (var clearedPool in ClearedPools.Keys)
                clearedPool.Clear();
        }

        foreach (var poolKey in Pools.Keys)
            Clear(poolKey);
    }

    static PoolManager()
    {
        // When the appdomain gets unloaded (e.g. web app redeployment) attempt to nicely
        // close idle connectors to prevent errors in PostgreSQL logs (#491).
        AppDomain.CurrentDomain.DomainUnload += (_, _) => ClearAll();
        AppDomain.CurrentDomain.ProcessExit += (_, _) => ClearAll();
    }
}