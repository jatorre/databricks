/*
* Copyright (c) 2025 ADBC Drivers Contributors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton factory that manages one telemetry client per host.
    /// Prevents rate limiting by sharing clients across connections to the same host.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each host gets its own <see cref="ITelemetryClient"/> instance, shared across all
    /// connections to that host. Reference counting tracks active connections: when the
    /// last connection to a host closes, the client is closed and removed.
    /// </para>
    /// <para>
    /// Thread-safe for concurrent access from multiple connections.
    /// Uses <see cref="ConcurrentDictionary{TKey, TValue}"/> with
    /// <see cref="ConcurrentDictionary{TKey, TValue}.AddOrUpdate"/> for atomic
    /// get-or-create with reference count increment.
    /// </para>
    /// <para>
    /// JDBC Reference: TelemetryClientManager pattern from JDBC driver.
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClientManager
    {
        private static TelemetryClientManager _instance = new TelemetryClientManager();

        private readonly ConcurrentDictionary<string, TelemetryClientHolder> _clients =
            new ConcurrentDictionary<string, TelemetryClientHolder>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private TelemetryClientManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of <see cref="TelemetryClientManager"/>.
        /// </summary>
        /// <returns>The singleton instance.</returns>
        public static TelemetryClientManager GetInstance() => _instance;

        /// <summary>
        /// Gets or creates a telemetry client for the specified host.
        /// If a client already exists for the host, increments its reference count
        /// and returns the same client. Otherwise, creates a new client using the
        /// provided exporter factory and configuration.
        /// </summary>
        /// <param name="host">The host to get or create a client for.</param>
        /// <param name="exporterFactory">Factory function to create a new <see cref="ITelemetryExporter"/> if needed.</param>
        /// <param name="config">The telemetry configuration for new client creation.</param>
        /// <returns>The shared <see cref="ITelemetryClient"/> for the host.</returns>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="host"/>, <paramref name="exporterFactory"/>,
        /// or <paramref name="config"/> is null.
        /// </exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="host"/> is empty or whitespace.</exception>
        public ITelemetryClient GetOrCreateClient(
            string host,
            Func<ITelemetryExporter> exporterFactory,
            TelemetryConfiguration config)
        {
            if (host == null)
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be empty or whitespace.", nameof(host));
            }

            if (exporterFactory == null)
            {
                throw new ArgumentNullException(nameof(exporterFactory));
            }

            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var holder = _clients.AddOrUpdate(
                host,
                _ => new TelemetryClientHolder(
                    new TelemetryClient(exporterFactory(), config)),
                (_, existing) =>
                {
                    Interlocked.Increment(ref existing._refCount);
                    return existing;
                });

            return holder.Client;
        }

        /// <summary>
        /// Decrements the reference count for the specified host's client.
        /// When the reference count reaches zero, closes and removes the client.
        /// </summary>
        /// <param name="host">The host whose client reference count should be decremented.</param>
        /// <returns>A task representing the asynchronous release operation.</returns>
        /// <remarks>
        /// If the host is not found (e.g., already released or never created),
        /// this method is a no-op.
        /// </remarks>
        public async Task ReleaseClientAsync(string host)
        {
            if (string.IsNullOrWhiteSpace(host))
            {
                return;
            }

            if (_clients.TryGetValue(host, out var holder))
            {
                var newCount = Interlocked.Decrement(ref holder._refCount);
                if (newCount == 0)
                {
                    if (_clients.TryRemove(host, out var removed))
                    {
                        await removed.Client.CloseAsync().ConfigureAwait(false);
                    }
                }
            }
        }

        /// <summary>
        /// For testing only: temporarily replaces the singleton instance.
        /// Returns an <see cref="IDisposable"/> that restores the original instance when disposed.
        /// </summary>
        /// <param name="testInstance">The test instance to use as the singleton.</param>
        /// <returns>An <see cref="IDisposable"/> that restores the original instance on dispose.</returns>
        internal static IDisposable UseTestInstance(TelemetryClientManager testInstance)
        {
            var original = _instance;
            _instance = testInstance ?? throw new ArgumentNullException(nameof(testInstance));
            return new TestInstanceScope(original);
        }

        /// <summary>
        /// For testing only: resets all state by closing and removing all clients.
        /// </summary>
        internal void Reset()
        {
            foreach (var host in _clients.Keys.ToList())
            {
                if (_clients.TryRemove(host, out var holder))
                {
                    holder.Client.CloseAsync().GetAwaiter().GetResult();
                }
            }
        }

        /// <summary>
        /// Scope that restores the original <see cref="TelemetryClientManager"/> instance on dispose.
        /// </summary>
        private sealed class TestInstanceScope : IDisposable
        {
            private readonly TelemetryClientManager _original;

            public TestInstanceScope(TelemetryClientManager original)
            {
                _original = original;
            }

            public void Dispose()
            {
                _instance = _original;
            }
        }
    }
}
