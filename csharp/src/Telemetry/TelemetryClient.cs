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
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Default implementation that batches events from multiple connections
    /// and exports via HTTP on a timer or when batch size is reached.
    /// </summary>
    /// <remarks>
    /// <para>
    /// TelemetryClient uses a <see cref="ConcurrentQueue{T}"/> for thread-safe event batching,
    /// a <see cref="Timer"/> for periodic flushing (default 5s), and a <see cref="SemaphoreSlim"/>
    /// to prevent concurrent flushes. One instance is shared per host.
    /// </para>
    /// <para>
    /// All exceptions are swallowed internally per the telemetry design principle
    /// that telemetry operations should never impact driver operations.
    /// </para>
    /// <para>
    /// JDBC Reference: TelemetryClient.java:15
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClient : ITelemetryClient
    {
        private readonly ConcurrentQueue<TelemetryFrontendLog> _queue = new ConcurrentQueue<TelemetryFrontendLog>();
        private readonly ITelemetryExporter _exporter;
        private readonly TelemetryConfiguration _config;
        private readonly Timer _flushTimer;
        private readonly SemaphoreSlim _flushLock = new SemaphoreSlim(1, 1);
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private volatile bool _disposed;

        /// <summary>
        /// Creates a new <see cref="TelemetryClient"/>.
        /// </summary>
        /// <param name="exporter">The telemetry exporter used to send batches.</param>
        /// <param name="config">The telemetry configuration with batch size and flush interval.</param>
        /// <exception cref="ArgumentNullException">Thrown when exporter or config is null.</exception>
        public TelemetryClient(
            ITelemetryExporter exporter,
            TelemetryConfiguration config)
        {
            _exporter = exporter ?? throw new ArgumentNullException(nameof(exporter));
            _config = config ?? throw new ArgumentNullException(nameof(config));

            // Start periodic flush timer (default: every 5 seconds)
            _flushTimer = new Timer(
                OnFlushTimer,
                null,
                _config.FlushIntervalMs,
                _config.FlushIntervalMs);
        }

        /// <summary>
        /// Queue event for batched export. Thread-safe, non-blocking.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue.</param>
        public void Enqueue(TelemetryFrontendLog log)
        {
            if (_disposed) return;

            _queue.Enqueue(log);

            // Trigger flush if batch size reached
            if (_queue.Count >= _config.BatchSize)
            {
                // Fire-and-forget, errors swallowed inside FlushAsync
                _ = FlushAsync();
            }
        }

        /// <summary>
        /// Flush all pending events to the exporter.
        /// Uses SemaphoreSlim(1,1) to prevent concurrent flushes.
        /// Drains queue up to batch size per flush.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous flush operation.</returns>
        public async Task FlushAsync(CancellationToken ct = default)
        {
            if (_disposed) return;

            // Prevent concurrent flushes - if another flush is in progress, skip
            if (!await _flushLock.WaitAsync(0, ct).ConfigureAwait(false))
                return;

            try
            {
                var batch = new List<TelemetryFrontendLog>();

                // Drain queue up to batch size
                while (batch.Count < _config.BatchSize && _queue.TryDequeue(out var log))
                {
                    batch.Add(log);
                }

                if (batch.Count > 0)
                {
                    // Export via circuit breaker → exporter → HTTP
                    await _exporter.ExportAsync(batch, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                // Swallow all exceptions per telemetry requirement
                Debug.WriteLine($"[TRACE] TelemetryClient flush error: {ex.Message}");
            }
            finally
            {
                _flushLock.Release();
            }
        }

        /// <summary>
        /// Gracefully close: stop timer, cancel pending operations, flush remaining events.
        /// </summary>
        /// <returns>A task representing the asynchronous close operation.</returns>
        public async Task CloseAsync()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                // Stop timer
                _flushTimer.Dispose();

                // Cancel any pending operations
                _cts.Cancel();

                // Final flush of remaining events (don't pass cancelled token)
                await FinalFlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient close error: {ex.Message}");
            }
            finally
            {
                _cts.Dispose();
                _flushLock.Dispose();
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

        /// <summary>
        /// Timer callback that triggers periodic flush.
        /// </summary>
        private void OnFlushTimer(object? state)
        {
            if (_disposed) return;
            _ = FlushAsync(_cts.Token);
        }

        /// <summary>
        /// Performs a final flush during close, bypassing the disposed check.
        /// Uses a fresh CancellationToken since _cts has been cancelled.
        /// </summary>
        private async Task FinalFlushAsync()
        {
            // Prevent concurrent flushes
            if (!await _flushLock.WaitAsync(0).ConfigureAwait(false))
                return;

            try
            {
                var batch = new List<TelemetryFrontendLog>();

                // Drain all remaining events from the queue
                while (_queue.TryDequeue(out var log))
                {
                    batch.Add(log);
                }

                if (batch.Count > 0)
                {
                    await _exporter.ExportAsync(batch, CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] TelemetryClient final flush error: {ex.Message}");
            }
            finally
            {
                _flushLock.Release();
            }
        }
    }
}
