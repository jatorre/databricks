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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry.Models;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Client that receives telemetry events from MetricsAggregators and exports them.
    /// One instance is shared per host via TelemetryClientManager.
    /// </summary>
    /// <remarks>
    /// <para>
    /// ITelemetryClient batches proto messages from all connections to the same host
    /// before HTTP export. Events are flushed periodically or when the batch size
    /// threshold is reached.
    /// </para>
    /// <para>
    /// All operations are thread-safe and non-blocking. Exceptions are swallowed
    /// internally per the telemetry design principle that telemetry operations
    /// should never impact driver operations.
    /// </para>
    /// <para>
    /// JDBC Reference: TelemetryClient.java:15
    /// </para>
    /// </remarks>
    internal interface ITelemetryClient : IAsyncDisposable
    {
        /// <summary>
        /// Queue a telemetry log for export. Non-blocking, thread-safe.
        /// Events are batched and flushed periodically or when batch size is reached.
        /// Called by MetricsAggregator when a statement completes.
        /// </summary>
        /// <param name="log">The telemetry frontend log to enqueue for export.</param>
        void Enqueue(TelemetryFrontendLog log);

        /// <summary>
        /// Force flush all pending events immediately.
        /// Called when connection closes to ensure no events are lost.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A task representing the asynchronous flush operation.</returns>
        Task FlushAsync(CancellationToken ct = default);

        /// <summary>
        /// Gracefully close the client. Flushes pending events first.
        /// Called by TelemetryClientManager when reference count reaches zero.
        /// </summary>
        /// <returns>A task representing the asynchronous close operation.</returns>
        Task CloseAsync();
    }
}
