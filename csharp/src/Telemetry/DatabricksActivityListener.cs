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
using System.Diagnostics;
using System.Threading.Tasks;

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Global singleton that listens to Activity events from the Databricks ADBC driver
    /// and routes them to the appropriate <see cref="MetricsAggregator"/> based on session ID.
    /// </summary>
    /// <remarks>
    /// <para>
    /// .NET <see cref="ActivityListener"/> is inherently global: when registered, it receives
    /// callbacks for ALL activities from subscribed <see cref="ActivitySource"/>s, regardless of
    /// which thread or connection created them. This class acts as a router, using the
    /// <c>session.id</c> tag on each activity to dispatch it to the correct per-connection
    /// <see cref="MetricsAggregator"/>.
    /// </para>
    /// <para>
    /// Connections call <see cref="RegisterAggregator"/> on open and
    /// <see cref="UnregisterAggregatorAsync"/> on close. The listener's sampling callback
    /// returns <see cref="ActivitySamplingResult.AllDataAndRecorded"/> when at least one
    /// aggregator is registered, and <see cref="ActivitySamplingResult.None"/> otherwise,
    /// so that no overhead is incurred when telemetry is inactive.
    /// </para>
    /// <para>
    /// All exceptions in callbacks are swallowed to ensure telemetry never impacts driver operations.
    /// </para>
    /// </remarks>
    internal sealed class DatabricksActivityListener : IDisposable
    {
        private static readonly Lazy<DatabricksActivityListener> _instance =
            new Lazy<DatabricksActivityListener>(() => new DatabricksActivityListener());

        /// <summary>
        /// Gets the singleton instance of <see cref="DatabricksActivityListener"/>.
        /// </summary>
        public static DatabricksActivityListener Instance => _instance.Value;

        /// <summary>
        /// Registry of aggregators keyed by session ID.
        /// Each connection registers its aggregator on open and unregisters on close.
        /// </summary>
        private readonly ConcurrentDictionary<string, MetricsAggregator> _aggregators =
            new ConcurrentDictionary<string, MetricsAggregator>();

        /// <summary>
        /// The underlying .NET ActivityListener that receives global activity callbacks.
        /// Created by <see cref="Start"/> and disposed by <see cref="Dispose"/>.
        /// </summary>
        private ActivityListener? _listener;

        private volatile bool _disposed;

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private DatabricksActivityListener()
        {
        }

        /// <summary>
        /// Registers a connection's <see cref="MetricsAggregator"/>.
        /// Called when a connection opens. Activities with the given session ID
        /// will be routed to this aggregator.
        /// </summary>
        /// <param name="sessionId">The connection's session ID.</param>
        /// <param name="aggregator">The aggregator to receive activities for this session.</param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when <paramref name="sessionId"/> or <paramref name="aggregator"/> is null.
        /// </exception>
        public void RegisterAggregator(string sessionId, MetricsAggregator aggregator)
        {
            if (sessionId == null) throw new ArgumentNullException(nameof(sessionId));
            if (aggregator == null) throw new ArgumentNullException(nameof(aggregator));

            _aggregators[sessionId] = aggregator;
        }

        /// <summary>
        /// Unregisters a connection's <see cref="MetricsAggregator"/> and flushes it.
        /// Called when a connection closes to ensure all pending telemetry is emitted.
        /// </summary>
        /// <param name="sessionId">The connection's session ID.</param>
        /// <returns>A task representing the asynchronous flush operation.</returns>
        public async Task UnregisterAggregatorAsync(string sessionId)
        {
            if (string.IsNullOrEmpty(sessionId)) return;

            if (_aggregators.TryRemove(sessionId, out var aggregator))
            {
                try
                {
                    await aggregator.FlushAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[TRACE] Telemetry error flushing aggregator for session {sessionId}: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Creates and registers the <see cref="ActivityListener"/> to start receiving
        /// activity callbacks from the Databricks driver's <see cref="ActivitySource"/>.
        /// </summary>
        /// <remarks>
        /// This method is idempotent: calling it multiple times has no additional effect
        /// after the first call.
        /// </remarks>
        public void Start()
        {
            if (_disposed || _listener != null) return;

            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == MetricsAggregator.DatabricksActivitySourceName,
                ActivityStopped = OnActivityStopped,
                Sample = SampleActivity
            };

            ActivitySource.AddActivityListener(_listener);
        }

        /// <summary>
        /// Returns the sampling result based on whether any aggregators are registered.
        /// Returns <see cref="ActivitySamplingResult.AllDataAndRecorded"/> when at least one
        /// aggregator exists, or <see cref="ActivitySamplingResult.None"/> otherwise.
        /// </summary>
        /// <returns>The current sampling result.</returns>
        internal ActivitySamplingResult GetSamplingResult()
        {
            return _aggregators.IsEmpty
                ? ActivitySamplingResult.None
                : ActivitySamplingResult.AllDataAndRecorded;
        }

        /// <summary>
        /// Sampling callback. Delegates to <see cref="GetSamplingResult"/>.
        /// </summary>
        private ActivitySamplingResult SampleActivity(ref ActivityCreationOptions<ActivityContext> options)
        {
            return GetSamplingResult();
        }

        /// <summary>
        /// Callback invoked when an activity stops. Extracts the <c>session.id</c> tag
        /// and routes the activity to the corresponding aggregator.
        /// </summary>
        /// <param name="activity">The activity that stopped.</param>
        private void OnActivityStopped(Activity activity)
        {
            try
            {
                if (activity == null) return;

                var sessionId = activity.GetTagItem("session.id")?.ToString();
                if (string.IsNullOrEmpty(sessionId)) return;

                if (_aggregators.TryGetValue(sessionId, out var aggregator))
                {
                    aggregator.ProcessActivity(activity);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"[TRACE] Telemetry error in OnActivityStopped: {ex.Message}");
            }
        }

        /// <summary>
        /// Disposes the listener and clears all registered aggregators.
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _listener?.Dispose();
            _listener = null;
            _aggregators.Clear();
        }

        /// <summary>
        /// For testing only: creates a new non-singleton instance.
        /// </summary>
        /// <returns>A fresh <see cref="DatabricksActivityListener"/> instance for test isolation.</returns>
        internal static DatabricksActivityListener CreateForTesting()
        {
            return new DatabricksActivityListener();
        }
    }
}
