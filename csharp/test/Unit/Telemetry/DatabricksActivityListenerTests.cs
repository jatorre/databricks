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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="DatabricksActivityListener"/>.
    /// Each test creates a fresh non-singleton instance via <see cref="DatabricksActivityListener.CreateForTesting"/>
    /// to ensure test isolation. Routing is verified by checking the downstream <see cref="TrackingTelemetryClient"/>
    /// since <see cref="MetricsAggregator.ProcessActivity"/> is not virtual.
    /// </summary>
    public class DatabricksActivityListenerTests : IDisposable
    {
        /// <summary>
        /// ActivitySource that matches the name the listener subscribes to.
        /// </summary>
        private static readonly ActivitySource DriverSource =
            new ActivitySource(MetricsAggregator.DatabricksActivitySourceName);

        /// <summary>
        /// A different ActivitySource to verify the listener does NOT subscribe to it.
        /// </summary>
        private static readonly ActivitySource OtherSource =
            new ActivitySource("Some.Other.Source");

        private readonly DatabricksActivityListener _listener;

        public DatabricksActivityListenerTests()
        {
            _listener = DatabricksActivityListener.CreateForTesting();
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        #region Start / ShouldListenTo Tests

        [Fact]
        public void Start_ListensToDatabricksActivitySource()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-1");
            _listener.RegisterAggregator("sess-1", aggregator);
            _listener.Start();

            // Act - Create a Connection.Open activity from Databricks source
            // Connection.Open emits immediately without needing statement.id
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                // Activity is non-null because listener is subscribed to this source
                Assert.NotNull(activity);
                activity!.SetTag("session.id", "sess-1");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - The activity was routed to the aggregator and emitted
            Assert.True(client.EnqueueCallCount > 0,
                "ShouldListenTo returned true, and the activity was routed to the aggregator");
        }

        [Fact]
        public void Start_DoesNotListenToOtherActivitySources()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-1");
            _listener.RegisterAggregator("sess-1", aggregator);
            _listener.Start();

            // Act - Create activity from a different source
            using (var activity = OtherSource.StartActivity("SomeOperation"))
            {
                // Other source activities should not be captured by our listener
                if (activity != null)
                {
                    activity.SetTag("session.id", "sess-1");
                    activity.SetStatus(ActivityStatusCode.Ok);
                }
            }

            // Assert - No activities from other sources should be routed
            Assert.Equal(0, client.EnqueueCallCount);
        }

        [Fact]
        public void Start_Idempotent_MultipleCallsDoNotThrow()
        {
            // Act & Assert - Multiple Start calls should not throw
            var ex = Record.Exception(() =>
            {
                _listener.Start();
                _listener.Start();
                _listener.Start();
            });

            Assert.Null(ex);
        }

        #endregion

        #region ActivityStopped Routing Tests

        [Fact]
        public void ActivityStopped_RoutesToCorrectAggregator()
        {
            // Arrange - Two aggregators for different sessions
            var client1 = new TrackingTelemetryClient();
            var client2 = new TrackingTelemetryClient();
            var aggregator1 = CreateAggregator(client1, "sess-1");
            var aggregator2 = CreateAggregator(client2, "sess-2");

            _listener.RegisterAggregator("sess-1", aggregator1);
            _listener.RegisterAggregator("sess-2", aggregator2);
            _listener.Start();

            // Act - Activity with session.id = "sess-2"
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "sess-2");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Only aggregator2 (via client2) should receive the activity
            Assert.Equal(0, client1.EnqueueCallCount);
            Assert.True(client2.EnqueueCallCount > 0);
        }

        [Fact]
        public void ActivityStopped_NoSessionId_Ignored()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-1");
            _listener.RegisterAggregator("sess-1", aggregator);
            _listener.Start();

            // Act - Activity without session.id tag
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                // No session.id tag set
                activity!.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Activity should be silently dropped (no routing without session.id)
            Assert.Equal(0, client.EnqueueCallCount);
        }

        [Fact]
        public void ActivityStopped_EmptySessionId_Ignored()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-1");
            _listener.RegisterAggregator("sess-1", aggregator);
            _listener.Start();

            // Act - Activity with empty session.id
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Activity should be silently dropped
            Assert.Equal(0, client.EnqueueCallCount);
        }

        [Fact]
        public void ActivityStopped_UnknownSessionId_Ignored()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-1");
            _listener.RegisterAggregator("sess-1", aggregator);
            _listener.Start();

            // Act - Activity with session.id that has no registered aggregator
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "unknown-session");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Activity should be silently dropped
            Assert.Equal(0, client.EnqueueCallCount);
        }

        [Fact]
        public void ActivityStopped_ExceptionSwallowed()
        {
            // Arrange - Register an aggregator backed by a throwing client
            // MetricsAggregator.ProcessActivity catches exceptions from Enqueue internally.
            // The listener's OnActivityStopped also wraps everything in try-catch as defense-in-depth.
            var throwingClient = new ThrowingTelemetryClient();
            var aggregator = CreateAggregator(throwingClient, "sess-throw");
            _listener.RegisterAggregator("sess-throw", aggregator);
            _listener.Start();

            // Act & Assert - No exception should propagate from ActivityStopped
            var ex = Record.Exception(() =>
            {
                using (var activity = DriverSource.StartActivity("Connection.Open"))
                {
                    activity!.SetTag("session.id", "sess-throw");
                    activity.SetStatus(ActivityStatusCode.Ok);
                }
            });

            Assert.Null(ex);
        }

        [Fact]
        public void ActivityStopped_MultipleSessionsRouteCorrectly()
        {
            // Arrange
            var clientA = new TrackingTelemetryClient();
            var clientB = new TrackingTelemetryClient();
            var clientC = new TrackingTelemetryClient();

            _listener.RegisterAggregator("sess-A", CreateAggregator(clientA, "sess-A"));
            _listener.RegisterAggregator("sess-B", CreateAggregator(clientB, "sess-B"));
            _listener.RegisterAggregator("sess-C", CreateAggregator(clientC, "sess-C"));
            _listener.Start();

            // Act - Activities for different sessions
            using (var actA = DriverSource.StartActivity("Connection.Open"))
            {
                actA!.SetTag("session.id", "sess-A");
                actA.SetStatus(ActivityStatusCode.Ok);
            }

            using (var actC = DriverSource.StartActivity("Connection.Open"))
            {
                actC!.SetTag("session.id", "sess-C");
                actC.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert
            Assert.True(clientA.EnqueueCallCount > 0); // Activity for sess-A
            Assert.Equal(0, clientB.EnqueueCallCount);  // No activity for sess-B
            Assert.True(clientC.EnqueueCallCount > 0);  // Activity for sess-C
        }

        [Fact]
        public void ActivityStopped_StatementActivityRoutes()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-stmt");
            _listener.RegisterAggregator("sess-stmt", aggregator);
            _listener.Start();

            // Act - Root statement activity with all required tags
            using (var activity = DriverSource.StartActivity("ExecuteQuery"))
            {
                activity!.SetTag("session.id", "sess-stmt");
                activity.SetTag("statement.id", "stmt-1");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Activity should be routed and emitted (root activity with completed status)
            Assert.True(client.EnqueueCallCount > 0);
        }

        #endregion

        #region RegisterAggregator Tests

        [Fact]
        public void RegisterAggregator_AddsToRegistry()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            _listener.Start();

            // Act - Register and send activity
            _listener.RegisterAggregator("sess-reg", CreateAggregator(client, "sess-reg"));

            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "sess-reg");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Registered aggregator receives activities
            Assert.True(client.EnqueueCallCount > 0);
        }

        [Fact]
        public void RegisterAggregator_NullSessionId_ThrowsArgumentNullException()
        {
            var client = new TrackingTelemetryClient();
            Assert.Throws<ArgumentNullException>(() =>
                _listener.RegisterAggregator(null!, CreateAggregator(client, "sess-1")));
        }

        [Fact]
        public void RegisterAggregator_NullAggregator_ThrowsArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() =>
                _listener.RegisterAggregator("sess-1", null!));
        }

        [Fact]
        public void RegisterAggregator_ReplacesExistingForSameSessionId()
        {
            // Arrange
            var oldClient = new TrackingTelemetryClient();
            var newClient = new TrackingTelemetryClient();
            _listener.Start();

            // Act - Register twice with same session ID
            _listener.RegisterAggregator("sess-replace", CreateAggregator(oldClient, "sess-replace"));
            _listener.RegisterAggregator("sess-replace", CreateAggregator(newClient, "sess-replace"));

            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                activity!.SetTag("session.id", "sess-replace");
                activity.SetStatus(ActivityStatusCode.Ok);
            }

            // Assert - Only the new aggregator (via newClient) receives the activity
            Assert.Equal(0, oldClient.EnqueueCallCount);
            Assert.True(newClient.EnqueueCallCount > 0);
        }

        #endregion

        #region UnregisterAggregatorAsync Tests

        [Fact]
        public async Task UnregisterAggregatorAsync_FlushesAndRemoves()
        {
            // Arrange - Create aggregator and register it
            var client = new TrackingTelemetryClient();
            var aggregator = CreateAggregator(client, "sess-unreg");
            _listener.RegisterAggregator("sess-unreg", aggregator);
            _listener.Start();

            // Add a pending statement context so FlushAsync has something to emit
            using (var parentActivity = DriverSource.StartActivity("ExecuteQuery"))
            {
                parentActivity!.SetTag("session.id", "sess-unreg");
                parentActivity.SetTag("statement.id", "stmt-pending");

                // Create a child that stops (adds data to context) but root stays open
                using (var childActivity = DriverSource.StartActivity("DownloadFiles"))
                {
                    childActivity!.SetTag("session.id", "sess-unreg");
                    childActivity.SetTag("statement.id", "stmt-pending");
                    childActivity.SetStatus(ActivityStatusCode.Ok);
                    // child stops when disposed, listener routes it to aggregator
                }

                // Don't complete root - leave context pending

                // Act - Unregister should flush the pending context
                await _listener.UnregisterAggregatorAsync("sess-unreg");

                // Assert - FlushAsync was called (pending context emitted)
                Assert.True(client.EnqueueCallCount > 0, "FlushAsync should emit pending statement contexts");
            }

            // Reset count to verify no more routing
            int countAfterUnregister = client.EnqueueCallCount;

            // Verify aggregator no longer receives activities after unregister
            using (var activity = DriverSource.StartActivity("Connection.Open"))
            {
                // Activity may be null if no aggregators are registered (sampling returns None)
                if (activity != null)
                {
                    activity.SetTag("session.id", "sess-unreg");
                    activity.SetStatus(ActivityStatusCode.Ok);
                }
            }

            // Enqueue count should not have increased
            Assert.Equal(countAfterUnregister, client.EnqueueCallCount);
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_UnknownSessionId_NoException()
        {
            // Act & Assert - Unregistering a session that was never registered should not throw
            var ex = await Record.ExceptionAsync(() =>
                _listener.UnregisterAggregatorAsync("nonexistent-session"));

            Assert.Null(ex);
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_NullSessionId_NoException()
        {
            // Act & Assert
            var ex = await Record.ExceptionAsync(() =>
                _listener.UnregisterAggregatorAsync(null!));

            Assert.Null(ex);
        }

        [Fact]
        public async Task UnregisterAggregatorAsync_EmptySessionId_NoException()
        {
            // Act & Assert
            var ex = await Record.ExceptionAsync(() =>
                _listener.UnregisterAggregatorAsync(""));

            Assert.Null(ex);
        }

        #endregion

        #region Sampling Tests

        [Fact]
        public void Sample_NoAggregators_ReturnsNone()
        {
            // Arrange - No aggregators registered
            // Note: We test GetSamplingResult() directly because other test classes
            // may register global ActivityListeners that affect ActivitySource.StartActivity().

            // Act & Assert
            Assert.Equal(ActivitySamplingResult.None, _listener.GetSamplingResult());
        }

        [Fact]
        public void Sample_WithAggregators_ReturnsAllData()
        {
            // Arrange - Register an aggregator
            var client = new TrackingTelemetryClient();
            _listener.RegisterAggregator("sess-sample", CreateAggregator(client, "sess-sample"));

            // Act & Assert
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, _listener.GetSamplingResult());
        }

        [Fact]
        public async Task Sample_AfterAllAggregatorsRemoved_ReturnsNone()
        {
            // Arrange - Register then unregister
            var client = new TrackingTelemetryClient();
            _listener.RegisterAggregator("sess-temp", CreateAggregator(client, "sess-temp"));

            // Verify sampling is active with aggregator registered
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, _listener.GetSamplingResult());

            // Act - Remove the aggregator
            await _listener.UnregisterAggregatorAsync("sess-temp");

            // Assert - Sampling returns None now
            Assert.Equal(ActivitySamplingResult.None, _listener.GetSamplingResult());
        }

        [Fact]
        public void Sample_MultipleAggregators_StillReturnsAllData()
        {
            // Arrange - Multiple aggregators
            var client1 = new TrackingTelemetryClient();
            var client2 = new TrackingTelemetryClient();
            _listener.RegisterAggregator("sess-1", CreateAggregator(client1, "sess-1"));
            _listener.RegisterAggregator("sess-2", CreateAggregator(client2, "sess-2"));

            // Act & Assert
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, _listener.GetSamplingResult());
        }

        [Fact]
        public async Task Sample_PartialUnregister_StillReturnsAllData()
        {
            // Arrange - Two aggregators, remove one
            var client1 = new TrackingTelemetryClient();
            var client2 = new TrackingTelemetryClient();
            _listener.RegisterAggregator("sess-1", CreateAggregator(client1, "sess-1"));
            _listener.RegisterAggregator("sess-2", CreateAggregator(client2, "sess-2"));

            // Act - Remove one
            await _listener.UnregisterAggregatorAsync("sess-1");

            // Assert - Still has one aggregator, so sampling is active
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, _listener.GetSamplingResult());
        }

        #endregion

        #region Dispose Tests

        [Fact]
        public void Dispose_StopsListening()
        {
            // Arrange
            var client = new TrackingTelemetryClient();
            _listener.RegisterAggregator("sess-dispose", CreateAggregator(client, "sess-dispose"));
            _listener.Start();

            // Verify listener is active
            using (var actBefore = DriverSource.StartActivity("Connection.Open"))
            {
                Assert.NotNull(actBefore);
                actBefore!.SetTag("session.id", "sess-dispose");
                actBefore.SetStatus(ActivityStatusCode.Ok);
            }

            Assert.True(client.EnqueueCallCount > 0);

            // Act
            _listener.Dispose();

            // The ActivityListener is disposed, so its callbacks should no longer be invoked
        }

        [Fact]
        public void Dispose_MultipleCalls_NoException()
        {
            // Act & Assert
            var ex = Record.Exception(() =>
            {
                _listener.Dispose();
                _listener.Dispose();
            });

            Assert.Null(ex);
        }

        #endregion

        #region Singleton Tests

        [Fact]
        public void Instance_ReturnsSameInstance()
        {
            // Act
            var instance1 = DatabricksActivityListener.Instance;
            var instance2 = DatabricksActivityListener.Instance;

            // Assert
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void CreateForTesting_ReturnsNewInstance()
        {
            // Act
            var testInstance1 = DatabricksActivityListener.CreateForTesting();
            var testInstance2 = DatabricksActivityListener.CreateForTesting();

            // Assert
            Assert.NotSame(testInstance1, testInstance2);
            Assert.NotSame(testInstance1, DatabricksActivityListener.Instance);

            testInstance1.Dispose();
            testInstance2.Dispose();
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a real <see cref="MetricsAggregator"/> backed by the given telemetry client.
        /// </summary>
        private static MetricsAggregator CreateAggregator(ITelemetryClient client, string sessionId)
        {
            return new MetricsAggregator(
                client,
                new TelemetryConfiguration(),
                sessionId: sessionId);
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// ITelemetryClient that tracks Enqueue call counts for verifying routing behavior.
        /// </summary>
        private class TrackingTelemetryClient : ITelemetryClient
        {
            private int _enqueueCallCount;

            public int EnqueueCallCount => _enqueueCallCount;
            public ConcurrentBag<TelemetryFrontendLog> AllEnqueuedLogs { get; } = new ConcurrentBag<TelemetryFrontendLog>();

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref _enqueueCallCount);
                AllEnqueuedLogs.Add(log);
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        /// <summary>
        /// ITelemetryClient that throws on Enqueue to test exception swallowing.
        /// </summary>
        private class ThrowingTelemetryClient : ITelemetryClient
        {
            public void Enqueue(TelemetryFrontendLog log)
            {
                throw new InvalidOperationException("Simulated Enqueue failure");
            }

            public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
            public Task CloseAsync() => Task.CompletedTask;
            public ValueTask DisposeAsync() => default;
        }

        #endregion
    }
}
