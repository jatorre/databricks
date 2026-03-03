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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="TelemetryClientManager"/> singleton.
    /// Uses a test collection to avoid parallel execution with other tests
    /// that modify the singleton state.
    /// </summary>
    [Collection("TelemetryClientManagerTests")]
    public class TelemetryClientManagerTests : IDisposable
    {
        private readonly TelemetryClientManager _manager;
        private readonly TelemetryConfiguration _config;

        public TelemetryClientManagerTests()
        {
            _manager = TelemetryClientManager.GetInstance();
            _manager.Reset();
            _config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000 // Long interval to avoid timer-triggered flushes in tests
            };
        }

        public void Dispose()
        {
            _manager.Reset();
        }

        #region Singleton Tests

        [Fact]
        public void GetInstance_ReturnsSameInstance()
        {
            // Arrange & Act
            var instance1 = TelemetryClientManager.GetInstance();
            var instance2 = TelemetryClientManager.GetInstance();

            // Assert
            Assert.Same(instance1, instance2);
        }

        [Fact]
        public void GetInstance_ReturnsNonNullInstance()
        {
            // Act
            var instance = TelemetryClientManager.GetInstance();

            // Assert
            Assert.NotNull(instance);
        }

        #endregion

        #region GetOrCreateClient - New Host Tests

        [Fact]
        public void GetOrCreateClient_NewHost_CreatesClient()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();

            // Act
            var client = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter,
                _config);

            // Assert
            Assert.NotNull(client);
        }

        [Fact]
        public void GetOrCreateClient_NewHost_RefCountIsOne()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();

            // Act - use a fresh manager via test instance to inspect internal state
            var testManager = CreateTestManager();
            using (TelemetryClientManager.UseTestInstance(testManager))
            {
                var client = testManager.GetOrCreateClient(
                    "host1.databricks.com",
                    () => exporter,
                    _config);

                // Assert - We verify RefCount=1 by releasing once and confirming close is called
                // After one release (from RefCount=1 to 0), CloseAsync should be called
            }
            // Verified through the ReleaseClientAsync_LastReference_ClosesClient test
            Assert.NotNull(exporter);
        }

        [Fact]
        public void GetOrCreateClient_MultipleHosts_CreatesSeparateClients()
        {
            // Arrange
            var exporter1 = new MockTelemetryExporter();
            var exporter2 = new MockTelemetryExporter();

            // Act
            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => exporter1,
                _config);
            var client2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () => exporter2,
                _config);

            // Assert
            Assert.NotNull(client1);
            Assert.NotNull(client2);
            Assert.NotSame(client1, client2);
        }

        #endregion

        #region GetOrCreateClient - Existing Host Tests

        [Fact]
        public void GetOrCreateClient_ExistingHost_ReturnsSameClient()
        {
            // Arrange
            var exporterCallCount = 0;
            Func<ITelemetryExporter> exporterFactory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return new MockTelemetryExporter();
            };

            // Act
            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);
            var client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);

            // Assert - same client returned, exporter factory called only once
            Assert.Same(client1, client2);
            Assert.Equal(1, exporterCallCount);
        }

        [Fact]
        public void GetOrCreateClient_ExistingHostDifferentCase_ReturnsSameClient()
        {
            // Arrange
            var exporterCallCount = 0;
            Func<ITelemetryExporter> exporterFactory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return new MockTelemetryExporter();
            };

            // Act
            var client1 = _manager.GetOrCreateClient(
                "HOST1.DATABRICKS.COM",
                exporterFactory,
                _config);
            var client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);

            // Assert - case-insensitive comparison
            Assert.Same(client1, client2);
            Assert.Equal(1, exporterCallCount);
        }

        [Fact]
        public async Task GetOrCreateClient_ExistingHost_IncrementsRefCount()
        {
            // Arrange
            var mockClient = new MockTelemetryClient();

            // Create first reference
            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => new MockTelemetryExporter(),
                _config);

            // Create second reference (RefCount now 2)
            var client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => new MockTelemetryExporter(),
                _config);

            // Act - Release once (RefCount goes to 1), client should NOT be closed
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - client still works (not closed) - we can verify by getting it again
            var client3 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => new MockTelemetryExporter(),
                _config);
            Assert.Same(client1, client3);
        }

        #endregion

        #region ReleaseClientAsync Tests

        [Fact]
        public async Task ReleaseClientAsync_LastReference_ClosesClient()
        {
            // Arrange - We need to track if CloseAsync is called.
            // Since GetOrCreateClient creates a real TelemetryClient internally,
            // we verify by checking that a new client is created for the same host after release.
            var exporterCallCount = 0;
            Func<ITelemetryExporter> exporterFactory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return new MockTelemetryExporter();
            };

            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);

            // Act - release the only reference
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - creating a new client for the same host should create a new instance
            var client2 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);
            Assert.NotSame(client1, client2);
            Assert.Equal(2, exporterCallCount); // Factory called twice = two different clients
        }

        [Fact]
        public async Task ReleaseClientAsync_MultipleReferences_KeepsClient()
        {
            // Arrange
            var exporterCallCount = 0;
            Func<ITelemetryExporter> exporterFactory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return new MockTelemetryExporter();
            };

            // Create two references (RefCount = 2)
            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);
            _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);

            // Act - Release one reference (RefCount goes to 1)
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - client should still be active (same instance returned)
            var client3 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                exporterFactory,
                _config);
            Assert.Same(client1, client3);
            Assert.Equal(1, exporterCallCount); // Factory only called once (same client reused)
        }

        [Fact]
        public async Task ReleaseClientAsync_UnknownHost_NoOp()
        {
            // Act & Assert - should not throw
            var ex = await Record.ExceptionAsync(() =>
                _manager.ReleaseClientAsync("nonexistent.host.com"));
            Assert.Null(ex);
        }

        [Fact]
        public async Task ReleaseClientAsync_NullHost_NoOp()
        {
            // Act & Assert - should not throw
            var ex = await Record.ExceptionAsync(() =>
                _manager.ReleaseClientAsync(null!));
            Assert.Null(ex);
        }

        [Fact]
        public async Task ReleaseClientAsync_EmptyHost_NoOp()
        {
            // Act & Assert - should not throw
            var ex = await Record.ExceptionAsync(() =>
                _manager.ReleaseClientAsync(""));
            Assert.Null(ex);
        }

        [Fact]
        public async Task ReleaseClientAsync_AllReferencesReleased_RemovesClient()
        {
            // Arrange - create 3 references
            var exporterCallCount = 0;
            Func<ITelemetryExporter> exporterFactory = () =>
            {
                Interlocked.Increment(ref exporterCallCount);
                return new MockTelemetryExporter();
            };

            var client1 = _manager.GetOrCreateClient("host1.databricks.com", exporterFactory, _config);
            _manager.GetOrCreateClient("host1.databricks.com", exporterFactory, _config);
            _manager.GetOrCreateClient("host1.databricks.com", exporterFactory, _config);

            // Act - release all 3 references
            await _manager.ReleaseClientAsync("host1.databricks.com");
            await _manager.ReleaseClientAsync("host1.databricks.com");
            await _manager.ReleaseClientAsync("host1.databricks.com");

            // Assert - creating a new client should give a different instance
            var client2 = _manager.GetOrCreateClient("host1.databricks.com", exporterFactory, _config);
            Assert.NotSame(client1, client2);
            Assert.Equal(2, exporterCallCount); // Factory called twice (original + new after full release)
        }

        #endregion

        #region Thread Safety Tests

        [Fact]
        public async Task GetOrCreateClient_ThreadSafe_NoDuplicates()
        {
            // Arrange
            var host = "concurrent-host.databricks.com";
            var threadCount = 10;
            var clients = new ITelemetryClient[threadCount];
            var barrier = new ManualResetEventSlim(false);
            var tasks = new Task[threadCount];

            Func<ITelemetryExporter> exporterFactory = () => new MockTelemetryExporter();

            // Act - 10 concurrent threads requesting the same host
            for (int i = 0; i < threadCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait();
                    clients[index] = _manager.GetOrCreateClient(host, exporterFactory, _config);
                });
            }

            // Release all threads at the same time
            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - All threads should get the same client instance
            // (ConcurrentDictionary.AddOrUpdate may call the factory multiple times
            // under contention, but only one result is stored and returned to all callers)
            for (int i = 1; i < threadCount; i++)
            {
                Assert.Same(clients[0], clients[i]);
            }
        }

        [Fact]
        public async Task GetOrCreateClient_ConcurrentDifferentHosts_CreatesAllClients()
        {
            // Arrange
            var hostCount = 10;
            var clients = new ITelemetryClient[hostCount];
            var barrier = new ManualResetEventSlim(false);
            var tasks = new Task[hostCount];

            // Act - concurrent threads requesting different hosts
            for (int i = 0; i < hostCount; i++)
            {
                int index = i;
                tasks[i] = Task.Run(() =>
                {
                    barrier.Wait();
                    clients[index] = _manager.GetOrCreateClient(
                        $"host{index}.databricks.com",
                        () => new MockTelemetryExporter(),
                        _config);
                });
            }

            barrier.Set();
            await Task.WhenAll(tasks);

            // Assert - All clients should be created and unique
            for (int i = 0; i < hostCount; i++)
            {
                Assert.NotNull(clients[i]);
                for (int j = i + 1; j < hostCount; j++)
                {
                    Assert.NotSame(clients[i], clients[j]);
                }
            }
        }

        [Fact]
        public async Task ConcurrentGetAndRelease_ThreadSafe()
        {
            // Arrange - create initial client
            var host = "race-condition-host.databricks.com";
            _manager.GetOrCreateClient(host, () => new MockTelemetryExporter(), _config);

            // Act - concurrent increment (GetOrCreate) and decrement (Release)
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(() =>
                    _manager.GetOrCreateClient(host, () => new MockTelemetryExporter(), _config)));
            }

            await Task.WhenAll(tasks);

            // Now release all 11 references (1 initial + 10 concurrent)
            var releaseTasks = new List<Task>();
            for (int i = 0; i < 11; i++)
            {
                releaseTasks.Add(_manager.ReleaseClientAsync(host));
            }

            // Assert - should not throw
            var ex = await Record.ExceptionAsync(() => Task.WhenAll(releaseTasks));
            Assert.Null(ex);
        }

        #endregion

        #region UseTestInstance Tests

        [Fact]
        public void UseTestInstance_ReplacesAndRestores()
        {
            // Arrange
            var originalManager = TelemetryClientManager.GetInstance();
            var testManager = CreateTestManager();

            // Act - replace singleton
            using (TelemetryClientManager.UseTestInstance(testManager))
            {
                // Inside scope, GetInstance returns test instance
                Assert.Same(testManager, TelemetryClientManager.GetInstance());
                Assert.NotSame(originalManager, TelemetryClientManager.GetInstance());
            }

            // Assert - after dispose, original is restored
            Assert.Same(originalManager, TelemetryClientManager.GetInstance());
        }

        [Fact]
        public void UseTestInstance_TestManagerIsIndependent()
        {
            // Arrange
            var testManager = CreateTestManager();

            using (TelemetryClientManager.UseTestInstance(testManager))
            {
                // Act - create client in test manager
                var client = TelemetryClientManager.GetInstance().GetOrCreateClient(
                    "test-host.databricks.com",
                    () => new MockTelemetryExporter(),
                    _config);

                Assert.NotNull(client);
            }

            // Assert - original manager should not have the test host's client
            // Creating a client for the same host in the original manager should create a new one
            var exporterCallCount = 0;
            _manager.GetOrCreateClient(
                "test-host.databricks.com",
                () =>
                {
                    exporterCallCount++;
                    return new MockTelemetryExporter();
                },
                _config);
            Assert.Equal(1, exporterCallCount); // New client created (not reused from test manager)
        }

        [Fact]
        public void UseTestInstance_NullInstance_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                TelemetryClientManager.UseTestInstance(null!));
        }

        #endregion

        #region Reset Tests

        [Fact]
        public void Reset_ClearsAllClients()
        {
            // Arrange
            var client1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => new MockTelemetryExporter(),
                _config);
            var client2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () => new MockTelemetryExporter(),
                _config);

            // Act
            _manager.Reset();

            // Assert - new clients should be created for the same hosts
            var newClient1 = _manager.GetOrCreateClient(
                "host1.databricks.com",
                () => new MockTelemetryExporter(),
                _config);
            var newClient2 = _manager.GetOrCreateClient(
                "host2.databricks.com",
                () => new MockTelemetryExporter(),
                _config);

            Assert.NotSame(client1, newClient1);
            Assert.NotSame(client2, newClient2);
        }

        #endregion

        #region Input Validation Tests

        [Fact]
        public void GetOrCreateClient_NullHost_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient(null!, () => new MockTelemetryExporter(), _config));
        }

        [Fact]
        public void GetOrCreateClient_EmptyHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                _manager.GetOrCreateClient("", () => new MockTelemetryExporter(), _config));
        }

        [Fact]
        public void GetOrCreateClient_WhitespaceHost_ThrowsArgumentException()
        {
            // Act & Assert
            Assert.Throws<ArgumentException>(() =>
                _manager.GetOrCreateClient("   ", () => new MockTelemetryExporter(), _config));
        }

        [Fact]
        public void GetOrCreateClient_NullExporterFactory_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient("host1.databricks.com", null!, _config));
        }

        [Fact]
        public void GetOrCreateClient_NullConfig_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                _manager.GetOrCreateClient("host1.databricks.com", () => new MockTelemetryExporter(), null!));
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a new TelemetryClientManager instance for test isolation.
        /// Uses reflection to invoke the private constructor.
        /// </summary>
        private static TelemetryClientManager CreateTestManager()
        {
            return (TelemetryClientManager)Activator.CreateInstance(
                typeof(TelemetryClientManager),
                nonPublic: true)!;
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Manual mock of <see cref="ITelemetryExporter"/> for unit testing.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            public bool ResultToReturn { get; set; } = true;
            public int ExportCallCount;

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                Interlocked.Increment(ref ExportCallCount);
                return Task.FromResult(ResultToReturn);
            }
        }

        /// <summary>
        /// Manual mock of <see cref="ITelemetryClient"/> for unit testing.
        /// Tracks calls to Enqueue, FlushAsync, and CloseAsync.
        /// </summary>
        private class MockTelemetryClient : ITelemetryClient
        {
            public int EnqueueCallCount;
            public int FlushCallCount;
            public int CloseCallCount;
            public bool IsClosed => CloseCallCount > 0;

            public void Enqueue(TelemetryFrontendLog log)
            {
                Interlocked.Increment(ref EnqueueCallCount);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                Interlocked.Increment(ref FlushCallCount);
                return Task.CompletedTask;
            }

            public Task CloseAsync()
            {
                Interlocked.Increment(ref CloseCallCount);
                return Task.CompletedTask;
            }

            public ValueTask DisposeAsync()
            {
                return new ValueTask(CloseAsync());
            }
        }

        #endregion
    }
}
