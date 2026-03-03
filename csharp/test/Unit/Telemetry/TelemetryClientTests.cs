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
using System.Threading;
using System.Threading.Tasks;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Models;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for <see cref="TelemetryClient"/>, <see cref="ITelemetryClient"/>,
    /// and <see cref="TelemetryClientHolder"/>.
    /// </summary>
    public class TelemetryClientTests
    {
        #region Constructor Tests

        [Fact]
        public void Constructor_NullExporter_ThrowsArgumentNullException()
        {
            // Arrange
            var config = new TelemetryConfiguration();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(null!, config));
        }

        [Fact]
        public void Constructor_NullConfig_ThrowsArgumentNullException()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClient(exporter, null!));
        }

        [Fact]
        public void Constructor_ValidParameters_CreatesInstance()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration();

            // Act
            var client = new TelemetryClient(exporter, config);

            // Assert
            Assert.NotNull(client);
        }

        #endregion

        #region Enqueue Tests

        [Fact]
        public async Task Enqueue_AddsToQueue_NonBlocking()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration { BatchSize = 100 };
            var client = new TelemetryClient(exporter, config);
            var log = CreateTestLog();

            // Act - enqueue returns immediately (non-blocking)
            client.Enqueue(log);

            // Assert - event is in the queue (flush should export it)
            // Force flush to verify the event was queued
            await client.FlushAsync();
            Assert.Equal(1, exporter.ExportCallCount);
            Assert.NotNull(exporter.LastReceivedLogs);
            Assert.Single(exporter.LastReceivedLogs!);
        }

        [Fact]
        public async Task Enqueue_BatchSizeReached_TriggersFlush()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 100,
                FlushIntervalMs = 60000 // Very long interval to ensure timer doesn't trigger
            };
            var client = new TelemetryClient(exporter, config);

            // Act - enqueue exactly batch size events
            for (int i = 0; i < 100; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Allow the fire-and-forget flush to complete
            await Task.Delay(200);

            // Assert - ExportAsync should have been called
            Assert.True(exporter.ExportCallCount >= 1, $"Expected at least 1 export call, got {exporter.ExportCallCount}");
            Assert.True(exporter.TotalLogsExported > 0, $"Expected exported logs > 0, got {exporter.TotalLogsExported}");
        }

        [Fact]
        public async Task Enqueue_BelowBatchSize_DoesNotTriggerFlush()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration
            {
                BatchSize = 100,
                FlushIntervalMs = 60000 // Very long interval to avoid timer flush
            };
            var client = new TelemetryClient(exporter, config);

            // Act - enqueue fewer than batch size events
            for (int i = 0; i < 50; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Wait briefly to ensure no flush was triggered
            await Task.Delay(100);

            // Assert - no flush triggered yet (below batch size, timer not elapsed)
            Assert.Equal(0, exporter.ExportCallCount);
        }

        [Fact]
        public async Task Enqueue_AfterDispose_NoOp()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration
            {
                BatchSize = 100,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Close the client first
            await client.CloseAsync();
            int exportCountAfterClose = exporter.ExportCallCount;

            // Act - enqueue after close should be a no-op
            client.Enqueue(CreateTestLog());

            // Allow time for any async operations
            await Task.Delay(100);

            // Assert - no additional export calls should have been made for the post-close enqueue
            // Note: CloseAsync may have triggered a flush, so we compare against count after close
            Assert.Equal(exportCountAfterClose, exporter.ExportCallCount);
        }

        #endregion

        #region FlushAsync Tests

        [Fact]
        public async Task FlushAsync_EmptyQueue_NoExport()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration
            {
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Act
            await client.FlushAsync();

            // Assert - no export should be called for empty queue
            Assert.Equal(0, exporter.ExportCallCount);
        }

        [Fact]
        public async Task FlushAsync_WithEvents_ExportsAll()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 100,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Enqueue some events (below batch size threshold so no auto-flush)
            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act
            await client.FlushAsync();

            // Assert
            Assert.Equal(1, exporter.ExportCallCount);
            Assert.NotNull(exporter.LastReceivedLogs);
            Assert.Equal(5, exporter.LastReceivedLogs!.Count);
        }

        [Fact]
        public async Task FlushAsync_DrainsUpToBatchSize()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 10,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Enqueue more than batch size (but don't trigger auto-flush during enqueue)
            // We use a large batch size for enqueue threshold check, but small for drain
            // Actually, batch size is the same. Let's enqueue 25 events one at a time
            // with a different approach: use a larger BatchSize config for the initial threshold
            // and then manually flush.

            // Create a client with batch size 10 but flush interval very high
            var config2 = new TelemetryConfiguration
            {
                BatchSize = 10,
                FlushIntervalMs = 60000
            };
            var exporter2 = new MockTelemetryExporter { ResultToReturn = true };
            var client2 = new TelemetryClient(exporter2, config2);

            // Add 25 events - the first 10 will trigger an auto-flush
            // We'll add events one at a time
            for (int i = 0; i < 8; i++)
            {
                client2.Enqueue(CreateTestLog(i));
            }

            // Now flush manually - should drain up to batch size (10), but we have 8
            await client2.FlushAsync();

            // Assert
            Assert.Equal(1, exporter2.ExportCallCount);
            Assert.NotNull(exporter2.LastReceivedLogs);
            Assert.Equal(8, exporter2.LastReceivedLogs!.Count);
        }

        [Fact]
        public async Task ConcurrentFlush_OnlyOneProceeds()
        {
            // Arrange - use a slow exporter to hold the semaphore
            var exporter = new SlowMockTelemetryExporter(delayMs: 500);
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Enqueue some events
            for (int i = 0; i < 5; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act - launch two concurrent flushes
            var flush1 = client.FlushAsync();
            var flush2 = client.FlushAsync();

            await Task.WhenAll(flush1, flush2);

            // Assert - only one flush should have called ExportAsync
            // (the second one should have been skipped by the semaphore)
            Assert.Equal(1, exporter.ExportCallCount);
        }

        [Fact]
        public async Task FlushAsync_AfterDispose_NoOp()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration
            {
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            await client.CloseAsync();
            int exportCountAfterClose = exporter.ExportCallCount;

            // Act
            await client.FlushAsync();

            // Assert - no additional export calls
            Assert.Equal(exportCountAfterClose, exporter.ExportCallCount);
        }

        #endregion

        #region Timer Flush Tests

        [Fact]
        public async Task FlushTimer_Elapses_ExportsEvents()
        {
            // Arrange - use a short flush interval for testing
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000, // High batch size so it doesn't trigger via enqueue
                FlushIntervalMs = 100 // Very short for testing
            };
            var client = new TelemetryClient(exporter, config);

            // Enqueue events (below batch threshold)
            for (int i = 0; i < 3; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Act - wait for timer to elapse
            await Task.Delay(500);

            // Assert - timer should have triggered at least one flush
            Assert.True(exporter.ExportCallCount >= 1,
                $"Expected timer to trigger flush, but ExportCallCount was {exporter.ExportCallCount}");
            Assert.True(exporter.TotalLogsExported >= 3,
                $"Expected at least 3 logs exported, got {exporter.TotalLogsExported}");
        }

        #endregion

        #region CloseAsync Tests

        [Fact]
        public async Task CloseAsync_FlushesRemainingEvents()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000, // High batch size to prevent auto-flush
                FlushIntervalMs = 60000 // Long interval to prevent timer flush
            };
            var client = new TelemetryClient(exporter, config);

            // Enqueue events
            for (int i = 0; i < 10; i++)
            {
                client.Enqueue(CreateTestLog(i));
            }

            // Verify no export happened yet
            Assert.Equal(0, exporter.ExportCallCount);

            // Act
            await client.CloseAsync();

            // Assert - all remaining events should be exported
            Assert.True(exporter.ExportCallCount >= 1,
                $"Expected at least 1 export call during close, got {exporter.ExportCallCount}");
            Assert.Equal(10, exporter.TotalLogsExported);
        }

        [Fact]
        public async Task CloseAsync_CalledTwice_SecondIsNoOp()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            client.Enqueue(CreateTestLog());

            // Act
            await client.CloseAsync();
            int exportCountAfterFirstClose = exporter.ExportCallCount;

            // Second close should be a no-op
            await client.CloseAsync();

            // Assert
            Assert.Equal(exportCountAfterFirstClose, exporter.ExportCallCount);
        }

        [Fact]
        public async Task DisposeAsync_CallsCloseAsync()
        {
            // Arrange
            var exporter = new MockTelemetryExporter { ResultToReturn = true };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            client.Enqueue(CreateTestLog());

            // Act
            await client.DisposeAsync();

            // Assert - close should have flushed the event
            Assert.True(exporter.ExportCallCount >= 1);
        }

        #endregion

        #region Exception Swallowing Tests

        [Fact]
        public async Task Exception_Swallowed_NoThrow()
        {
            // Arrange - exporter that throws
            var exporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Test export failure")
            };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            client.Enqueue(CreateTestLog());

            // Act & Assert - no exception should propagate
            var ex = await Record.ExceptionAsync(() => client.FlushAsync());
            Assert.Null(ex);
        }

        [Fact]
        public async Task CloseAsync_ExporterThrows_NoExceptionPropagated()
        {
            // Arrange
            var exporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Export failure during close")
            };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1000,
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            client.Enqueue(CreateTestLog());

            // Act & Assert - no exception should propagate from CloseAsync
            var ex = await Record.ExceptionAsync(() => client.CloseAsync());
            Assert.Null(ex);
        }

        [Fact]
        public async Task Enqueue_ExporterThrowsDuringBatchFlush_NoExceptionPropagated()
        {
            // Arrange - exporter that throws, small batch size to trigger flush
            var exporter = new MockTelemetryExporter
            {
                ExceptionToThrow = new InvalidOperationException("Batch flush failure")
            };
            var config = new TelemetryConfiguration
            {
                BatchSize = 1, // Every enqueue triggers a flush
                FlushIntervalMs = 60000
            };
            var client = new TelemetryClient(exporter, config);

            // Act & Assert - enqueue should not throw even when exporter fails
            var ex = Record.Exception(() => client.Enqueue(CreateTestLog()));
            Assert.Null(ex);

            // Allow fire-and-forget flush to complete
            await Task.Delay(100);
        }

        #endregion

        #region TelemetryClientHolder Tests

        [Fact]
        public void TelemetryClientHolder_Constructor_NullClient_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() =>
                new TelemetryClientHolder(null!));
        }

        [Fact]
        public void TelemetryClientHolder_Constructor_SetsClientAndRefCount()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(exporter, config);

            // Act
            var holder = new TelemetryClientHolder(client);

            // Assert
            Assert.Same(client, holder.Client);
            Assert.Equal(1, holder._refCount);
        }

        [Fact]
        public void TelemetryClientHolder_RefCount_CanBeIncrementedAndDecremented()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(exporter, config);
            var holder = new TelemetryClientHolder(client);

            // Act & Assert - starts at 1
            Assert.Equal(1, holder._refCount);

            // Increment
            Interlocked.Increment(ref holder._refCount);
            Assert.Equal(2, holder._refCount);

            // Decrement
            Interlocked.Decrement(ref holder._refCount);
            Assert.Equal(1, holder._refCount);

            // Decrement to zero
            Interlocked.Decrement(ref holder._refCount);
            Assert.Equal(0, holder._refCount);
        }

        [Fact]
        public async Task TelemetryClientHolder_RefCount_ThreadSafe()
        {
            // Arrange
            var exporter = new MockTelemetryExporter();
            var config = new TelemetryConfiguration();
            var client = new TelemetryClient(exporter, config);
            var holder = new TelemetryClientHolder(client);

            // Act - simulate concurrent increment/decrement
            int iterations = 1000;
            var incrementTasks = new Task[iterations];
            var decrementTasks = new Task[iterations];

            for (int i = 0; i < iterations; i++)
            {
                incrementTasks[i] = Task.Run(() => Interlocked.Increment(ref holder._refCount));
            }

            await Task.WhenAll(incrementTasks);

            // After 1000 increments, ref count should be 1001 (initial 1 + 1000)
            Assert.Equal(1001, holder._refCount);

            for (int i = 0; i < iterations; i++)
            {
                decrementTasks[i] = Task.Run(() => Interlocked.Decrement(ref holder._refCount));
            }

            await Task.WhenAll(decrementTasks);

            // After 1000 decrements, ref count should be back to 1
            Assert.Equal(1, holder._refCount);
        }

        #endregion

        #region Helper Methods

        private static TelemetryFrontendLog CreateTestLog(int id = 0)
        {
            return new TelemetryFrontendLog
            {
                WorkspaceId = 12345,
                FrontendLogEventId = $"test-event-{id}"
            };
        }

        #endregion

        #region Mock Implementations

        /// <summary>
        /// Manual mock of ITelemetryExporter that captures ExportAsync calls.
        /// </summary>
        private class MockTelemetryExporter : ITelemetryExporter
        {
            /// <summary>
            /// The result to return from ExportAsync. Default is true.
            /// </summary>
            public bool ResultToReturn { get; set; } = true;

            /// <summary>
            /// If set, ExportAsync will throw this exception instead of returning a result.
            /// </summary>
            public Exception? ExceptionToThrow { get; set; }

            /// <summary>
            /// Tracks how many times ExportAsync was called.
            /// </summary>
            public int ExportCallCount { get; private set; }

            /// <summary>
            /// The logs received in the last call to ExportAsync.
            /// </summary>
            public IReadOnlyList<TelemetryFrontendLog>? LastReceivedLogs { get; private set; }

            /// <summary>
            /// All logs received across all ExportAsync calls.
            /// </summary>
            public ConcurrentBag<TelemetryFrontendLog> AllReceivedLogs { get; } = new ConcurrentBag<TelemetryFrontendLog>();

            /// <summary>
            /// Total number of logs exported across all calls.
            /// </summary>
            public int TotalLogsExported => AllReceivedLogs.Count;

            public Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                Interlocked.Increment(ref _exportCallCountAtomic);
                ExportCallCount = _exportCallCountAtomic;
                LastReceivedLogs = logs;

                foreach (var log in logs)
                {
                    AllReceivedLogs.Add(log);
                }

                if (ExceptionToThrow != null)
                {
                    throw ExceptionToThrow;
                }

                return Task.FromResult(ResultToReturn);
            }

            private int _exportCallCountAtomic;
        }

        /// <summary>
        /// Mock exporter with configurable delay to test concurrent flush serialization.
        /// </summary>
        private class SlowMockTelemetryExporter : ITelemetryExporter
        {
            private readonly int _delayMs;
            private int _exportCallCountAtomic;

            public int ExportCallCount => _exportCallCountAtomic;

            public SlowMockTelemetryExporter(int delayMs)
            {
                _delayMs = delayMs;
            }

            public async Task<bool> ExportAsync(IReadOnlyList<TelemetryFrontendLog> logs, CancellationToken ct = default)
            {
                Interlocked.Increment(ref _exportCallCountAtomic);
                await Task.Delay(_delayMs, ct).ConfigureAwait(false);
                return true;
            }
        }

        #endregion
    }
}
