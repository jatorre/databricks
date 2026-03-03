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
using System.Diagnostics;
using System.Linq;
using AdbcDrivers.Databricks.Telemetry;
using AdbcDrivers.Databricks.Telemetry.Proto;
using AdbcDrivers.Databricks.Telemetry.TagDefinitions;
using Xunit;

namespace AdbcDrivers.Databricks.Tests.Unit.Telemetry
{
    /// <summary>
    /// Tests for statement and CloudFetch activity tags.
    /// Validates that all required telemetry tags are correctly propagated and processed.
    /// </summary>
    public class StatementActivityTagsTests : IDisposable
    {
        private static readonly ActivitySource TestSource = new("Test.StatementActivityTags");
        private readonly ActivityListener _listener;

        public StatementActivityTagsTests()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public void Dispose()
        {
            _listener.Dispose();
        }

        #region StatementExecutionEvent Tag Constants Tests

        [Fact]
        public void StatementExecutionEvent_ContainsStatementIdConstant()
        {
            Assert.Equal("statement.id", StatementExecutionEvent.StatementId);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsSessionIdConstant()
        {
            Assert.Equal("session.id", StatementExecutionEvent.SessionId);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsStatementTypeConstant()
        {
            Assert.Equal("statement.type", StatementExecutionEvent.StatementType);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsResultFormatConstant()
        {
            Assert.Equal("result.format", StatementExecutionEvent.ResultFormat);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsResultCompressionEnabledConstant()
        {
            Assert.Equal("result.compression_enabled", StatementExecutionEvent.ResultCompressionEnabled);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsPollCountConstant()
        {
            Assert.Equal("poll.count", StatementExecutionEvent.PollCount);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsPollLatencyMsConstant()
        {
            Assert.Equal("poll.latency_ms", StatementExecutionEvent.PollLatencyMs);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsResultReadyLatencyMsConstant()
        {
            Assert.Equal("result.ready_latency_ms", StatementExecutionEvent.ResultReadyLatencyMs);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsResultConsumptionLatencyMsConstant()
        {
            Assert.Equal("result.consumption_latency_ms", StatementExecutionEvent.ResultConsumptionLatencyMs);
        }

        [Fact]
        public void StatementExecutionEvent_ContainsCloudFetchDownloadSummaryEventConstant()
        {
            Assert.Equal("cloudfetch.download_summary", StatementExecutionEvent.CloudFetchDownloadSummaryEvent);
        }

        [Fact]
        public void StatementExecutionEvent_GetDatabricksExportTags_ContainsAllRequiredTags()
        {
            // Act
            var tags = StatementExecutionEvent.GetDatabricksExportTags();

            // Assert - verify all required statement tags are included
            Assert.Contains(StatementExecutionEvent.StatementId, tags);
            Assert.Contains(StatementExecutionEvent.SessionId, tags);
            Assert.Contains(StatementExecutionEvent.StatementType, tags);
            Assert.Contains(StatementExecutionEvent.ResultFormat, tags);
            Assert.Contains(StatementExecutionEvent.ResultCompressionEnabled, tags);
            Assert.Contains(StatementExecutionEvent.PollCount, tags);
            Assert.Contains(StatementExecutionEvent.PollLatencyMs, tags);
            Assert.Contains(StatementExecutionEvent.ResultReadyLatencyMs, tags);
            Assert.Contains(StatementExecutionEvent.ResultConsumptionLatencyMs, tags);
        }

        #endregion

        #region Statement Activity with session.id and statement.id Tests

        [Fact]
        public void StatementActivity_WithSessionAndStatementId_MergesCorrectly()
        {
            // Arrange - Simulate a statement activity with routing tags
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "test-session-123");
                a.SetTag(StatementExecutionEvent.StatementId, "test-statement-456");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, true);
                a.SetTag(StatementExecutionEvent.ResultReadyLatencyMs, 250L);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("test-session-123", context.SessionId);
            Assert.Equal("test-statement-456", context.StatementId);
            Assert.Equal(StatementType.StatementQuery, context.StatementType);
            Assert.True(context.CompressionEnabled);
            Assert.Equal(250L, context.ResultReadyLatencyMs);
        }

        [Fact]
        public void MetadataActivity_WithSessionAndStatementId_MergesCorrectly()
        {
            // Arrange - Simulate a metadata activity (GetCatalogs)
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetCatalogs", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-meta-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-meta-1");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("session-meta-1", context.SessionId);
            Assert.Equal("stmt-meta-1", context.StatementId);
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
        }

        [Fact]
        public void MetadataActivity_GetSchemas_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetSchemas", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-schema-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-schema-1");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal("session-schema-1", context.SessionId);
            Assert.Equal("stmt-schema-1", context.StatementId);
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
        }

        [Fact]
        public void MetadataActivity_GetTables_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetTables", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-tables-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-tables-1");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
        }

        [Fact]
        public void MetadataActivity_GetColumns_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetColumns", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-cols-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-cols-1");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
        }

        [Fact]
        public void MetadataActivity_GetTableTypes_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("GetTableTypes", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-types-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-types-1");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementMetadata, context.StatementType);
        }

        [Fact]
        public void UpdateActivity_WithStatementType_MergesCorrectly()
        {
            // Arrange - Simulate an update statement
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteUpdate", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-update-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-update-1");
                a.SetTag(StatementExecutionEvent.StatementType, "update");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, false);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(StatementType.StatementUpdate, context.StatementType);
            Assert.False(context.CompressionEnabled);
        }

        #endregion

        #region Child Activity Tag Propagation Tests

        [Fact]
        public void ChildActivity_PropagatesRoutingTags_FromParent()
        {
            // Arrange - Simulate parent with routing tags and child that propagates them
            var context = new StatementTelemetryContext();

            // Root activity sets routing tags
            var rootActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "root-session");
                a.SetTag(StatementExecutionEvent.StatementId, "root-stmt");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");
            });

            // Child activity propagates routing tags (simulating what DatabricksStatement does)
            var childActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "root-session"); // Propagated from parent
                a.SetTag(StatementExecutionEvent.StatementId, "root-stmt");  // Propagated from parent
                // Add download summary
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 5;
                eventTags["successful_downloads"] = 5;
                eventTags["total_time_ms"] = 2000L;
                eventTags["initial_chunk_latency_ms"] = 80L;
                eventTags["slowest_chunk_latency_ms"] = 300L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            // Act
            context.MergeFrom(rootActivity);
            context.MergeFrom(childActivity);

            // Assert - All data is aggregated with same routing
            Assert.Equal("root-session", context.SessionId);
            Assert.Equal("root-stmt", context.StatementId);
            Assert.Equal(StatementType.StatementQuery, context.StatementType);
            Assert.Equal("cloudfetch", context.ResultFormat);
            Assert.Equal(5, context.TotalChunksPresent);
            Assert.Equal(80L, context.InitialChunkLatencyMs);
            Assert.Equal(300L, context.SlowestChunkLatencyMs);
        }

        [Fact]
        public void ChildActivity_PollActivity_PropagatesRoutingTags()
        {
            // Arrange - Simulate polling activity with propagated routing tags
            var context = new StatementTelemetryContext();

            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "poll-session");
                a.SetTag(StatementExecutionEvent.StatementId, "poll-stmt");
                a.SetTag(StatementExecutionEvent.PollCount, 3);
                a.SetTag(StatementExecutionEvent.PollLatencyMs, 1500L);
            });

            // Act
            context.MergeFrom(pollActivity);

            // Assert
            Assert.Equal("poll-session", context.SessionId);
            Assert.Equal("poll-stmt", context.StatementId);
            Assert.Equal(3, context.PollCount);
            Assert.Equal(1500L, context.PollLatencyMs);
        }

        #endregion

        #region CloudFetch Download Summary Event Tests

        [Fact]
        public void CloudFetchDownloadSummary_WithChunkLatencyMetrics_MergesCorrectly()
        {
            // Arrange - Simulate the cloudfetch.download_summary event with all chunk metrics
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-cf-1");
                a.SetTag(StatementExecutionEvent.SessionId, "sess-cf-1");

                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 15;
                eventTags["successful_downloads"] = 12;
                eventTags["total_time_ms"] = 6000L;
                eventTags["initial_chunk_latency_ms"] = 120L;
                eventTags["slowest_chunk_latency_ms"] = 800L;
                a.AddEvent(new ActivityEvent(StatementExecutionEvent.CloudFetchDownloadSummaryEvent, default, eventTags));
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(15, context.TotalChunksPresent);
            Assert.Equal(12, context.TotalChunksIterated);
            Assert.Equal(6000L, context.SumChunksDownloadTimeMs);
            Assert.Equal(120L, context.InitialChunkLatencyMs);
            Assert.Equal(800L, context.SlowestChunkLatencyMs);
        }

        [Fact]
        public void CloudFetchDownloadSummary_ZeroChunkLatency_MergesCorrectly()
        {
            // Arrange - initial_chunk_latency_ms = 0 (fast download)
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-cf-zero");
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 1;
                eventTags["successful_downloads"] = 1;
                eventTags["total_time_ms"] = 50L;
                eventTags["initial_chunk_latency_ms"] = 0L;
                eventTags["slowest_chunk_latency_ms"] = 0L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(0L, context.InitialChunkLatencyMs);
            Assert.Equal(0L, context.SlowestChunkLatencyMs);
        }

        [Fact]
        public void CloudFetchDownloadSummary_BuildsProtoWithChunkMetrics()
        {
            // Arrange - Complete flow with chunk metrics
            var context = new StatementTelemetryContext("session-proto", null, null);

            var downloadActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-proto-chunks");
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 10;
                eventTags["successful_downloads"] = 9;
                eventTags["total_time_ms"] = 4500L;
                eventTags["initial_chunk_latency_ms"] = 90L;
                eventTags["slowest_chunk_latency_ms"] = 450L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            var executeActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-proto-chunks");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, true);
                a.SetTag(StatementExecutionEvent.ResultReadyLatencyMs, 300L);
            });

            context.MergeFrom(downloadActivity);
            context.MergeFrom(executeActivity);

            // Act
            var proto = context.BuildProto();

            // Assert - Proto chunk details populated
            Assert.NotNull(proto.SqlOperation.ChunkDetails);
            Assert.Equal(10, proto.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(9, proto.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(4500L, proto.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
            Assert.Equal(90L, proto.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(450L, proto.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);

            // Result latency
            Assert.Equal(300L, proto.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);

            // Execution result format
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, proto.SqlOperation.ExecutionResult);
            Assert.True(proto.SqlOperation.IsCompressed);
        }

        #endregion

        #region result.format Tag Tests

        [Fact]
        public void ResultFormat_CloudFetch_SetsExternalLinks()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-format-cf");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");
            });

            // Act
            context.MergeFrom(activity);
            var proto = context.BuildProto();

            // Assert
            Assert.Equal("cloudfetch", context.ResultFormat);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, proto.SqlOperation.ExecutionResult);
        }

        [Fact]
        public void ResultFormat_InlineArrow_SetsInlineArrow()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-format-arrow");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultFormat, "inline_arrow");
            });

            // Act
            context.MergeFrom(activity);
            var proto = context.BuildProto();

            // Assert
            Assert.Equal("inline_arrow", context.ResultFormat);
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineArrow, proto.SqlOperation.ExecutionResult);
        }

        #endregion

        #region Poll Metrics Tags Tests

        [Fact]
        public void PollMetrics_WithCountAndLatency_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-poll-metrics");
                a.SetTag(StatementExecutionEvent.PollCount, 7);
                a.SetTag(StatementExecutionEvent.PollLatencyMs, 3500L);
            });

            // Act
            context.MergeFrom(pollActivity);

            // Assert
            Assert.Equal(7, context.PollCount);
            Assert.Equal(3500L, context.PollLatencyMs);
        }

        [Fact]
        public void PollMetrics_BuildsProtoCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();

            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-poll-proto");
                a.SetTag(StatementExecutionEvent.PollCount, 5);
                a.SetTag(StatementExecutionEvent.PollLatencyMs, 2500L);
            });

            var executeActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-poll-proto");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
            });

            context.MergeFrom(pollActivity);
            context.MergeFrom(executeActivity);

            // Act
            var proto = context.BuildProto();

            // Assert
            Assert.Equal(5, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(2500L, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);
        }

        #endregion

        #region Result Latency Tags Tests

        [Fact]
        public void ResultReadyLatency_SetsCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-latency-1");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultReadyLatencyMs, 500L);
                a.SetTag(StatementExecutionEvent.ResultConsumptionLatencyMs, 2000L);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.Equal(500L, context.ResultReadyLatencyMs);
            Assert.Equal(2000L, context.ResultConsumptionLatencyMs);
        }

        [Fact]
        public void ResultReadyLatency_BuildsProtoCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-latency-proto");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultReadyLatencyMs, 750L);
                a.SetTag(StatementExecutionEvent.ResultConsumptionLatencyMs, 3000L);
            });
            context.MergeFrom(activity);

            // Act
            var proto = context.BuildProto();

            // Assert
            Assert.Equal(750L, proto.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
            Assert.Equal(3000L, proto.SqlOperation.ResultLatency.ResultSetConsumptionLatencyMillis);
        }

        #endregion

        #region End-to-End Telemetry Flow Tests

        [Fact]
        public void EndToEnd_QueryWithCloudFetch_AllTagsMergeCorrectly()
        {
            // Arrange - Simulate full ExecuteQuery → Poll → CloudFetch flow
            var context = new StatementTelemetryContext("session-e2e-1");

            // Step 1: Polling activity (child, happens first)
            var pollActivity = CreateAndStopActivity("PollOperationStatus", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-e2e-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-e2e-1");
                a.SetTag(StatementExecutionEvent.PollCount, 4);
                a.SetTag(StatementExecutionEvent.PollLatencyMs, 800L);
            });

            // Step 2: CloudFetch download activity (child)
            var downloadActivity = CreateAndStopActivity("DownloadFiles", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-e2e-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-e2e-1");
                var eventTags = new ActivityTagsCollection();
                eventTags["total_files"] = 8;
                eventTags["successful_downloads"] = 8;
                eventTags["total_time_ms"] = 3000L;
                eventTags["initial_chunk_latency_ms"] = 60L;
                eventTags["slowest_chunk_latency_ms"] = 500L;
                a.AddEvent(new ActivityEvent("cloudfetch.download_summary", default, eventTags));
            });

            // Step 3: Root statement activity
            var executeActivity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-e2e-1");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-e2e-1");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultFormat, "cloudfetch");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, true);
                a.SetTag(StatementExecutionEvent.ResultReadyLatencyMs, 200L);
            });

            // Act - Merge in child-first order
            context.MergeFrom(pollActivity);
            context.MergeFrom(downloadActivity);
            context.MergeFrom(executeActivity);

            // Build proto
            var proto = context.BuildProto();

            // Assert - Complete telemetry log
            Assert.Equal("session-e2e-1", proto.SessionId);
            Assert.Equal("stmt-e2e-1", proto.SqlStatementId);
            Assert.Equal(StatementType.StatementQuery, proto.SqlOperation.StatementType);
            Assert.Equal(ExecutionResultFormat.ExecutionResultExternalLinks, proto.SqlOperation.ExecutionResult);
            Assert.True(proto.SqlOperation.IsCompressed);

            // Poll metrics
            Assert.Equal(4, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(800L, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);

            // Chunk metrics
            Assert.Equal(8, proto.SqlOperation.ChunkDetails.TotalChunksPresent);
            Assert.Equal(8, proto.SqlOperation.ChunkDetails.TotalChunksIterated);
            Assert.Equal(3000L, proto.SqlOperation.ChunkDetails.SumChunksDownloadTimeMillis);
            Assert.Equal(60L, proto.SqlOperation.ChunkDetails.InitialChunkLatencyMillis);
            Assert.Equal(500L, proto.SqlOperation.ChunkDetails.SlowestChunkLatencyMillis);

            // Result latency
            Assert.Equal(200L, proto.SqlOperation.ResultLatency.ResultSetReadyLatencyMillis);
        }

        [Fact]
        public void EndToEnd_MetadataWithInlineResults_AllTagsMergeCorrectly()
        {
            // Arrange - Simulate metadata query with inline results (no CloudFetch, no polling)
            var context = new StatementTelemetryContext("session-meta-e2e");

            var activity = CreateAndStopActivity("GetCatalogs", a =>
            {
                a.SetTag(StatementExecutionEvent.SessionId, "session-meta-e2e");
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-meta-e2e");
                a.SetTag(StatementExecutionEvent.StatementType, "metadata");
                a.SetTag(StatementExecutionEvent.ResultFormat, "inline_arrow");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, false);
            });

            // Act
            context.MergeFrom(activity);
            var proto = context.BuildProto();

            // Assert
            Assert.Equal("session-meta-e2e", proto.SessionId);
            Assert.Equal("stmt-meta-e2e", proto.SqlStatementId);
            Assert.Equal(StatementType.StatementMetadata, proto.SqlOperation.StatementType);
            Assert.Equal(ExecutionResultFormat.ExecutionResultInlineArrow, proto.SqlOperation.ExecutionResult);
            Assert.False(proto.SqlOperation.IsCompressed);

            // No poll metrics (direct results)
            Assert.Equal(0, proto.SqlOperation.OperationDetail.NOperationStatusCalls);
            Assert.Equal(0L, proto.SqlOperation.OperationDetail.OperationStatusLatencyMillis);

            // No chunk details (inline results)
            Assert.Equal(0, proto.SqlOperation.ChunkDetails.TotalChunksPresent);
        }

        #endregion

        #region result.compression_enabled Tests

        [Fact]
        public void ResultCompressionEnabled_True_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-compress-true");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, true);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.True(context.CompressionEnabled);
        }

        [Fact]
        public void ResultCompressionEnabled_False_MergesCorrectly()
        {
            // Arrange
            var context = new StatementTelemetryContext();
            var activity = CreateAndStopActivity("ExecuteQuery", a =>
            {
                a.SetTag(StatementExecutionEvent.StatementId, "stmt-compress-false");
                a.SetTag(StatementExecutionEvent.StatementType, "query");
                a.SetTag(StatementExecutionEvent.ResultCompressionEnabled, false);
            });

            // Act
            context.MergeFrom(activity);

            // Assert
            Assert.False(context.CompressionEnabled);
        }

        #endregion

        #region Helper Methods

        /// <summary>
        /// Creates a real Activity with the given operation name, configures it,
        /// and stops it (populating Duration).
        /// </summary>
        private Activity CreateAndStopActivity(string operationName, Action<Activity>? configure = null)
        {
            var activity = TestSource.StartActivity(operationName);
            if (activity == null)
            {
                throw new InvalidOperationException(
                    $"Failed to create activity '{operationName}'. Ensure ActivityListener is registered.");
            }

            configure?.Invoke(activity);
            activity.Stop();
            return activity;
        }

        #endregion
    }
}
