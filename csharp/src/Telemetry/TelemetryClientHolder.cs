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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Holds a telemetry client and its reference count.
    /// Used by TelemetryClientManager for per-host client lifecycle management.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Reference counting tracks how many connections are using this client.
    /// When the reference count reaches zero, the client is closed and removed.
    /// The _refCount field starts at 1 (for the initial connection that created it)
    /// and is incremented/decremented using <see cref="System.Threading.Interlocked"/>.
    /// </para>
    /// <para>
    /// JDBC Reference: TelemetryClientHolder.java:5
    /// </para>
    /// </remarks>
    internal sealed class TelemetryClientHolder
    {
        /// <summary>
        /// The reference count tracking how many connections are using this client.
        /// Starts at 1 for the initial connection. Modified via Interlocked operations.
        /// </summary>
        internal int _refCount = 1;

        /// <summary>
        /// Gets the telemetry client held by this holder.
        /// </summary>
        public ITelemetryClient Client { get; }

        /// <summary>
        /// Creates a new <see cref="TelemetryClientHolder"/>.
        /// </summary>
        /// <param name="client">The telemetry client to hold.</param>
        /// <exception cref="ArgumentNullException">Thrown when client is null.</exception>
        public TelemetryClientHolder(ITelemetryClient client)
        {
            Client = client ?? throw new ArgumentNullException(nameof(client));
        }
    }
}
