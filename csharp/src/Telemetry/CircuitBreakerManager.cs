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

namespace AdbcDrivers.Databricks.Telemetry
{
    /// <summary>
    /// Singleton that manages circuit breakers per host.
    /// Each host gets its own CircuitBreaker instance so that failures
    /// on one host do not affect other hosts.
    /// </summary>
    internal sealed class CircuitBreakerManager
    {
        private static readonly CircuitBreakerManager Instance = new CircuitBreakerManager();

        private readonly ConcurrentDictionary<string, CircuitBreaker> _breakers =
            new ConcurrentDictionary<string, CircuitBreaker>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Private constructor to enforce singleton pattern.
        /// </summary>
        private CircuitBreakerManager()
        {
        }

        /// <summary>
        /// Gets the singleton instance of <see cref="CircuitBreakerManager"/>.
        /// </summary>
        /// <returns>The singleton instance.</returns>
        public static CircuitBreakerManager GetInstance() => Instance;

        /// <summary>
        /// Gets or creates a circuit breaker for the specified host.
        /// Returns the same instance for repeated calls with the same host.
        /// </summary>
        /// <param name="host">The host to get a circuit breaker for.</param>
        /// <returns>The circuit breaker instance for the host.</returns>
        /// <exception cref="ArgumentNullException">Thrown when host is null.</exception>
        /// <exception cref="ArgumentException">Thrown when host is empty or whitespace.</exception>
        public CircuitBreaker GetCircuitBreaker(string host)
        {
            if (host == null)
            {
                throw new ArgumentNullException(nameof(host));
            }

            if (string.IsNullOrWhiteSpace(host))
            {
                throw new ArgumentException("Host cannot be empty or whitespace.", nameof(host));
            }

            return _breakers.GetOrAdd(host, _ => new CircuitBreaker());
        }

        /// <summary>
        /// Resets the manager by clearing all circuit breakers.
        /// This is primarily for testing purposes.
        /// </summary>
        internal void Reset()
        {
            _breakers.Clear();
        }
    }
}
