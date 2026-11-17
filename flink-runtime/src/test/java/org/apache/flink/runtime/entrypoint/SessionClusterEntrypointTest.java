/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStore;
import org.apache.flink.runtime.dispatcher.MemoryArchivedApplicationStore;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SessionClusterEntrypoint}. */
class SessionClusterEntrypointTest {

    @TempDir private Path tempDir;

    @Test
    void testHaArchivedApplicationStoreRequiresHighAvailability() {
        final Configuration configuration = createHaArchivedApplicationStoreConfiguration();

        final TestingSessionClusterEntrypoint entrypoint =
                new TestingSessionClusterEntrypoint(configuration);
        try {
            assertThatThrownBy(
                            () ->
                                    entrypoint.createArchivedApplicationStore(
                                            configuration,
                                            new TestingHighAvailabilityServices(),
                                            new ManuallyTriggeredScheduledExecutor()))
                    .isInstanceOf(IllegalConfigurationException.class)
                    .hasMessageContaining(HighAvailabilityOptions.HA_MODE.key());
        } finally {
            entrypoint.closeAsync().join();
        }
    }

    @Test
    void testHaArchivedApplicationStoreIsCreatedByHaServices() throws Exception {
        final Configuration configuration = createHaArchivedApplicationStoreConfiguration();
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                JobManagerOptions.COMPLETED_APPLICATION_STORE_EXPIRATION_TIME,
                Duration.ofMinutes(42L));
        configuration.set(JobManagerOptions.COMPLETED_APPLICATION_STORE_MAX_CAPACITY, 1337);

        final ArchivedApplicationStore haArchivedApplicationStore =
                new MemoryArchivedApplicationStore();
        final ManuallyTriggeredScheduledExecutor scheduledExecutor =
                new ManuallyTriggeredScheduledExecutor();
        final CompletableFuture<Duration> expirationTimeFuture = new CompletableFuture<>();
        final CompletableFuture<Integer> maximumCapacityFuture = new CompletableFuture<>();
        final CompletableFuture<ScheduledExecutor> scheduledExecutorFuture =
                new CompletableFuture<>();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServices() {
                    @Override
                    public ArchivedApplicationStore createArchivedApplicationStore(
                            Duration expirationTime,
                            int maximumCapacity,
                            ScheduledExecutor scheduledExecutor) {
                        expirationTimeFuture.complete(expirationTime);
                        maximumCapacityFuture.complete(maximumCapacity);
                        scheduledExecutorFuture.complete(scheduledExecutor);
                        return haArchivedApplicationStore;
                    }
                };

        final TestingSessionClusterEntrypoint entrypoint =
                new TestingSessionClusterEntrypoint(configuration);
        try (final ArchivedApplicationStore archivedApplicationStore =
                entrypoint.createArchivedApplicationStore(
                        configuration, haServices, scheduledExecutor)) {
            assertThat(archivedApplicationStore).isSameAs(haArchivedApplicationStore);
            assertThat(expirationTimeFuture).isCompletedWithValue(Duration.ofMinutes(42L));
            assertThat(maximumCapacityFuture).isCompletedWithValue(1337);
            assertThat(scheduledExecutorFuture).isCompletedWithValue(scheduledExecutor);
        } finally {
            entrypoint.closeAsync().join();
        }
    }

    private Configuration createHaArchivedApplicationStoreConfiguration() {
        final Configuration configuration = new Configuration();
        // closing the entrypoint deletes the web tmp directory
        configuration.set(WebOptions.TMP_DIR, tempDir.resolve("web").toString());
        configuration.set(
                JobManagerOptions.COMPLETED_APPLICATION_STORE_TYPE,
                JobManagerOptions.ArchivedApplicationStoreType.HA);
        return configuration;
    }

    private static class TestingSessionClusterEntrypoint extends SessionClusterEntrypoint {

        private TestingSessionClusterEntrypoint(Configuration configuration) {
            super(configuration);
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration) {
            throw new UnsupportedOperationException();
        }
    }
}
