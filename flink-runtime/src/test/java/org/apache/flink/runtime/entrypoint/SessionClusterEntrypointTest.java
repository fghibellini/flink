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
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreFactory;
import org.apache.flink.runtime.dispatcher.MemoryArchivedApplicationStore;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
                                    entrypoint.createArchivedApplicationStoreFactory(
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
    void testHaArchivedApplicationStoresAreCreatedByHaServicesPerLeadershipSession()
            throws Exception {
        final Configuration configuration = createHaArchivedApplicationStoreConfiguration();
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");

        final List<ArchivedApplicationStore> haArchivedApplicationStores = new ArrayList<>();
        final TestingHighAvailabilityServices haServices =
                new TestingHighAvailabilityServices() {
                    @Override
                    public ArchivedApplicationStore createArchivedApplicationStore() {
                        final ArchivedApplicationStore archivedApplicationStore =
                                new MemoryArchivedApplicationStore();
                        haArchivedApplicationStores.add(archivedApplicationStore);
                        return archivedApplicationStore;
                    }
                };

        final TestingSessionClusterEntrypoint entrypoint =
                new TestingSessionClusterEntrypoint(configuration);
        try (final ArchivedApplicationStoreFactory archivedApplicationStoreFactory =
                entrypoint.createArchivedApplicationStoreFactory(
                        configuration, haServices, new ManuallyTriggeredScheduledExecutor())) {
            assertThat(haArchivedApplicationStores).isEmpty();

            final ArchivedApplicationStore firstArchivedApplicationStore =
                    archivedApplicationStoreFactory.createArchivedApplicationStore();
            final ArchivedApplicationStore secondArchivedApplicationStore =
                    archivedApplicationStoreFactory.createArchivedApplicationStore();

            assertThat(haArchivedApplicationStores)
                    .containsExactly(firstArchivedApplicationStore, secondArchivedApplicationStore);
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
