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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStore;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KubernetesUtils#createArchivedApplicationStore}. */
class KubernetesArchivedApplicationStoreTest extends KubernetesHighAvailabilityTestBase {

    @TempDir private Path tempDir;

    @Test
    void testArchivedApplicationStoreIsCreatedFromConfiguration() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            leaderCallbackGrantLeadership();
                            configuration.set(
                                    HighAvailabilityOptions.HA_STORAGE_PATH,
                                    tempDir.toUri().toString());
                            configuration.set(
                                    JobManagerOptions.COMPLETED_APPLICATION_STORE_MAX_CAPACITY, 1);

                            final ArchivedApplication firstApplication =
                                    createArchivedApplication();
                            final ArchivedApplication secondApplication =
                                    createArchivedApplication();
                            final String firstKey =
                                    Constants.ARCHIVED_APPLICATION_STORE_KEY_PREFIX
                                            + firstApplication.getApplicationId();
                            final String secondKey =
                                    Constants.ARCHIVED_APPLICATION_STORE_KEY_PREFIX
                                            + secondApplication.getApplicationId();

                            try (final ArchivedApplicationStore archivedApplicationStore =
                                    KubernetesUtils.createArchivedApplicationStore(
                                            configuration,
                                            flinkKubeClient,
                                            LEADER_CONFIGMAP_NAME,
                                            LOCK_IDENTITY,
                                            new ManuallyTriggeredScheduledExecutor())) {
                                archivedApplicationStore.put(firstApplication);
                                assertThat(getLeaderConfigMap().getData()).containsKey(firstKey);

                                // exceeds the configured maximum capacity, which evicts the first
                                // application
                                archivedApplicationStore.put(secondApplication);
                                assertThat(getLeaderConfigMap().getData())
                                        .doesNotContainKey(firstKey)
                                        .containsKey(secondKey);
                            }

                            // closing the store only releases the locks but keeps the entries
                            assertThat(getLeaderConfigMap().getData()).containsKey(secondKey);
                        });
            }
        };
    }

    private static ArchivedApplication createArchivedApplication() {
        return new ArchivedApplication(
                ApplicationID.generate(),
                "test-application",
                ApplicationState.FINISHED,
                new long[ApplicationState.values().length],
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
