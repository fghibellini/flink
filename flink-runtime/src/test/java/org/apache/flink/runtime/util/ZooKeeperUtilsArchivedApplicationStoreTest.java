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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStore;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.rest.util.NoOpFatalErrorHandler;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ZooKeeperUtils#createArchivedApplicationStore}. */
class ZooKeeperUtilsArchivedApplicationStoreTest {

    private final ZooKeeperExtension zooKeeperExtension = new ZooKeeperExtension();

    @RegisterExtension
    private final EachCallbackWrapper<ZooKeeperExtension> eachWrapper =
            new EachCallbackWrapper<>(zooKeeperExtension);

    @TempDir private Path tempDir;

    @Test
    void testArchivedApplicationStoreIsCreatedFromConfiguration() throws Exception {
        final String archivedApplicationsPath = "/custom-archived-applications";
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, tempDir.toUri().toString());
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_ARCHIVED_APPLICATIONS_PATH,
                archivedApplicationsPath);
        configuration.set(JobManagerOptions.COMPLETED_APPLICATION_STORE_MAX_CAPACITY, 1);

        final ArchivedApplication firstApplication = createArchivedApplication();
        final ArchivedApplication secondApplication = createArchivedApplication();
        final String firstZNode =
                archivedApplicationsPath + "/" + firstApplication.getApplicationId();
        final String secondZNode =
                archivedApplicationsPath + "/" + secondApplication.getApplicationId();

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        try {
            final CuratorFramework client = curatorFrameworkWrapper.asCuratorFramework();

            try (final ArchivedApplicationStore archivedApplicationStore =
                    ZooKeeperUtils.createArchivedApplicationStore(
                            client, configuration, new ManuallyTriggeredScheduledExecutor())) {
                archivedApplicationStore.put(firstApplication);
                assertThat(client.checkExists().forPath(firstZNode)).isNotNull();

                // exceeds the configured maximum capacity, which evicts the first application
                archivedApplicationStore.put(secondApplication);
                assertThat(client.checkExists().forPath(firstZNode)).isNull();
                assertThat(client.checkExists().forPath(secondZNode)).isNotNull();
            }

            // closing the store only releases the locks but keeps the persisted entries
            assertThat(client.checkExists().forPath(secondZNode)).isNotNull();
        } finally {
            curatorFrameworkWrapper.close();
        }
    }

    @Test
    void testPersistedApplicationsAreRecoveredAndLockedUntilClose() throws Exception {
        final String archivedApplicationsPath = "/archived-applications";
        final Configuration configuration = new Configuration();
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperExtension.getConnectString());
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, tempDir.toUri().toString());
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_ARCHIVED_APPLICATIONS_PATH,
                archivedApplicationsPath);

        final ArchivedApplication archivedApplication = createArchivedApplication();
        final String lockZNodes =
                archivedApplicationsPath + "/" + archivedApplication.getApplicationId() + "/locks";

        final CuratorFrameworkWithUnhandledErrorListener curatorFrameworkWrapper =
                ZooKeeperUtils.startCuratorFramework(configuration, NoOpFatalErrorHandler.INSTANCE);
        try {
            final CuratorFramework client = curatorFrameworkWrapper.asCuratorFramework();

            try (final ArchivedApplicationStore archivedApplicationStore =
                    ZooKeeperUtils.createArchivedApplicationStore(
                            client, configuration, new ManuallyTriggeredScheduledExecutor())) {
                archivedApplicationStore.put(archivedApplication);
                assertThat(client.getChildren().forPath(lockZNodes)).hasSize(1);
            }
            assertThat(client.getChildren().forPath(lockZNodes)).isEmpty();

            try (final ArchivedApplicationStore archivedApplicationStore =
                    ZooKeeperUtils.createArchivedApplicationStore(
                            client, configuration, new ManuallyTriggeredScheduledExecutor())) {
                archivedApplicationStore.recover();

                assertThat(archivedApplicationStore.get(archivedApplication.getApplicationId()))
                        .contains(archivedApplication);
                assertThat(client.getChildren().forPath(lockZNodes)).hasSize(1);
            }
            assertThat(client.getChildren().forPath(lockZNodes)).isEmpty();
        } finally {
            curatorFrameworkWrapper.close();
        }
    }

    private static ArchivedApplication createArchivedApplication() {
        final long[] statusTimestamps = new long[ApplicationState.values().length];
        // recovered applications which ended longer than the expiration time ago are discarded
        statusTimestamps[ApplicationState.FINISHED.ordinal()] = System.currentTimeMillis();
        return new ArchivedApplication(
                ApplicationID.generate(),
                "test-application",
                ApplicationState.FINISHED,
                statusTimestamps,
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
