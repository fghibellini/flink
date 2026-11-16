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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.jobmanager.ZooKeeperApplicationStoreUtil;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateTerminalArchivedApplications;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the {@link StateHandleArchivedApplicationStore}. */
class StateHandleArchivedApplicationStoreTest {

    private final Map<String, ArchivedApplication> persistedApplications = new HashMap<>();

    private final TestingStateHandleStore.Builder<ArchivedApplication> stateHandleStoreBuilder =
            TestingStateHandleStore.<ArchivedApplication>newBuilder()
                    .setAddFunction(
                            (name, application) -> {
                                if (persistedApplications.putIfAbsent(name, application) != null) {
                                    throw new StateHandleStore.AlreadyExistException(name);
                                }
                                return null;
                            })
                    .setRemoveFunction(name -> persistedApplications.remove(name) != null);

    @Test
    void testPutPersistsApplication() throws IOException {
        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.put(archivedApplication);

            assertThat(store.get(archivedApplication.getApplicationId()))
                    .containsSame(archivedApplication);
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(archivedApplication), archivedApplication));
        }
    }

    @Test
    void testPutKeepsExistingPersistedApplication() throws IOException {
        final ArchivedApplication persistedApplication =
                generateTerminalArchivedApplications(1).iterator().next();
        final ArchivedApplication sparseApplication =
                new ArchivedApplication(
                        persistedApplication.getApplicationId(),
                        persistedApplication.getApplicationName(),
                        persistedApplication.getApplicationStatus(),
                        new long[0],
                        Collections.emptyMap(),
                        Collections.emptyList());
        persistedApplications.put(nameOf(persistedApplication), persistedApplication);

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.put(sparseApplication);

            assertThat(store.get(sparseApplication.getApplicationId()))
                    .containsSame(sparseApplication);
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(persistedApplication), persistedApplication));
        }
    }

    @Test
    void testPutFailureIsReportedButApplicationIsStillServed() throws IOException {
        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();
        final FlinkException testException = new FlinkException("Test exception.");
        final StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore =
                stateHandleStoreBuilder
                        .setAddFunction(
                                (name, application) -> {
                                    throw testException;
                                })
                        .build();

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStore, Integer.MAX_VALUE)) {
            assertThatThrownBy(() -> store.put(archivedApplication))
                    .isInstanceOf(IOException.class)
                    .hasCause(testException);

            assertThat(store.get(archivedApplication.getApplicationId()))
                    .containsSame(archivedApplication);
        }
    }

    @Test
    void testEvictedApplicationIsRemovedFromStateHandleStore() throws IOException {
        final Iterator<ArchivedApplication> archivedApplications =
                generateTerminalArchivedApplications(2).iterator();
        final ArchivedApplication evictedApplication = archivedApplications.next();
        final ArchivedApplication retainedApplication = archivedApplications.next();

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), 1)) {
            store.put(evictedApplication);
            store.put(retainedApplication);

            assertThat(store.get(evictedApplication.getApplicationId())).isEmpty();
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(retainedApplication), retainedApplication));
        }
    }

    @Test
    void testCloseReleasesLocksButKeepsPersistedApplications() throws IOException {
        final ArchivedApplication archivedApplication =
                generateTerminalArchivedApplications(1).iterator().next();
        final AtomicBoolean locksReleased = new AtomicBoolean(false);
        final ArchivedApplicationStore store =
                createStore(
                        stateHandleStoreBuilder
                                .setReleaseAllHandlesRunnable(() -> locksReleased.set(true))
                                .build(),
                        Integer.MAX_VALUE);

        store.put(archivedApplication);
        store.close();

        assertThat(locksReleased).isTrue();
        assertThat(persistedApplications)
                .containsOnly(entry(nameOf(archivedApplication), archivedApplication));
    }

    private static ArchivedApplicationStore createStore(
            StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore,
            int maximumCapacity) {
        return new StateHandleArchivedApplicationStore<>(
                stateHandleStore,
                ZooKeeperApplicationStoreUtil.INSTANCE,
                Duration.ofHours(1),
                maximumCapacity,
                null,
                null);
    }

    private static String nameOf(ArchivedApplication archivedApplication) {
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        return ZooKeeperApplicationStoreUtil.INSTANCE.applicationIdToName(applicationId);
    }
}
