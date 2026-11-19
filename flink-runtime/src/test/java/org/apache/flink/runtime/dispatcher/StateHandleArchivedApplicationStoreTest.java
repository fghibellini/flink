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
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreTestUtils.generateTerminalArchivedApplications;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for the {@link StateHandleArchivedApplicationStore}. */
class StateHandleArchivedApplicationStoreTest {

    private static final Duration EXPIRATION_TIME = Duration.ofHours(1);

    private final ManualClock clock = new ManualClock(EXPIRATION_TIME.toMillis() * 10);

    private final Map<String, ArchivedApplication> persistedApplications = new HashMap<>();

    private final Set<String> lockedNames = new HashSet<>();

    private final TestingRetrievableStateStorageHelper<ArchivedApplication> storageHelper =
            new TestingRetrievableStateStorageHelper<>();

    private final TestingStateHandleStore.Builder<ArchivedApplication> stateHandleStoreBuilder =
            TestingStateHandleStore.<ArchivedApplication>newBuilder()
                    .setAddFunction(
                            (name, application) -> {
                                if (persistedApplications.putIfAbsent(name, application) != null) {
                                    throw new StateHandleStore.AlreadyExistException(name);
                                }
                                lockedNames.add(name);
                                return null;
                            })
                    .setRemoveFunction(
                            name -> {
                                lockedNames.remove(name);
                                return persistedApplications.remove(name) != null;
                            })
                    .setGetAllSupplier(
                            () ->
                                    persistedApplications.values().stream()
                                            .map(this::lockAndRetrieveHandle)
                                            .collect(Collectors.toList()))
                    .setReleaseConsumer(lockedNames::remove)
                    .setReleaseAllHandlesRunnable(lockedNames::clear);

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
    void testCloseReleasesLocksButKeepsPersistedApplications() throws Exception {
        final ArchivedApplication recoveredApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        final ArchivedApplication putApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        persist(recoveredApplication);
        final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE);

        store.recover();
        store.put(putApplication);

        assertThat(lockedNames)
                .containsExactlyInAnyOrder(nameOf(recoveredApplication), nameOf(putApplication));

        store.close();

        assertThat(lockedNames).isEmpty();
        assertThat(persistedApplications)
                .containsOnlyKeys(nameOf(recoveredApplication), nameOf(putApplication));
    }

    @Test
    void testRecoverLoadsPersistedApplicationsAndKeepsTheirLocks() throws Exception {
        final ArchivedApplication firstApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        final ArchivedApplication secondApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        persist(firstApplication, secondApplication);

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.recover();

            assertThat(store.size()).isEqualTo(2);
            assertThat(store.get(firstApplication.getApplicationId()))
                    .containsSame(firstApplication);
            assertThat(store.get(secondApplication.getApplicationId()))
                    .containsSame(secondApplication);
            assertThat(lockedNames)
                    .containsExactlyInAnyOrder(nameOf(firstApplication), nameOf(secondApplication));
            assertThat(persistedApplications).hasSize(2);
        }
    }

    @Test
    void testRecoverSkipsUnreadableApplicationsAndReleasesTheirLocks() throws Exception {
        final ArchivedApplication unreadableApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        final ArchivedApplication readableApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        persist(unreadableApplication, readableApplication);
        storageHelper.setRetrieveStateFunction(
                application -> {
                    if (application == unreadableApplication) {
                        throw new IOException("Test exception.");
                    }
                    return application;
                });

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.recover();

            assertThat(store.size()).isOne();
            assertThat(store.get(readableApplication.getApplicationId()))
                    .containsSame(readableApplication);
            assertThat(lockedNames).containsExactly(nameOf(readableApplication));
            assertThat(persistedApplications).hasSize(2);
        }
    }

    @Test
    void testRecoverRemovesExpiredApplications() throws Exception {
        final long expirationTimeMillis = EXPIRATION_TIME.toMillis();
        final ArchivedApplication expiredApplication =
                createArchivedApplication(clock.absoluteTimeMillis() - expirationTimeMillis);
        final ArchivedApplication unexpiredApplication =
                createArchivedApplication(clock.absoluteTimeMillis() - expirationTimeMillis + 1);
        persist(expiredApplication, unexpiredApplication);

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.recover();

            assertThat(store.get(expiredApplication.getApplicationId())).isEmpty();
            assertThat(store.get(unexpiredApplication.getApplicationId()))
                    .containsSame(unexpiredApplication);
            assertThat(lockedNames).containsExactly(nameOf(unexpiredApplication));
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(unexpiredApplication), unexpiredApplication));
        }
    }

    @Test
    void testRecoverKeepsNewestApplicationsIfMaximumCapacityIsExceeded() throws Exception {
        final ArchivedApplication newerApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        final ArchivedApplication olderApplication =
                createArchivedApplication(clock.absoluteTimeMillis() - 1);
        persist(newerApplication, olderApplication);
        final StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore =
                stateHandleStoreBuilder
                        .setGetAllSupplier(
                                () ->
                                        Arrays.asList(
                                                lockAndRetrieveHandle(newerApplication),
                                                lockAndRetrieveHandle(olderApplication)))
                        .build();

        try (final ArchivedApplicationStore store = createStore(stateHandleStore, 1)) {
            store.recover();

            assertThat(store.get(olderApplication.getApplicationId())).isEmpty();
            assertThat(store.get(newerApplication.getApplicationId()))
                    .containsSame(newerApplication);
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(newerApplication), newerApplication));
        }
    }

    @Test
    void testRecoverFailsIfPersistedApplicationsCannotBeListed() throws IOException {
        final FlinkException testException = new FlinkException("Test exception.");
        final StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore =
                stateHandleStoreBuilder
                        .setGetAllSupplier(
                                () -> {
                                    throw testException;
                                })
                        .build();

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStore, Integer.MAX_VALUE)) {
            assertThatThrownBy(store::recover).isSameAs(testException);
        }
    }

    @Test
    void testRecoverFailsAfterClose() throws Exception {
        persist(createArchivedApplication(clock.absoluteTimeMillis()));
        final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE);

        store.close();

        assertThatThrownBy(store::recover).isInstanceOf(IllegalStateException.class);
        assertThat(lockedNames).isEmpty();
    }

    @Test
    void testPutKeepsRecoveredApplication() throws Exception {
        final ArchivedApplication recoveredApplication =
                createArchivedApplication(clock.absoluteTimeMillis());
        final ArchivedApplication sparseApplication =
                new ArchivedApplication(
                        recoveredApplication.getApplicationId(),
                        recoveredApplication.getApplicationName(),
                        recoveredApplication.getApplicationStatus(),
                        new long[ApplicationState.values().length],
                        Collections.emptyMap(),
                        Collections.emptyList());
        persist(recoveredApplication);

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.recover();

            store.put(sparseApplication);

            assertThat(store.get(recoveredApplication.getApplicationId()))
                    .containsSame(recoveredApplication);
            assertThat(persistedApplications)
                    .containsOnly(entry(nameOf(recoveredApplication), recoveredApplication));
        }
    }

    private ArchivedApplicationStore createStore(
            StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore,
            int maximumCapacity) {
        return new StateHandleArchivedApplicationStore<>(
                stateHandleStore,
                ZooKeeperArchivedApplicationStoreUtil.INSTANCE,
                EXPIRATION_TIME,
                maximumCapacity,
                null,
                null,
                clock);
    }

    private void persist(ArchivedApplication... archivedApplications) {
        for (ArchivedApplication archivedApplication : archivedApplications) {
            persistedApplications.put(nameOf(archivedApplication), archivedApplication);
        }
    }

    private static ArchivedApplication createArchivedApplication(long endTime) {
        final long[] statusTimestamps = new long[ApplicationState.values().length];
        statusTimestamps[ApplicationState.FINISHED.ordinal()] = endTime;
        return new ArchivedApplication(
                ApplicationID.generate(),
                "test-application",
                ApplicationState.FINISHED,
                statusTimestamps,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private Tuple2<RetrievableStateHandle<ArchivedApplication>, String> lockAndRetrieveHandle(
            ArchivedApplication archivedApplication) {
        final String name = nameOf(archivedApplication);
        lockedNames.add(name);
        return Tuple2.of(storageHelper.store(archivedApplication), name);
    }

    private static String nameOf(ArchivedApplication archivedApplication) {
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        return ZooKeeperArchivedApplicationStoreUtil.INSTANCE.applicationIdToName(applicationId);
    }
}
