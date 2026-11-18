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
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.TestingRetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final List<String> retrievedNames = new ArrayList<>();

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
                                return null;
                            })
                    .setRemoveFunction(name -> persistedApplications.remove(name) != null)
                    // ZooKeeper lists the child nodes, i.e. the names without the leading slash
                    .setGetAllHandlesSupplier(
                            () ->
                                    persistedApplications.keySet().stream()
                                            .map(ZooKeeperUtils::trimStartingSlash)
                                            .collect(Collectors.toList()))
                    .setGetFunction(
                            name -> {
                                retrievedNames.add(name);
                                final ArchivedApplication application =
                                        persistedApplications.get(name);
                                if (application == null) {
                                    throw new StateHandleStore.NotExistException(name);
                                }
                                lockedNames.add(name);
                                return storageHelper.store(application);
                            })
                    .setReleaseConsumer(lockedNames::remove);

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

    @Test
    void testRecoverLoadsPersistedApplicationsAndReleasesTheirLocks() throws Exception {
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
            assertThat(retrievedNames)
                    .containsExactlyInAnyOrder(nameOf(firstApplication), nameOf(secondApplication));
            assertThat(lockedNames).isEmpty();
            assertThat(persistedApplications).hasSize(2);
        }
    }

    @Test
    void testRecoverSkipsApplicationsWhichAreAlreadyStored() throws Exception {
        final ArchivedApplication storedApplication =
                createArchivedApplication(clock.absoluteTimeMillis());

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStoreBuilder.build(), Integer.MAX_VALUE)) {
            store.put(storedApplication);

            store.recover();

            assertThat(retrievedNames).isEmpty();
            assertThat(store.get(storedApplication.getApplicationId()))
                    .containsSame(storedApplication);
        }
    }

    @Test
    void testRecoverSkipsUnreadableApplications() throws Exception {
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
        final StateHandleStore<ArchivedApplication, IntegerResourceVersion> stateHandleStore =
                stateHandleStoreBuilder
                        .setGetAllHandlesSupplier(
                                () ->
                                        Arrays.asList(
                                                "malformed-name",
                                                trimmedNameOf(unreadableApplication),
                                                trimmedNameOf(readableApplication)))
                        .build();

        try (final ArchivedApplicationStore store =
                createStore(stateHandleStore, Integer.MAX_VALUE)) {
            store.recover();

            assertThat(store.size()).isOne();
            assertThat(store.get(readableApplication.getApplicationId()))
                    .containsSame(readableApplication);
            assertThat(lockedNames).isEmpty();
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
                        .setGetAllHandlesSupplier(
                                () ->
                                        Arrays.asList(
                                                trimmedNameOf(newerApplication),
                                                trimmedNameOf(olderApplication)))
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
                        .setGetAllHandlesSupplier(
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

    private static String trimmedNameOf(ArchivedApplication archivedApplication) {
        return ZooKeeperUtils.trimStartingSlash(nameOf(archivedApplication));
    }

    private static String nameOf(ArchivedApplication archivedApplication) {
        final ApplicationID applicationId = archivedApplication.getApplicationId();
        return ZooKeeperArchivedApplicationStoreUtil.INSTANCE.applicationIdToName(applicationId);
    }
}
