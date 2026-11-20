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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStore;
import org.apache.flink.runtime.dispatcher.ArchivedApplicationStoreFactory;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.MemoryArchivedApplicationStore;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link ArchivedApplicationStore} which serves the {@link ArchivedApplication} instances from
 * memory (see {@link MemoryArchivedApplicationStore}) and additionally persists every stored
 * application in a {@link StateHandleStore}.
 *
 * <p>A store is meant to be used during a single leadership session (see {@link
 * ArchivedApplicationStoreFactory}). {@link #recover()} loads the persisted applications into
 * memory and, like {@link #put(ArchivedApplication)}, locks their entries until the store is
 * closed. Applications which already expired are removed instead. Closing the store releases all
 * locks which it holds but keeps the persisted entries, so that the next leader can recover them.
 *
 * <p>If an application is already stored, in memory or in the {@link StateHandleStore}, the
 * existing entry is kept. This prevents e.g. the sparse {@link ArchivedApplication} which the
 * {@link Dispatcher} creates when recovering a dirty application result from overwriting the
 * complete one that was persisted before the failover.
 *
 * <p>Applications which are evicted from memory, because they expired or because the maximum
 * capacity was exceeded, are removed from the {@link StateHandleStore} as well.
 *
 * <p>The names of the entries in the {@link StateHandleStore} are derived from the application ids
 * by the given {@link ArchivedApplicationStoreUtil}.
 *
 * @param <R> type of the resource version of the {@link StateHandleStore}
 */
public class StateHandleArchivedApplicationStore<R extends ResourceVersion<R>>
        implements ArchivedApplicationStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(StateHandleArchivedApplicationStore.class);

    /**
     * Guards the {@link #stateHandleStore} which is accessed by puts and the recovery as well as by
     * evictions, which can be triggered by the periodic cleanup.
     */
    private final Object lock = new Object();

    /** Prevents a recovery which races with {@link #close()} from locking the persisted entries. */
    @GuardedBy("lock")
    private boolean running = true;

    private final StateHandleStore<ArchivedApplication, R> stateHandleStore;

    private final ArchivedApplicationStoreUtil archivedApplicationStoreUtil;

    private final MemoryArchivedApplicationStore memoryStore;

    private final Duration expirationTime;

    private final Clock clock;

    /**
     * Creates the store.
     *
     * @param clock used to decide whether a recovered application already expired, based on its end
     *     time
     */
    public StateHandleArchivedApplicationStore(
            StateHandleStore<ArchivedApplication, R> stateHandleStore,
            ArchivedApplicationStoreUtil archivedApplicationStoreUtil,
            Duration expirationTime,
            int maximumCapacity,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable Ticker ticker,
            Clock clock) {
        this.stateHandleStore = checkNotNull(stateHandleStore);
        this.archivedApplicationStoreUtil = checkNotNull(archivedApplicationStoreUtil);
        this.expirationTime = checkNotNull(expirationTime);
        this.clock = checkNotNull(clock);

        LOG.info(
                "Initializing {}: State handle store {}, expiration time {}, maximum capacity {}.",
                StateHandleArchivedApplicationStore.class.getSimpleName(),
                stateHandleStore,
                expirationTime,
                maximumCapacity);

        this.memoryStore =
                new MemoryArchivedApplicationStore(
                        expirationTime,
                        maximumCapacity,
                        scheduledExecutor,
                        ticker,
                        this::removePersistedApplication);
    }

    @Override
    public int size() {
        return memoryStore.size();
    }

    @Override
    public void recover() throws Exception {
        synchronized (lock) {
            checkState(running, "The store has already been closed.");

            LOG.info("Recovering archived applications from {}.", stateHandleStore);

            final List<Tuple2<RetrievableStateHandle<ArchivedApplication>, String>> handles =
                    stateHandleStore.getAllAndLock();

            final List<ArchivedApplication> recoveredApplications = new ArrayList<>(handles.size());
            int numExpired = 0;
            int numFailed = 0;

            for (Tuple2<RetrievableStateHandle<ArchivedApplication>, String> handle : handles) {
                final String name = handle.f1;
                final ArchivedApplication archivedApplication;
                try {
                    archivedApplication = handle.f0.retrieveState();
                } catch (Exception e) {
                    numFailed++;
                    LOG.warn(
                            "Could not recover archived application {} from {}. Skipping it.",
                            name,
                            stateHandleStore,
                            e);
                    releaseQuietly(name);
                    continue;
                }

                if (isExpired(archivedApplication)) {
                    numExpired++;
                    removePersistedApplication(archivedApplication.getApplicationId());
                } else {
                    recoveredApplications.add(archivedApplication);
                }
            }

            // the oldest applications are put first, so that they are the first ones to be
            // evicted if the maximum capacity is exceeded
            recoveredApplications.sort(
                    Comparator.comparingLong(StateHandleArchivedApplicationStore::getEndTime));
            for (ArchivedApplication archivedApplication : recoveredApplications) {
                memoryStore.put(archivedApplication);
            }

            LOG.info(
                    "Recovered {} archived applications from {}. Removed {} expired and skipped {} unreadable archived applications.",
                    recoveredApplications.size(),
                    stateHandleStore,
                    numExpired,
                    numFailed);
        }
    }

    @Override
    public Optional<ArchivedApplication> get(ApplicationID applicationId) {
        return memoryStore.get(applicationId);
    }

    @Override
    public void put(ArchivedApplication archivedApplication) throws IOException {
        final ApplicationID applicationId = archivedApplication.getApplicationId();

        if (memoryStore.get(applicationId).isPresent()) {
            LOG.info(
                    "Archived application {} is already stored. Keeping the existing entry.",
                    applicationId);
            return;
        }
        memoryStore.put(archivedApplication);

        final String name = archivedApplicationStoreUtil.applicationIdToName(applicationId);

        synchronized (lock) {
            try {
                stateHandleStore.addAndLock(name, archivedApplication);
                LOG.debug(
                        "Persisted archived application {} in {}.",
                        applicationId,
                        stateHandleStore);
            } catch (StateHandleStore.AlreadyExistException e) {
                LOG.info(
                        "Archived application {} already exists in {}. Keeping the existing entry.",
                        applicationId,
                        stateHandleStore);
            } catch (Exception e) {
                throw new IOException(
                        String.format(
                                "Could not persist archived application %s in %s.",
                                applicationId, stateHandleStore),
                        e);
            }
        }
    }

    @Override
    public Collection<ApplicationDetails> getApplicationDetails() {
        return memoryStore.getApplicationDetails();
    }

    @Override
    public Optional<ExecutionGraphInfo> getExecutionGraphInfo(JobID jobId) {
        return memoryStore.getExecutionGraphInfo(jobId);
    }

    @Override
    public Collection<JobDetails> getJobDetails() {
        return memoryStore.getJobDetails();
    }

    @Override
    public JobsOverview getJobsOverview() {
        return memoryStore.getJobsOverview();
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            running = false;

            memoryStore.close();

            try {
                stateHandleStore.releaseAll();
            } catch (Exception e) {
                throw new IOException(
                        "Could not release the locks of the archived applications in "
                                + stateHandleStore
                                + '.',
                        e);
            }
        }
    }

    private void releaseQuietly(String name) {
        try {
            stateHandleStore.release(name);
        } catch (Exception e) {
            LOG.warn(
                    "Could not release the lock of archived application {} in {}.",
                    name,
                    stateHandleStore,
                    e);
        }
    }

    private boolean isExpired(ArchivedApplication archivedApplication) {
        final long expirationMillis = expirationTime.toMillis();
        return expirationMillis > 0
                && clock.absoluteTimeMillis() - getEndTime(archivedApplication) >= expirationMillis;
    }

    private static long getEndTime(ArchivedApplication archivedApplication) {
        return archivedApplication.getStatusTimestamp(archivedApplication.getApplicationStatus());
    }

    private void removePersistedApplication(ApplicationID applicationId) {
        final String name = archivedApplicationStoreUtil.applicationIdToName(applicationId);

        synchronized (lock) {
            try {
                if (stateHandleStore.releaseAndTryRemove(name)) {
                    LOG.debug(
                            "Removed evicted archived application {} from {}.",
                            applicationId,
                            stateHandleStore);
                } else {
                    LOG.warn(
                            "Could not remove evicted archived application {} from {} because it is still locked.",
                            applicationId,
                            stateHandleStore);
                }
            } catch (Exception e) {
                LOG.warn(
                        "Could not remove evicted archived application {} from {}.",
                        applicationId,
                        stateHandleStore,
                        e);
            }
        }
    }
}
