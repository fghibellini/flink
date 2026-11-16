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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.jobmanager.ApplicationStoreUtil;
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.persistence.ResourceVersion;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava33.com.google.common.base.Ticker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ArchivedApplicationStore} which serves the {@link ArchivedApplication} instances from
 * memory (see {@link MemoryArchivedApplicationStore}) and additionally persists every stored
 * application in a {@link StateHandleStore}.
 *
 * <p>If an entry for an application already exists in the {@link StateHandleStore}, the existing
 * entry is kept. This prevents e.g. the sparse {@link ArchivedApplication} which the {@link
 * Dispatcher} creates when recovering a dirty application result from overwriting the complete one
 * that was persisted before the failover.
 *
 * <p>Applications which are evicted from memory, because they expired or because the maximum
 * capacity was exceeded, are removed from the {@link StateHandleStore} as well. Closing the store
 * only releases the locks on the persisted entries but keeps them.
 *
 * <p>The {@link StateHandleStore} must be dedicated to archived applications because the entries
 * are named after the application id by the given {@link ApplicationStoreUtil}.
 *
 * @param <R> type of the resource version of the {@link StateHandleStore}
 */
public class StateHandleArchivedApplicationStore<R extends ResourceVersion<R>>
        implements ArchivedApplicationStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(StateHandleArchivedApplicationStore.class);

    /**
     * Guards the {@link #stateHandleStore} which is accessed by puts as well as by evictions, which
     * can be triggered by the periodic cleanup.
     */
    private final Object lock = new Object();

    private final StateHandleStore<ArchivedApplication, R> stateHandleStore;

    private final ApplicationStoreUtil applicationStoreUtil;

    private final MemoryArchivedApplicationStore memoryStore;

    public StateHandleArchivedApplicationStore(
            StateHandleStore<ArchivedApplication, R> stateHandleStore,
            ApplicationStoreUtil applicationStoreUtil,
            Duration expirationTime,
            int maximumCapacity,
            @Nullable ScheduledExecutor scheduledExecutor,
            @Nullable Ticker ticker) {
        this.stateHandleStore = checkNotNull(stateHandleStore);
        this.applicationStoreUtil = checkNotNull(applicationStoreUtil);

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
    public Optional<ArchivedApplication> get(ApplicationID applicationId) {
        return memoryStore.get(applicationId);
    }

    @Override
    public void put(ArchivedApplication archivedApplication) throws IOException {
        // the application is served from memory even if it cannot be persisted
        memoryStore.put(archivedApplication);

        final ApplicationID applicationId = archivedApplication.getApplicationId();
        final String name = applicationStoreUtil.applicationIdToName(applicationId);

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
        memoryStore.close();

        synchronized (lock) {
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

    private void removePersistedApplication(ApplicationID applicationId) {
        final String name = applicationStoreUtil.applicationIdToName(applicationId);

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
