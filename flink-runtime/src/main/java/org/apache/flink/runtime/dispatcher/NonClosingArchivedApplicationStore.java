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
import org.apache.flink.runtime.messages.webmonitor.ApplicationDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link ArchivedApplicationStore} which delegates to another store but does not close it, so that
 * the delegate can outlive the leadership session it is handed out to.
 *
 * @see ArchivedApplicationStoreFactory#shared(ArchivedApplicationStore)
 */
class NonClosingArchivedApplicationStore implements ArchivedApplicationStore {

    private final ArchivedApplicationStore delegate;

    NonClosingArchivedApplicationStore(ArchivedApplicationStore delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void recover() throws Exception {
        delegate.recover();
    }

    @Override
    public Optional<ArchivedApplication> get(ApplicationID applicationId) {
        return delegate.get(applicationId);
    }

    @Override
    public void put(ArchivedApplication archivedApplication) throws IOException {
        delegate.put(archivedApplication);
    }

    @Override
    public Collection<ApplicationDetails> getApplicationDetails() {
        return delegate.getApplicationDetails();
    }

    @Override
    public Optional<ExecutionGraphInfo> getExecutionGraphInfo(JobID jobId) {
        return delegate.getExecutionGraphInfo(jobId);
    }

    @Override
    public Collection<JobDetails> getJobDetails() {
        return delegate.getJobDetails();
    }

    @Override
    public JobsOverview getJobsOverview() {
        return delegate.getJobsOverview();
    }

    @Override
    public void close() {}
}
