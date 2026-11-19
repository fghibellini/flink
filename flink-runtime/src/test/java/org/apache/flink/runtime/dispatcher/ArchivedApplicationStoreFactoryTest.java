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

import org.apache.flink.runtime.application.ArchivedApplication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ArchivedApplicationStoreFactory}. */
class ArchivedApplicationStoreFactoryTest {

    @Test
    void testSharedStoreOutlivesLeadershipSessionsAndIsClosedWithFactory() throws Exception {
        final MemoryArchivedApplicationStore sharedStore = new MemoryArchivedApplicationStore();
        final ArchivedApplication archivedApplication =
                ArchivedApplicationStoreTestUtils.generateTerminalArchivedApplications(1)
                        .iterator()
                        .next();
        final ArchivedApplicationStoreFactory factory =
                ArchivedApplicationStoreFactory.shared(sharedStore);

        try (final ArchivedApplicationStore firstSessionStore =
                factory.createArchivedApplicationStore()) {
            firstSessionStore.put(archivedApplication);
        }

        try (final ArchivedApplicationStore secondSessionStore =
                factory.createArchivedApplicationStore()) {
            assertThat(secondSessionStore.get(archivedApplication.getApplicationId()))
                    .containsSame(archivedApplication);
        }

        factory.close();

        assertThat(sharedStore.size()).isZero();
    }
}
