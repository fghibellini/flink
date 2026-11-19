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

import java.io.Closeable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Factory for the {@link ArchivedApplicationStore} which the {@link Dispatcher} uses during a
 * single leadership session.
 *
 * <p>The factory lives as long as the process, whereas {@link #createArchivedApplicationStore()} is
 * called whenever the process is granted the leadership. The created store is closed when the
 * leadership session ends.
 */
@FunctionalInterface
public interface ArchivedApplicationStoreFactory extends Closeable {

    /**
     * Creates the {@link ArchivedApplicationStore} for a new leadership session.
     *
     * @return the store which is used during the leadership session
     * @throws Exception if the store could not be created
     */
    ArchivedApplicationStore createArchivedApplicationStore() throws Exception;

    /** Releases the resources which are shared by the stores of all leadership sessions. */
    @Override
    default void close() throws IOException {}

    /**
     * Creates a factory which hands out the given store to every leadership session, so that the
     * archived applications are kept as long as the process runs. Closing the handed out stores has
     * no effect; the given store is closed together with the factory.
     *
     * @param archivedApplicationStore the store which is shared by all leadership sessions
     * @return the factory
     */
    static ArchivedApplicationStoreFactory shared(
            ArchivedApplicationStore archivedApplicationStore) {
        checkNotNull(archivedApplicationStore);

        return new ArchivedApplicationStoreFactory() {
            @Override
            public ArchivedApplicationStore createArchivedApplicationStore() {
                return new NonClosingArchivedApplicationStore(archivedApplicationStore);
            }

            @Override
            public void close() throws IOException {
                archivedApplicationStore.close();
            }
        };
    }
}
