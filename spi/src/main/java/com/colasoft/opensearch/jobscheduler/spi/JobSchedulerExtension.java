/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The ColaSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.colasoft.opensearch.jobscheduler.spi;

/**
 * SPI of job scheduler.
 */
public interface JobSchedulerExtension {
    /**
     * @return job type string.
     */
    String getJobType();

    /**
     * @return job index name.
     */
    String getJobIndex();

    /**
     * @return job runner implementation.
     */
    ScheduledJobRunner getJobRunner();

    /**
     * @return job document parser.
     */
    ScheduledJobParser getJobParser();
}
