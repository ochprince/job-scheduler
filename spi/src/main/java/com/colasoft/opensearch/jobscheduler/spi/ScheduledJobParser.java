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

import com.colasoft.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

public interface ScheduledJobParser {
    ScheduledJobParameter parse(XContentParser xContentParser, String id, JobDocVersion jobDocVersion) throws IOException;
}
