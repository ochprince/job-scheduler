/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package com.colasoft.opensearch.jobscheduler;

import org.junit.Assert;
import com.colasoft.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import com.colasoft.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import com.colasoft.opensearch.action.admin.cluster.node.info.NodeInfo;
import com.colasoft.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import com.colasoft.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import com.colasoft.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import com.colasoft.opensearch.cluster.health.ClusterHealthStatus;
import com.colasoft.opensearch.plugins.PluginInfo;
import com.colasoft.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobSchedulerPluginIT extends OpenSearchIntegTestCase {

    public void testPluginsAreInstalled() {
        ClusterHealthRequest request = new ClusterHealthRequest();
        ClusterHealthResponse response = OpenSearchIntegTestCase.client().admin().cluster().health(request).actionGet();
        Assert.assertEquals(ClusterHealthStatus.GREEN, response.getStatus());

        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = OpenSearchIntegTestCase.client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();
        List<PluginInfo> pluginInfos = nodesInfoResponse.getNodes()
            .stream()
            .flatMap(
                (Function<NodeInfo, Stream<PluginInfo>>) nodeInfo -> nodeInfo.getInfo(PluginsAndModules.class).getPluginInfos().stream()
            )
            .collect(Collectors.toList());
        Assert.assertTrue(pluginInfos.stream().anyMatch(pluginInfo -> pluginInfo.getName().equals("opensearch-job-scheduler")));
    }
}
