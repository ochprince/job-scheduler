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
package com.colasoft.opensearch.jobscheduler;

import com.colasoft.opensearch.jobscheduler.scheduler.JobScheduler;
import com.colasoft.opensearch.jobscheduler.spi.JobSchedulerExtension;
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobParser;
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobRunner;
import com.colasoft.opensearch.jobscheduler.spi.schedule.Schedule;
import com.colasoft.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import com.colasoft.opensearch.jobscheduler.spi.utils.LockService;
import com.colasoft.opensearch.jobscheduler.sweeper.JobSweeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.colasoft.opensearch.client.Client;
import com.colasoft.opensearch.cluster.metadata.IndexNameExpressionResolver;
import com.colasoft.opensearch.cluster.service.ClusterService;
import com.colasoft.opensearch.core.ParseField;
import com.colasoft.opensearch.common.io.stream.NamedWriteableRegistry;
import com.colasoft.opensearch.common.settings.Setting;
import com.colasoft.opensearch.common.settings.Settings;
import com.colasoft.opensearch.common.util.concurrent.OpenSearchExecutors;
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry;
import com.colasoft.opensearch.env.Environment;
import com.colasoft.opensearch.env.NodeEnvironment;
import com.colasoft.opensearch.index.IndexModule;
import com.colasoft.opensearch.plugins.ExtensiblePlugin;
import com.colasoft.opensearch.plugins.Plugin;
import com.colasoft.opensearch.repositories.RepositoriesService;
import com.colasoft.opensearch.script.ScriptService;
import com.colasoft.opensearch.threadpool.ExecutorBuilder;
import com.colasoft.opensearch.threadpool.FixedExecutorBuilder;
import com.colasoft.opensearch.threadpool.ThreadPool;
import com.colasoft.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class JobSchedulerPlugin extends Plugin implements ExtensiblePlugin {

    public static final String OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME = "open_distro_job_scheduler";

    private static final Logger log = LogManager.getLogger(JobSchedulerPlugin.class);

    private JobSweeper sweeper;
    private JobScheduler scheduler;
    private LockService lockService;
    private Map<String, ScheduledJobProvider> indexToJobProviders;
    private Set<String> indicesToListen;

    public JobSchedulerPlugin() {
        this.indicesToListen = new HashSet<>();
        this.indexToJobProviders = new HashMap<>();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.lockService = new LockService(client, clusterService);
        this.scheduler = new JobScheduler(threadPool, this.lockService);
        this.sweeper = initSweeper(
            environment.settings(),
            client,
            clusterService,
            threadPool,
            xContentRegistry,
            this.scheduler,
            this.lockService
        );
        clusterService.addListener(this.sweeper);
        clusterService.addLifecycleListener(this.sweeper);

        return Collections.emptyList();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.REQUEST_TIMEOUT);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.SWEEP_PERIOD);
        settingList.add(LegacyOpenDistroJobSchedulerSettings.JITTER_LIMIT);
        settingList.add(JobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingList.add(JobSchedulerSettings.REQUEST_TIMEOUT);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingList.add(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingList.add(JobSchedulerSettings.SWEEP_PERIOD);
        settingList.add(JobSchedulerSettings.JITTER_LIMIT);
        return settingList;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        final int processorCount = OpenSearchExecutors.allocatedProcessors(settings);

        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(
            new FixedExecutorBuilder(
                settings,
                OPEN_DISTRO_JOB_SCHEDULER_THREAD_POOL_NAME,
                processorCount,
                200,
                "opendistro.jobscheduler.threadpool"
            )
        );

        return executorBuilders;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (this.indicesToListen.contains(indexModule.getIndex().getName())) {
            indexModule.addIndexOperationListener(this.sweeper);
            log.info("JobSweeper started listening to operations on index {}", indexModule.getIndex().getName());
        }
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {

        for (JobSchedulerExtension extension : loader.loadExtensions(JobSchedulerExtension.class)) {
            String jobType = extension.getJobType();
            String jobIndexName = extension.getJobIndex();
            ScheduledJobParser jobParser = extension.getJobParser();
            ScheduledJobRunner runner = extension.getJobRunner();
            if (this.indexToJobProviders.containsKey(jobIndexName)) {
                continue;
            }

            ScheduledJobProvider provider = new ScheduledJobProvider(jobType, jobIndexName, jobParser, runner);
            this.indexToJobProviders.put(jobIndexName, provider);
            this.indicesToListen.add(jobIndexName);
            log.info("Loaded scheduler extension: {}, index: {}", jobType, jobIndexName);
        }
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        List<NamedXContentRegistry.Entry> registryEntries = new ArrayList<>();

        // register schedule
        NamedXContentRegistry.Entry scheduleEntry = new NamedXContentRegistry.Entry(
            Schedule.class,
            new ParseField("schedule"),
            ScheduleParser::parse
        );
        registryEntries.add(scheduleEntry);

        return registryEntries;
    }

    private JobSweeper initSweeper(
        Settings settings,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        NamedXContentRegistry registry,
        JobScheduler scheduler,
        LockService lockService
    ) {
        return new JobSweeper(settings, client, clusterService, threadPool, registry, this.indexToJobProviders, scheduler, lockService);
    }
}
