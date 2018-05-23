/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.scheduler.spi.cloudfoundry;

import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import io.jsonwebtoken.lang.Assert;
import io.pivotal.scheduler.SchedulerClient;
import io.pivotal.scheduler.v1.ExpressionType;
import io.pivotal.scheduler.v1.jobs.CreateJobRequest;
import io.pivotal.scheduler.v1.jobs.DeleteJobRequest;
import io.pivotal.scheduler.v1.jobs.ListJobSchedulesRequest;
import io.pivotal.scheduler.v1.jobs.ListJobSchedulesResponse;
import io.pivotal.scheduler.v1.jobs.ListJobsRequest;
import io.pivotal.scheduler.v1.jobs.ScheduleJobRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudfoundry.client.v2.applications.SummaryApplicationResponse;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.cloudfoundry.operations.applications.AbstractApplicationSummary;
import org.cloudfoundry.operations.applications.ApplicationSummary;
import org.cloudfoundry.operations.spaces.SpaceSummary;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.cloud.deployer.spi.cloudfoundry.CloudFoundry2630AndLaterTaskLauncher;
import org.springframework.cloud.deployer.spi.cloudfoundry.CloudFoundryConnectionProperties;
import org.springframework.cloud.deployer.spi.core.AppDeploymentRequest;
import org.springframework.cloud.scheduler.spi.core.CreateScheduleException;
import org.springframework.cloud.scheduler.spi.core.ScheduleInfo;
import org.springframework.cloud.scheduler.spi.core.ScheduleRequest;
import org.springframework.cloud.scheduler.spi.core.Scheduler;
import org.springframework.cloud.scheduler.spi.core.SchedulerException;
import org.springframework.cloud.scheduler.spi.core.SchedulerPropertyKeys;

/**
 * A Cloud Foundry implementation of the Scheduler interface.
 *
 * @author Glenn Renfro
 */
public class CloudFoundryAppScheduler implements Scheduler {

	final private SchedulerClient client;

	final private CloudFoundryOperations operations;

	final private CloudFoundryConnectionProperties properties;

	private CloudFoundry2630AndLaterTaskLauncher taskLauncher;

	protected static final Log logger = LogFactory.getLog(CloudFoundryAppScheduler.class);

	public CloudFoundryAppScheduler(SchedulerClient client, CloudFoundryOperations operations,
			CloudFoundryConnectionProperties properties, CloudFoundry2630AndLaterTaskLauncher taskLauncher) {
		Assert.notNull(client, "client must not be null");
		Assert.notNull(operations, "operations must not be null");
		Assert.notNull(properties, "properties must not be null");
		Assert.notNull(taskLauncher, "taskLauncher must not be null");

		this.client = client;
		this.operations = operations;
		this.properties = properties;
		this.taskLauncher = taskLauncher;
	}

	@Override
	public void schedule(ScheduleRequest scheduleRequest) {
		String appName = scheduleRequest.getDefinition().getName();
		String jobName = scheduleRequest.getScheduleName();
		String command = stageTask(scheduleRequest);

		String cronExpression = scheduleRequest.getSchedulerProperties().get(SchedulerPropertyKeys.CRON_EXPRESSION);
		Assert.hasText(cronExpression, String.format(
				"request's scheduleProperties must have a %s that is not null nor empty",
				SchedulerPropertyKeys.CRON_EXPRESSION));
		scheduleJob(appName, jobName, cronExpression, command);
	}

	@Override
	public void unschedule(String scheduleName) {
		getJob(scheduleName).flatMap(scheduleJobInfo -> {
			return this.client.jobs().delete(DeleteJobRequest.builder()
					.jobId(scheduleJobInfo.getJobId())
					.build()); })
				.onErrorMap(e -> {
					if (e instanceof NoSuchElementException) {
						throw new SchedulerException(String.format("Failed to unschedule, schedule %s does not exist.", scheduleName), e);
					}
					throw new SchedulerException("Failed to unschedule: " + scheduleName, e);
				})
				.block();
	}

	@Override
	public List<ScheduleInfo> list(String taskDefinitionName) {
		throw new UnsupportedOperationException("Interface is not implemented for list method.");
	}

	@Override
	public List<ScheduleInfo> list() {
		throw new UnsupportedOperationException("Interface is not implemented for list method.");
	}

	private void scheduleJob(String appName, String scheduleName, String expression, String command) {
		logger.debug(String.format("Scheduling Task: ", appName));
		getApplicationByAppName(appName)
				.flatMap(abstractApplicationSummary -> {
					return this.client.jobs().create(CreateJobRequest.builder()
							.applicationId(abstractApplicationSummary.getId()) // App GUID
							.command(command)
							.name(scheduleName)
							.build());
				}).flatMap(createJobResponse -> {
			return this.client.jobs().schedule(ScheduleJobRequest.
					builder().
					jobId(createJobResponse.getId()).
					expression(expression).
					expressionType(ExpressionType.CRON).
					enabled(true).
					build()); })
				.onErrorMap(e -> {
					throw new CreateScheduleException("Failed to schedule: " + scheduleName, e); })
				.block();
	}

	private String stageTask(ScheduleRequest scheduleRequest) {
		logger.debug(String.format("Staging Task: ",
				scheduleRequest.getDefinition().getName()));
		AppDeploymentRequest request = new AppDeploymentRequest(
				scheduleRequest.getDefinition(),
				scheduleRequest.getResource(),
				scheduleRequest.getDeploymentProperties());
		SummaryApplicationResponse response = taskLauncher.stage(request);
		return taskLauncher.getCommand(response, request);
	}

	private Mono<AbstractApplicationSummary> getApplicationByAppName(String appName) {
		return requestListApplications()
				.log()
				.filter(application -> appName.equals(application.getName()))
				.log()
				.singleOrEmpty()
				.cast(AbstractApplicationSummary.class);
	}

	private Flux<ApplicationSummary> requestListApplications() {
		return this.operations.applications()
				.list();
	}

	private Flux<ScheduleJobInfo> getJobs() {
		Flux<ApplicationSummary> applicationSummaries = cacheAppSummaries();
		return  this.getSpace(this.properties.getSpace()).flatMap(requestSummary -> {
			return this.client
					.jobs()
					.list(ListJobsRequest.builder()
							.spaceId(requestSummary.getId())
							.build()); })
				.flatMapIterable(jobs -> {
					return jobs.getResources(); })// iterate over the resources returned.
				.flatMap(job -> {
					return getApplication(applicationSummaries,
							job.getApplicationId()) // get the application name for each job.
							.map(app -> {
								ScheduleJobInfo scheduleJobInfo = new ScheduleJobInfo();
								scheduleJobInfo.setScheduleProperties(new HashMap<>());
								scheduleJobInfo.setScheduleName(job.getName());
								scheduleJobInfo.setTaskDefinitionName(app.getName());
								scheduleJobInfo.setJobId(job.getId());
								return scheduleJobInfo; }); });
	}

	public Mono<ScheduleJobInfo> getJob(String scheduleName) {
		return getJobs().filter(scheduleInfo ->
				scheduleInfo.getScheduleName().equals(scheduleName))
				.single();
	}

	public Mono<ListJobSchedulesResponse> getScheduleExpression(String jobId) {
		return this.client.jobs()
				.listSchedules(
						ListJobSchedulesRequest
								.builder()
								.jobId(jobId)
								.build());
	}

	private Flux<ApplicationSummary> cacheAppSummaries() {
		return requestListApplications()
				.cache(); //cache results from first call.  No need to re-retrieve each time.
	}

	private Mono<ApplicationSummary> getApplication(Flux<ApplicationSummary> applicationSummaries,
			String appId) {
		return applicationSummaries
				.filter(application -> appId.equals(application.getId()))
				.singleOrEmpty();
	}

	private Mono<SpaceSummary> getSpace(String spaceName) {
		return requestSpaces()
				.cache() //cache results from first call.
				.filter(space -> spaceName.equals(space.getName()))
				.singleOrEmpty()
				.cast(SpaceSummary.class);
	}

	private Flux<SpaceSummary> requestSpaces() {
		return this.operations.spaces()
				.list();
	}

}
