/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.xd.dirt.stream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.xd.dirt.integration.bus.MessageBus;

/**
 * Launches jobs by sending launch messages via the {@link MessageBus}.
 *
 * @author Patrick Peralta
 */
public class JobLauncher {

	/**
	 * Prefix for channels dedicated to launching jobs.
	 */
	private final String JOB_CHANNEL_PREFIX = "job:";

	/**
	 * MessageBus used to deliver job launch messages.
	 */
	private final MessageBus messageBus;

	/**
	 * Map of job name to {@link MessageChannel}.
	 */
	private final ConcurrentMap<String, MessageChannel> jobChannels = new ConcurrentHashMap<String, MessageChannel>();

	/**
	 * Repository for job definitions.
	 */
	private final JobDefinitionRepository definitionRepository;

	/**
	 * Repository for deployed jobs.
	 */
	private final JobRepository instanceRepository;

	/**
	 * Construct a {@link JobLauncher}.
	 * @param messageBus            MessageBus used to deliver job launch messages
	 * @param definitionRepository  repository for job definitions
	 * @param instanceRepository    repository for deployed jobs
	 */
	public JobLauncher(MessageBus messageBus, JobDefinitionRepository definitionRepository,
			JobRepository instanceRepository) {
		this.messageBus = messageBus;
		this.definitionRepository = definitionRepository;
		this.instanceRepository = instanceRepository;
	}

	/**
	 * Launch a job with the given name.
	 *
	 * @param name           name of job to launch
	 * @param jobParameters  job parameters
	 *
	 * @throws NoSuchDefinitionException if the job definition does not exist
	 * @throws NotDeployedException      if the job exists but has not been deployed
	 */
	public void launch(String name, String jobParameters) {
		MessageChannel channel = this.jobChannels.get(name);
		if (channel == null) {
			this.jobChannels.putIfAbsent(name, new DirectChannel());
			channel = this.jobChannels.get(name);
			this.messageBus.bindProducer(JOB_CHANNEL_PREFIX + name, channel, null);
		}

		JobDefinition job = this.definitionRepository.findOne(name);
		if (job == null) {
			throw new NoSuchDefinitionException(name, String.format("Job %s not found", name));
		}
		if (instanceRepository.findOne(name) == null) {
			throw new NotDeployedException(name, String.format("Job %s not deployed", name));
		}

		channel.send(MessageBuilder.withPayload(jobParameters != null ? jobParameters : "").build());
	}

}
