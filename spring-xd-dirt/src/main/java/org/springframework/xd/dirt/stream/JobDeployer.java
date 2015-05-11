/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.xd.dirt.stream;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.Assert;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentHandler;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;

/**
 * @author Glenn Renfro
 * @author Luke Taylor
 * @author Ilayaperumal Gopinathan
 * @author Gunnar Hillert
 */
public class JobDeployer extends AbstractDeployer<JobDefinition, Job> implements DisposableBean {

	private final String JOB_CHANNEL_PREFIX = "job:";

	private final MessageBus messageBus;

	private final ConcurrentMap<String, MessageChannel> jobChannels = new ConcurrentHashMap<String, MessageChannel>();

	public JobDeployer(ZooKeeperConnection zkConnection, XDParser parser,
			DeploymentValidator validator,  DeploymentHandler deploymentHandler, MessageBus messageBus) {
		super(zkConnection, parser, ParsingContext.job, validator, deploymentHandler);
		Assert.notNull(messageBus, "MessageBus must not be null");
		this.messageBus = messageBus;
	}

	public void launch(String name, String jobParameters) {
		validator.validateDeployed(name);

		MessageChannel channel = jobChannels.get(name);
		if (channel == null) {
			jobChannels.putIfAbsent(name, new DirectChannel());
			channel = jobChannels.get(name);
			messageBus.bindProducer(JOB_CHANNEL_PREFIX + name, channel, null);
		}

		channel.send(MessageBuilder.withPayload(jobParameters != null ? jobParameters : "").build());
	}

	@Override
	protected String getDeploymentPath(String name) {
		return Paths.build(Paths.JOB_DEPLOYMENTS, name);
	}

	@Override
	public void destroy() throws Exception {
		for (Map.Entry<String, MessageChannel> entry : jobChannels.entrySet()) {
			messageBus.unbindProducer(JOB_CHANNEL_PREFIX + entry.getKey(), entry.getValue());
		}
	}

}
