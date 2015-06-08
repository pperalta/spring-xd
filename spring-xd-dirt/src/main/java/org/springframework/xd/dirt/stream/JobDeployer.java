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

import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectedEvent;
import org.springframework.xd.dirt.server.admin.deployment.zk.SupervisorElectionListener;

/**
 * @author Glenn Renfro
 * @author Luke Taylor
 * @author Ilayaperumal Gopinathan
 * @author Gunnar Hillert
 */
//public class JobDeployer extends AbstractInstancePersistingDeployer<JobDefinition, Job> implements DisposableBean {
//public class JobDeployer implements ResourceDeployer, SupervisorElectionListener {
public class JobDeployer  {

//	private final ResourceDeployer deployer;
//
//	public JobDeployer(ResourceDeployer deployer) {
//		this.deployer = deployer;
//	}
//
//	@Override
//	public void deploy(DeploymentUnit deploymentUnit) {
//		deployer.deploy(name);
//	}
//
//	@Override
//	public void undeploy(DeploymentUnit deploymentUnit) {
//		deployer.undeploy(name);
//	}
//
//	@Override
//	public void undeployAll() {
//		deployer.undeployAll();
//	}
//
//	@Override
//	public DeploymentUnitStatus getDeploymentStatus(String name) {
//		return deployer.getDeploymentStatus(name);
//	}
//
//	@Override
//	public void onSupervisorElected(SupervisorElectedEvent event) throws Exception {
//		if (this.deployer instanceof SupervisorElectionListener) {
//			((SupervisorElectionListener) this.deployer).onSupervisorElected(event);
//		}
//	}

//	private final String JOB_CHANNEL_PREFIX = "job:";
//
//	private final MessageBus messageBus;
//
//	private final ConcurrentMap<String, MessageChannel> jobChannels = new ConcurrentHashMap<String, MessageChannel>();
//
//	public JobDeployer(ZooKeeperConnection zkConnection, JobDefinitionRepository definitionRepository,
//			JobRepository instanceRepository, XDParser parser, MessageBus messageBus,
//			DeploymentHandler deploymentHandler) {
//		super(zkConnection, definitionRepository, instanceRepository, parser, deploymentHandler, job);
//		Assert.notNull(messageBus, "MessageBus must not be null");
//		this.messageBus = messageBus;
//	}
//
//	@Override
//	protected Job makeInstance(JobDefinition definition) {
//		return new Job(definition);
//	}
//

//	public void launch(String name, String jobParameters) {
//		throw new UnsupportedOperationException();
//	}

//	public void launch(String name, String jobParameters) {
//		MessageChannel channel = jobChannels.get(name);
//		if (channel == null) {
//			jobChannels.putIfAbsent(name, new DirectChannel());
//			channel = jobChannels.get(name);
//			messageBus.bindProducer(JOB_CHANNEL_PREFIX + name, channel, null);
//		}
//		// Double check so that user gets an informative error message
//		JobDefinition job = getDefinitionRepository().findOne(name);
//		if (job == null) {
//			throwNoSuchDefinitionException(name);
//		}
//		if (instanceRepository.findOne(name) == null) {
//			throwNotDeployedException(name);
//		}
//		// todo: is this Assert necessary? if not we can remove the parser dependency and parse method
//		Assert.isTrue(parse(name, job.getDefinition()).size() == 1, "Expecting only a single module");
//		channel.send(MessageBuilder.withPayload(jobParameters != null ? jobParameters : "").build());
//	}
//
//	@Override
//	protected JobDefinition createDefinition(String name, String definition) {
//		return new JobDefinition(name, definition);
//	}
//
//	@Override
//	protected String getDeploymentPath(JobDefinition definition) {
//		return Paths.build(Paths.JOB_DEPLOYMENTS, definition.getName());
//	}
//
//	private List<ModuleDescriptor> parse(String name, String definition) {
//		return parser.parse(name, definition, definitionKind);
//	}
//
//	@Override
//	public void destroy() throws Exception {
//		for (Map.Entry<String, MessageChannel> entry : jobChannels.entrySet()) {
//			messageBus.unbindProducer(JOB_CHANNEL_PREFIX + entry.getKey(), entry.getValue());
//		}
//	}

}
