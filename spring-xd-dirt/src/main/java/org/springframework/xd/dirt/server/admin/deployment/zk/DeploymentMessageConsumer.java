/*
 * Copyright 2015 the original author or authors.
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

package org.springframework.xd.dirt.server.admin.deployment.zk;

import static org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitType.*;

import java.util.Collections;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentAction;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitType;
import org.springframework.xd.dirt.stream.JobDefinition;
import org.springframework.xd.dirt.stream.JobDefinitionRepository;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.StreamDefinition;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDeployer;
import org.springframework.xd.store.DomainRepository;

/**
 * Consumer for {@link org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage}
 * which delegates to the corresponding @{@link org.springframework.xd.dirt.core.ResourceDeployer}
 * to process the deployment requests.
 *
 * @author Ilayaperumal Gopinathan
 */
public class DeploymentMessageConsumer implements QueueConsumer<DeploymentMessage> {

	private static final Logger logger = LoggerFactory.getLogger(DeploymentMessageConsumer.class);

	@Autowired
	private StreamDeployer streamDeployer;

	@Autowired
	private JobDeployer jobDeployer;

	@Autowired
	private StreamDefinitionRepository streamRepository;

	@Autowired
	private JobDefinitionRepository jobRepository;

	public DeploymentMessageConsumer() {
	}

	public DeploymentMessageConsumer(StreamDeployer streamDeployer, JobDeployer jobDeployer,
			StreamDefinitionRepository streamRepository, JobDefinitionRepository jobRepository) {
		this.streamDeployer = streamDeployer;
		this.jobDeployer = jobDeployer;
		this.streamRepository = streamRepository;
		this.jobRepository = jobRepository;
	}

	/**
	 * Consume the deployment message and delegate to the deployer.
	 *
	 * @param message the deployment message
	 * @throws Exception
	 */
	@Override
	public void consumeMessage(DeploymentMessage message) throws Exception {
		DeploymentUnitType type = message.getDeploymentUnitType();
		DeploymentAction action = message.getDeploymentAction();
		DomainRepository<?, String> repository = type == Job ? jobRepository : streamRepository;
		ResourceDeployer<?> deployer = type == Job ? jobDeployer : streamDeployer;
		String name = message.getUnitName();

		switch (action) {
			case create:
			case createAndDeploy: {
				switch (type) {
					case Stream:
						streamRepository.save(new StreamDefinition(name, message.getDefinition()));
						break;
					case Job:
						jobRepository.save(new JobDefinition(name, message.getDefinition()));
						break;
				}
			}
			if (DeploymentAction.createAndDeploy == action) {
				deployer.deploy(name, Collections.<String, String>emptyMap());
			}
			break;
			case deploy:
				deployer.deploy(name, message.getDeploymentProperties());
				break;
			case undeploy:
				deployer.undeploy(name);
				break;
			case undeployAll:
				deployer.undeployAll();
				break;
			case destroy:
				deployer.undeploy(name);
				repository.delete(name);
				break;
			case destroyAll:
				deployer.undeployAll();
				repository.deleteAll();
				break;
		}
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		logger.trace("Deployment Queue consumer state changed: " + newState);
	}
}
