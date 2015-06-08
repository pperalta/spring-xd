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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.queue.QueueConsumer;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.xd.dirt.core.DeploymentUnit;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentAction;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentMessage;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitType;
import org.springframework.xd.dirt.stream.JobDefinition;
import org.springframework.xd.dirt.stream.JobDefinitionRepository;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.NotDeployedException;
import org.springframework.xd.dirt.stream.StreamDefinition;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDeployer;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
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

//	@Autowired
//	private StreamDeployer streamDeployer;
//
//	@Autowired
//	private JobDeployer jobDeployer;

	@Autowired
	private ResourceDeployer streamDeployer;

	@Autowired
	private ResourceDeployer jobDeployer;

	@Autowired
	private ZooKeeperConnection zkConnection;

	@Autowired
	private StreamDefinitionRepository streamRepository;

	@Autowired
	private JobDefinitionRepository jobRepository;

	@Autowired
	private StreamFactory streamFactory;

	@Autowired
	private JobFactory jobFactory;

//	// todo: for testing only; this will be removed eventually
//	public void consumeMessage(DeploymentMessage message, StreamDeployer streamDeployer, JobDeployer jobDeployer) throws Exception {
//		this.streamDeployer = streamDeployer;
//		this.jobDeployer = jobDeployer;
//		this.consumeMessage(message);
//	}

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
		DomainRepository<?, String> repository = type == DeploymentUnitType.Job ? jobRepository : streamRepository;
		ResourceDeployer deployer = type == DeploymentUnitType.Job ? jobDeployer : streamDeployer;
		String name = message.getUnitName();
		String errorDesc = null;

		try {
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
			}

			// todo: seems like I'm missing something that is
			// preventing the stream from being loaded via DeploymentLoader

			DeploymentUnit deploymentUnit = type == DeploymentUnitType.Job
					? DeploymentLoader.loadJob(zkConnection.getClient(), name, jobFactory)
					: DeploymentLoader.loadStream(zkConnection.getClient(), name, streamFactory);
			logger.warn("deployment unit: {}", deploymentUnit);

			switch (action) {
				case createAndDeploy:
				case deploy:
					deployer.deploy(deploymentUnit);
					break;
				case undeploy:
					deployer.undeploy(deploymentUnit);
					break;
				case undeployAll:
					deployer.undeployAll();
					break;
				case destroy:
					try {
						deployer.undeploy(deploymentUnit);
					}
					catch (NotDeployedException e) {
						// ignore
					}
					repository.delete(name);
					break;
				case destroyAll:
					deployer.undeployAll();
					repository.deleteAll();
					break;
			}
		}
		catch (Throwable t) {
			errorDesc = ZooKeeperUtils.getStackTrace(t);
			throw t;
		}
		finally {
			writeResponse(message, errorDesc);
		}
	}

	/**
	 * Write the deployment message response.
	 *
	 * @param message    deployment message
	 * @param errorDesc  error description if an error occurred processing
	 *                   the request
	 */
	private void writeResponse(DeploymentMessage message, String errorDesc) {
		try {
			String requestId = message.getRequestId();
			if (StringUtils.hasText(requestId)) {
				logger.debug("Processed deployment request " + requestId);
				String resultPath = Paths.build(Paths.DEPLOYMENTS, Paths.RESPONSES, requestId,
						errorDesc == null
								? ZKDeploymentMessagePublisher.SUCCESS
								: ZKDeploymentMessagePublisher.ERROR);
				logger.debug("creating result path {}", resultPath);
				zkConnection.getClient().create().forPath(resultPath,
						errorDesc == null ? null : errorDesc.getBytes());
			}
		}
		catch (Exception e) {
			logger.info("Could not publish response to deployment message", e);
		}
	}

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState) {
		logger.trace("Deployment Queue consumer state changed: " + newState);
	}
}
