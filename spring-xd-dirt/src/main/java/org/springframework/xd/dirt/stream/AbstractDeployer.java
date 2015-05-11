/*
 * Copyright 2011-2015 the original author or authors.
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

package org.springframework.xd.dirt.stream;

import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.core.BaseDefinition;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentHandler;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.rest.domain.support.DeploymentPropertiesFormat;

/**
 * Abstract implementation of the @link {@link org.springframework.xd.dirt.core.ResourceDeployer}
 * interface. It provides the basic support for calling CrudRepository methods and sending
 * deployment messages.
 *
 * @author Luke Taylor
 * @author Mark Pollack
 * @author Eric Bottard
 * @author Andy Clement
 * @author David Turanski
 */
public abstract class AbstractDeployer<D extends BaseDefinition, I extends BaseInstance<D>>
		implements ResourceDeployer<D> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractDeployer.class);

	private final ZooKeeperConnection zkConnection;

	protected final XDParser parser;

	protected final DeploymentHandler deploymentHandler;

	/**
	 * Used in exception messages as well as indication to the parser.
	 */
	protected final ParsingContext definitionKind;

	protected final DeploymentValidator validator;

	protected AbstractDeployer(ZooKeeperConnection zkConnection, XDParser parser,
			ParsingContext parsingContext, DeploymentValidator validator,
			DeploymentHandler deploymentHandler) {
		Assert.notNull(zkConnection, "ZooKeeper connection cannot be null");
		Assert.notNull(parsingContext, "Entity type kind cannot be null");
		this.zkConnection = zkConnection;
		this.definitionKind = parsingContext;
		this.parser = parser;
		this.validator = validator;
		this.deploymentHandler = deploymentHandler;
	}

	@Override
	public void deploy(String name, Map<String, String> properties) {
		logger.info("Deploying {}", name);

		validator.validateBeforeDeploy(name, properties);
		prepareDeployment(name, properties);
		deploymentHandler.deploy(name);
	}

	@Override
	public void undeploy(String name) {
		logger.info("Undeploying {}", name);

		validator.validateBeforeUndeploy(name);
		logger.info("deployment handler {}", deploymentHandler);
		deploymentHandler.undeploy(name);
	}

	@Override
	public void undeployAll() {
		deploymentHandler.undeployAll();
	}

	/**
	 * Return the ZooKeeper connection.
	 *
	 * @return the ZooKeeper connection
	 */
	protected ZooKeeperConnection getZooKeeperConnection() {
		return zkConnection;
	}

	/**
	 * Prepares a deployment by persisting the deployment data (including formatted
	 * deployment properties) and current deployment state of "deploying".
	 *
	 * @see org.springframework.xd.dirt.core.DeploymentUnitStatus.State#deploying
	 */
	protected void prepareDeployment(String name, Map<String, String> properties) {
		Assert.hasText(name, "name cannot be blank or null");
		logger.trace("Preparing deployment of '{}'", name);

		try {
			String deploymentPath = getDeploymentPath(name);
			String statusPath = Paths.build(deploymentPath, Paths.STATUS);
			byte[] propertyBytes = DeploymentPropertiesFormat.formatDeploymentProperties(properties).getBytes("UTF-8");
			byte[] statusBytes = ZooKeeperUtils.mapToBytes(
					new DeploymentUnitStatus(DeploymentUnitStatus.State.deploying).toMap());

			zkConnection.getClient().inTransaction()
					.create().forPath(deploymentPath, propertyBytes).and()
					.create().withMode(CreateMode.EPHEMERAL).forPath(statusPath, statusBytes).and()
					.commit();
			logger.trace("Deployment state of '{}' set to 'deploying'", name);
		}
		catch (KeeperException.NodeExistsException e) {
			throw new AlreadyDeployedException(name,
					String.format("The %s named '%%s' is already deployed", definitionKind));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	/**
	 * Return the ZooKeeper path used for deployment requests for the
	 * given definition.
	 *
	 * @param name name of definition
	 *
	 * @return ZooKeeper path for deployment requests
	 */
	protected abstract String getDeploymentPath(String name);

}
