/*
 * Copyright 2013-2014 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.xd.dirt.server.admin.deployment.DeploymentHandler;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;

/**
 * Default implementation of {@link StreamDeployer} that uses provided
 * {@link StreamDefinitionRepository} and {@link StreamRepository} to
 * persist stream deployment and undeployment requests.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Andy Clement
 * @author Eric Bottard
 * @author Gunnar Hillert
 * @author Patrick Peralta
 * @author Ilayaperumal Gopinathan
 */
public class StreamDeployer extends AbstractDeployer<StreamDefinition, Stream> {

	private static final Logger logger = LoggerFactory.getLogger(StreamDeployer.class);

	/**
	 * Construct a StreamDeployer.
	 *
	 * @param zkConnection       ZooKeeper connection
	 * @param parser             stream definition parser
	 * @param validator          deployment validator
	 * @param deploymentHandler  deployment handler
	 */
	public StreamDeployer(ZooKeeperConnection zkConnection, XDParser parser,
			DeploymentValidator validator, DeploymentHandler deploymentHandler) {
		super(zkConnection, parser, ParsingContext.stream, validator, deploymentHandler);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDeploymentPath(String name) {
		return Paths.build(Paths.STREAM_DEPLOYMENTS, name);
	}

}
