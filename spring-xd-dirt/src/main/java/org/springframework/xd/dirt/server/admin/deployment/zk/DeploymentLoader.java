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

package org.springframework.xd.dirt.server.admin.deployment.zk;

import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.core.DeploymentUnitStatus;
import org.springframework.xd.dirt.core.Job;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitType;
import org.springframework.xd.dirt.stream.AlreadyDeployedException;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.rest.domain.support.DeploymentPropertiesFormat;

/**
 * Utility for loading streams and jobs for the purpose of deployment.
 * <p/>
 *
 * @see org.springframework.xd.dirt.server.admin.deployment.zk.ContainerListener
 * @see org.springframework.xd.dirt.server.admin.deployment.zk.ZKStreamDeploymentHandler
 * @see org.springframework.xd.dirt.server.admin.deployment.zk.ZKJobDeploymentHandler
 *
 * @author Patrick Peralta
 * @author Ilayaperumal Gopinathan
 */
public class DeploymentLoader {

	private final static Logger logger = LoggerFactory.getLogger(DeploymentLoader.class);

	private final StreamFactory streamFactory;

	private final JobFactory jobFactory;

	private final ZooKeeperConnection zkConnection;


	public DeploymentLoader(StreamFactory streamFactory, JobFactory jobFactory, ZooKeeperConnection zkConnection) {
		this.streamFactory = streamFactory;
		this.jobFactory = jobFactory;
		this.zkConnection = zkConnection;
	}

	/**
	 * todo
	 *
	 * @param name
	 * @param properties
	 * @return
	 */
	public Stream newStreamInstance(String name, Map<String, String> properties) {
		writeDeploymentProperties(DeploymentUnitType.Stream, name, properties);
		return loadStream(name);
	}

	/**
	 * todo
	 *
	 * @param name
	 * @param properties
	 * @return
	 */
	public Job newJobInstance(String name, Map<String, String> properties) {
		writeDeploymentProperties(DeploymentUnitType.Job, name, properties);
		return loadJob(name);
	}

	/**
	 * todo
	 * @param type
	 * @param name
	 * @param properties
	 */
	protected void writeDeploymentProperties(DeploymentUnitType type, String name, Map<String, String> properties) {
		Assert.hasText(name, "name cannot be blank or null");
		logger.trace("Preparing deployment of '{}'", name);

		try {
			String deploymentPath = Paths.build(type == DeploymentUnitType.Stream
					? Paths.STREAM_DEPLOYMENTS
					: Paths.JOB_DEPLOYMENTS, name);

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
					String.format("Stream '%s' is already deployed", name));
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

	/**
	 * Load the {@link org.springframework.xd.dirt.core.Stream} instance
	 * for a given stream name <i>if the stream is deployed</i>. It will
	 * include the stream definition as well as any deployment properties
	 * data for the stream deployment.
	 *
	 * @param streamName     the name of the stream to load
	 * @return the stream instance, or {@code null} if the stream does
	 *         not exist or is not deployed
	 */
	public Stream loadStream(String streamName) {
		try {
			CuratorFramework client = zkConnection.getClient();
			byte[] definition = client.getData().forPath(Paths.build(Paths.STREAMS, streamName));
			Map<String, String> definitionMap = ZooKeeperUtils.bytesToMap(definition);

			byte[] deploymentPropertiesData = client.getData().forPath(
					Paths.build(Paths.STREAM_DEPLOYMENTS, streamName));
			if (deploymentPropertiesData != null && deploymentPropertiesData.length > 0) {
				definitionMap.put("deploymentProperties", new String(deploymentPropertiesData, "UTF-8"));
			}
			return streamFactory.createStream(streamName, definitionMap);
		}
		catch (KeeperException.NoNodeException e) {
			logger.warn("===> error loading stream", e);
			// stream is not deployed or does not exist
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
		return null;
	}

	/**
	 * Load the {@link org.springframework.xd.dirt.core.Job}
	 * instance for a given job name<i>if the job is deployed</i>.
	 *
	 * @param jobName  the name of the job to load
	 * @return the job instance, or {@code null} if the job does not exist
	 *         or is not deployed
	 */
	public Job loadJob(String jobName) {
		try {
			CuratorFramework client = zkConnection.getClient();
			byte[] definition = client.getData().forPath(Paths.build(Paths.JOBS, jobName));
			Map<String, String> definitionMap = ZooKeeperUtils.bytesToMap(definition);

			byte[] deploymentPropertiesData = client.getData().forPath(
					Paths.build(Paths.JOB_DEPLOYMENTS, jobName));
			if (deploymentPropertiesData != null && deploymentPropertiesData.length > 0) {
				definitionMap.put("deploymentProperties", new String(deploymentPropertiesData, "UTF-8"));
			}
			return jobFactory.createJob(jobName, definitionMap);
		}
		catch (KeeperException.NoNodeException e) {
			// job is not deployed
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
		return null;
	}

}
