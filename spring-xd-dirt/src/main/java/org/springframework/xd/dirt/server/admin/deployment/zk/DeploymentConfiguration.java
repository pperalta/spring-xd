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

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.AuditAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoDataAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.xd.dirt.cluster.AdminAttributes;
import org.springframework.xd.dirt.container.store.AdminRepository;
import org.springframework.xd.dirt.container.store.ZooKeeperAdminRepository;
import org.springframework.xd.dirt.core.ResourceDeployer;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.module.ModuleRegistry;
import org.springframework.xd.dirt.server.admin.deployment.DefaultDeploymentUnitStateCalculator;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentStrategy;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitStateCalculator;
import org.springframework.xd.dirt.server.admin.deployment.JobDeploymentStrategy;
import org.springframework.xd.dirt.server.admin.deployment.StreamDeploymentStrategy;
import org.springframework.xd.dirt.stream.AlreadyDeployedException;
import org.springframework.xd.dirt.stream.DefinitionAlreadyExistsException;
import org.springframework.xd.dirt.stream.DeploymentValidator;
import org.springframework.xd.dirt.stream.JobDefinitionRepository;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.JobRepository;
import org.springframework.xd.dirt.stream.NoSuchDefinitionException;
import org.springframework.xd.dirt.stream.NotDeployedException;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDeployer;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.stream.StreamRepository;
import org.springframework.xd.dirt.stream.XDStreamParser;
import org.springframework.xd.dirt.stream.ZooKeeperResourceDeployer;
import org.springframework.xd.dirt.util.RuntimeUtils;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;

/**
 * Configuration class that holds the beans required for deployment management.
 *
 * @author Ilayaperumal Gopinathan
 */
@Configuration
@EnableAutoConfiguration(exclude = {BatchAutoConfiguration.class, JmxAutoConfiguration.class,
		AuditAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class })
public class DeploymentConfiguration {

	@Autowired
	private ZooKeeperConnection zkConnection;

	@Autowired
	private StreamDefinitionRepository streamDefinitionRepository;

	@Autowired
	private StreamRepository streamRepository;

	@Autowired
	private JobDefinitionRepository jobDefinitionRepository;

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private ModuleRegistry moduleRegistry;

	@Autowired
	private ModuleOptionsMetadataResolver moduleOptionsMetadataResolver;

	@Autowired
	private MessageBus messageBus;

	@Bean
	public XDStreamParser parser() {
		return new XDStreamParser(streamDefinitionRepository, moduleRegistry, moduleOptionsMetadataResolver);
	}

	@Bean
	public StreamFactory streamFactory() {
		return new StreamFactory(streamDefinitionRepository, moduleRegistry, moduleOptionsMetadataResolver);
	}

	@Bean
	public JobFactory jobFactory() {
		return new JobFactory(jobDefinitionRepository, moduleRegistry, moduleOptionsMetadataResolver);
	}

	@Bean
	public StreamDeploymentStrategy streamDeploymentStrategy() {
		return new StreamDeploymentStrategy();
	}

	@Bean
	public JobDeploymentStrategy jobDeploymentStrategy() {
		return new JobDeploymentStrategy();
	}

	@Bean
	public DeploymentUnitStateCalculator deploymentUnitStateCalculator() {
		return new DefaultDeploymentUnitStateCalculator();
	}

	@Bean
	public ResourceDeployer streamDeployer() {
		return new ZooKeeperResourceDeployer(streamDeploymentStrategy());
	}

	@Bean
	public ResourceDeployer jobDeployer() {
		return new ZooKeeperResourceDeployer(jobDeploymentStrategy());
	}

	@Bean
	@Deprecated
	// todo: revisit this
	public DeploymentValidator stubValidator() {
		return new DeploymentValidator() {
			@Override
			public void validateBeforeSave(String name, String definition) throws DefinitionAlreadyExistsException {
			}

			@Override
			public void validateBeforeDeploy(String name, Map<String, String> properties) throws AlreadyDeployedException, DefinitionAlreadyExistsException {
			}

			@Override
			public void validateBeforeUndeploy(String name) throws NoSuchDefinitionException, NotDeployedException {
			}

			@Override
			public void validateBeforeDelete(String name) throws NoSuchDefinitionException {
			}
		};
	}

	@Bean
	public AdminAttributes adminAttributes() {
		AdminAttributes adminAttributes = new AdminAttributes();
		adminAttributes.setHost(RuntimeUtils.getHost()).setIp(RuntimeUtils.getIpAddress()).setPid(
				RuntimeUtils.getPid());
		return adminAttributes;
	}

	@Bean
	public DeploymentSupervisor deploymentSupervisor() {
		return new DeploymentSupervisor(adminAttributes());
	}

	@Bean
	public ModuleDeploymentWriter moduleDeploymentWriter() {
		return new ModuleDeploymentWriter();
	}

	@Bean
	public DefaultDeploymentStateRecalculator stateCalculator() {
		return new DefaultDeploymentStateRecalculator();
	}

	@Bean
	public DeploymentQueue deploymentQueue() {
		return new DeploymentQueue(zkConnection);
	}

	@Bean
	public ZKDeploymentMessagePublisher deploymentMessageProducer() {
		return new ZKDeploymentMessagePublisher(deploymentQueue());
	}

	@Bean
	public DeploymentMessageConsumer deploymentMessageConsumer() {
		return new DeploymentMessageConsumer();
	}

	@Bean
	public AdminRepository adminRepository() {
		return new ZooKeeperAdminRepository(zkConnection);
	}

}
