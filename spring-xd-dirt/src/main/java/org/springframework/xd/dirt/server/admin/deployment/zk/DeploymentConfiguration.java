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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.AuditAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.xd.dirt.cluster.AdminAttributes;
import org.springframework.xd.dirt.container.store.AdminRepository;
import org.springframework.xd.dirt.container.store.ZooKeeperAdminRepository;
import org.springframework.xd.dirt.core.DefaultDeploymentValidator;
import org.springframework.xd.dirt.integration.bus.MessageBus;
import org.springframework.xd.dirt.job.JobFactory;
import org.springframework.xd.dirt.module.ModuleRegistry;
import org.springframework.xd.dirt.server.admin.deployment.DefaultDeploymentUnitStateCalculator;
import org.springframework.xd.dirt.server.admin.deployment.DeploymentUnitStateCalculator;
import org.springframework.xd.dirt.stream.JobDefinitionRepository;
import org.springframework.xd.dirt.stream.JobDeployer;
import org.springframework.xd.dirt.stream.JobRepository;
import org.springframework.xd.dirt.stream.ParsingContext;
import org.springframework.xd.dirt.stream.StreamDefinitionFactory;
import org.springframework.xd.dirt.stream.StreamDefinitionMigrator;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDeployer;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.stream.StreamRepository;
import org.springframework.xd.dirt.stream.XDStreamParser;
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
		AuditAutoConfiguration.class})
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
	public DeploymentUnitStateCalculator deploymentUnitStateCalculator() {
		return new DefaultDeploymentUnitStateCalculator();
	}

	@Bean
	public ZKStreamDeploymentHandler streamDeploymentHandler() {
		return new ZKStreamDeploymentHandler();
	}

	@Bean
	public DefaultDeploymentValidator streamDeploymentValidator() {
		return new DefaultDeploymentValidator(streamDefinitionRepository, streamRepository,
				parser(), ParsingContext.stream);
	}

	@Bean
	public StreamDeployer zkStreamDeployer() {
		return new StreamDeployer(zkConnection, parser(), streamDeploymentValidator(),
				streamDeploymentHandler());
	}

	@Bean
	public ZKJobDeploymentHandler jobDeploymentHandler() {
		return new ZKJobDeploymentHandler();
	}

	@Bean
	public DefaultDeploymentValidator jobDeploymentValidator() {
		return new DefaultDeploymentValidator(jobDefinitionRepository, jobRepository,
				parser(), ParsingContext.job);
	}

	@Bean
	public StreamDefinitionMigrator streamDefinitionMigrator() {
		return new StreamDefinitionMigrator(zkConnection, parser(), streamDefinitionRepository);
	}

	@Bean
	public JobDeployer zkJobDeployer() {
		return new JobDeployer(zkConnection, parser(), jobDeploymentValidator(),
				jobDeploymentHandler(), messageBus);
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

	@Bean
	public StreamDefinitionFactory streamDefinitionFactory() {
		return new StreamDefinitionFactory(parser());
	}

}
