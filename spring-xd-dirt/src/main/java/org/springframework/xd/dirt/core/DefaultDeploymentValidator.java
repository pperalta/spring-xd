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

package org.springframework.xd.dirt.core;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.Assert;
import org.springframework.xd.dirt.stream.AlreadyDeployedException;
import org.springframework.xd.dirt.stream.BaseInstance;
import org.springframework.xd.dirt.stream.DefinitionAlreadyExistsException;
import org.springframework.xd.dirt.stream.NoSuchDefinitionException;
import org.springframework.xd.dirt.stream.NotDeployedException;
import org.springframework.xd.dirt.stream.ParsingContext;
import org.springframework.xd.dirt.stream.XDParser;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.store.DomainRepository;

/**
 * Implementation of {@link DeploymentValidator} which uses {@link DomainRepository}
 * to validate pre-conditions required for various operations.
 *
 * @author Patrick Peralta
 */
public class DefaultDeploymentValidator implements DeploymentValidator {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * Pattern used for parsing a single deployment property key. Group 1 is the
	 * module name, Group 2 is the deployment property name.
	 */
	private static final Pattern DEPLOYMENT_PROPERTY_PATTERN = Pattern.compile("module\\.([^\\.]+)\\.([^=]+)");

	/**
	 * Domain repository for stream/job definitions.
	 */
	private final DomainRepository<? extends BaseDefinition, String> definitionRepo;

	/**
	 * Instance repository for stream/job definitions.
	 */
	private final DomainRepository<? extends BaseInstance, String> instanceRepo;

	/**
	 * DSL parser.
	 */
	private final XDParser parser;

	/**
	 * Parsing context (stream, job, etc) for parsing the DSL.
	 */
	private final ParsingContext definitionKind;

	/**
	 * Construct a DefaultDeploymentValidator.
	 *
	 * @param definitionRepo  domain repository for streams/jobs
	 * @param instanceRepo    instance repository for streams/jobs
	 * @param parser          DSL parser
	 * @param definitionKind  DSL parsing context (stream, job, etc)
	 */
	public DefaultDeploymentValidator(DomainRepository<? extends BaseDefinition, String> definitionRepo,
			DomainRepository<? extends BaseInstance, String> instanceRepo,
			XDParser parser, ParsingContext definitionKind) {
		this.definitionRepo = definitionRepo;
		this.instanceRepo = instanceRepo;
		this.parser = parser;
		this.definitionKind = definitionKind;
	}

	@Override
	public void validateBeforeSave(String name, String definitionString) throws DefinitionAlreadyExistsException {
		Assert.hasText(name, "name cannot be blank or null");
		BaseDefinition definition = definitionRepo.findOne(name);
		if (definition != null) {
			throw new DefinitionAlreadyExistsException(definition.getName(),
					String.format("There is already a %s named '%%s'", definitionKind));
		}
		Assert.notNull(definitionString, "Definition may not be null");
		parser.parse(name, definitionString, definitionKind);
	}

	@Override
	public void validateBeforeDeploy(String name, Map<String, String> properties)
			throws AlreadyDeployedException, DefinitionAlreadyExistsException {
		Assert.hasText(name, "name cannot be blank or null");
		Assert.notNull(properties, "properties cannot be null");
		BaseDefinition definition = definitionRepo.findOne(name);
		if (definition == null) {
			throwNoSuchDefinitionException(name);
		}
		validateDeploymentProperties(definition, properties);
		if (instanceRepo.exists(name)) {
			throw new AlreadyDeployedException(name,
					String.format("The %s named '%%s' is already deployed", definitionKind));
		}
	}

	/**
	 * Validates that all deployment properties of the form
	 * {@code module.<modulename>.<key>} reference module names
	 * that belong to the stream/job definition).
	 */
	private void validateDeploymentProperties(BaseDefinition definition, Map<String, String> properties) {
		List<ModuleDescriptor> modules = parser.parse(definition.getName(), definition.getDefinition(), definitionKind);
		Set<String> moduleLabels = new HashSet<String>(modules.size());
		for (ModuleDescriptor md : modules) {
			moduleLabels.add(md.getModuleLabel());
		}
		for (Map.Entry<String, String> pair : properties.entrySet()) {
			Matcher matcher = DEPLOYMENT_PROPERTY_PATTERN.matcher(pair.getKey());
			Assert.isTrue(matcher.matches(),
					String.format("'%s' does not match '%s'", pair.getKey(), DEPLOYMENT_PROPERTY_PATTERN));
			String moduleName = matcher.group(1);
			Assert.isTrue("*".equals(moduleName) || moduleLabels.contains(moduleName),
					String.format("'%s' refers to a module that is not in the list: %s", pair.getKey(), moduleLabels));
		}
	}

	@Override
	public void validateDeployed(String name) throws NoSuchDefinitionException, NotDeployedException {
		Assert.hasText(name, "name cannot be blank or null");

		if (definitionRepo.findOne(name) == null) {
			throwNoSuchDefinitionException(name);
		}

		if (instanceRepo.findOne(name) == null) {
			throw new NotDeployedException(name,
					String.format("The %s named '%%s' is not currently deployed", definitionKind));
		}
	}

	@Override
	public void validateBeforeUndeploy(String name) throws NoSuchDefinitionException, NotDeployedException {
		validateDeployed(name);
	}

	@Override
	public void validateBeforeDelete(String name) throws NoSuchDefinitionException {
		logger.warn("validateBeforeDelete({})", name);
		if (definitionRepo.findOne(name) == null) {
			throwNoSuchDefinitionException(name);
		}
		else {
			logger.warn("validateBeforeDelete({}) found in repo {}", name, definitionRepo.findOne(name));
		}
	}

	protected void throwNoSuchDefinitionException(String name) {
		throw new NoSuchDefinitionException(name,
				String.format("There is no %s definition named '%%s'", definitionKind));
	}

}
