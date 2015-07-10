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

package org.springframework.xd.dirt.rest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import io.pivotal.receptor.client.ReceptorClient;
import io.pivotal.receptor.commands.DesiredLRPCreateRequest;
import io.pivotal.receptor.support.EnvironmentVariable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.ExposesResourceFor;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.ResourceAssembler;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.stream.StreamDefinition;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.stream.XDParser;
import org.springframework.xd.module.ModuleDescriptor;
import org.springframework.xd.rest.domain.StreamDefinitionResource;

/**
 * @author Patrick Peralta
 */
@Controller
@RequestMapping("/streams")
@ExposesResourceFor(StreamDefinitionResource.class)
public class NewController {

	private final ResourceAssembler<StreamDefinition, StreamDefinitionResource> streamAssembler
			= new StreamDefinitionResourceAssembler();

	private final StreamDefinitionRepository repository = new InMemoryStreamDefinitionRepository();

	private final ModuleDeployer moduleDeployer = new ReceptorModuleDeployer();

	private final StreamFactory streamFactory;

	@Autowired
	public NewController(XDParser parser) {
		this.streamFactory = new StreamFactory(parser);
	}

	@ResponseBody
	@RequestMapping(value = "/definitions", method = RequestMethod.GET)
	@ResponseStatus(HttpStatus.OK)
	public PagedResources<StreamDefinitionResource> list(Pageable pageable,
			PagedResourcesAssembler<StreamDefinition> assembler) {
		Page<StreamDefinition> page = repository.findAll(pageable);
		return assembler.toResource(page, streamAssembler);
	}

	@RequestMapping(value = "/definitions", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.CREATED)
	public void save(@RequestParam("name") String name, @RequestParam("definition") String definition,
			@RequestParam(value = "deploy", defaultValue = "true") boolean deploy) throws Exception {
		repository.save(new StreamDefinition(name, definition));

		if (deploy) {
			Stream stream = streamFactory.createStream(name, Collections.singletonMap("definition", definition));
			for (Iterator<ModuleDescriptor> iterator = stream.getDeploymentOrderIterator(); iterator.hasNext();) {
				moduleDeployer.deploy(iterator.next());
			}
		}
	}

	@RequestMapping(value = "/definitions/{name}", method = RequestMethod.DELETE)
	@ResponseStatus(HttpStatus.OK)
	public void delete(@PathVariable("name") String name) throws Exception {
		StreamDefinition streamDefinition = repository.findOne(name);
		Stream stream = streamFactory.createStream(name, Collections.singletonMap("definition", streamDefinition.getDefinition()));
		for (ModuleDescriptor moduleDescriptor : stream.getModuleDescriptorsAsDeque()) {
			moduleDeployer.undeploy(moduleDescriptor);
		}
		repository.delete(name);
	}

	interface ModuleDeployer {
		void deploy(ModuleDescriptor descriptor);

		void undeploy(ModuleDescriptor descriptor);
	}

	class ReceptorModuleDeployer implements ModuleDeployer {
		public static final String DOCKER_PATH = "docker://192.168.59.103:5000/xd-module";

		public static final String BASE_ADDRESS = "192.168.11.11.xip.io";

		public static final String ADMIN_GUID = "xd-admin";

		private final ReceptorClient receptorClient = new ReceptorClient();

		@Override
		public void deploy(ModuleDescriptor descriptor) {
			String guid = guid(descriptor);
			DesiredLRPCreateRequest request = new DesiredLRPCreateRequest();
			request.setProcessGuid(guid);
			request.setRootfs(DOCKER_PATH);
			request.runAction().setPath("java");
			request.runAction().addArg("-Djava.security.egd=file:/dev/./urandom");
			request.runAction().addArg("-jar");
			request.runAction().addArg("/xd-module.jar");
			List<EnvironmentVariable> environmentVariables = new ArrayList<EnvironmentVariable>();
			for (EnvironmentVariable var : request.getEnv()) {
				environmentVariables.add(var);
			}
			environmentVariables.add(new EnvironmentVariable("XD_MODULE", path(descriptor)));
			Map<String, String> parameters = descriptor.getParameters();
			if (parameters != null && parameters.size() > 0) {
				for (Map.Entry<String, String> option : parameters.entrySet()) {
					environmentVariables.add(new EnvironmentVariable("OPTION_" + option.getKey(), option.getValue()));
				}
			}
			request.setEnv(environmentVariables.toArray(new EnvironmentVariable[environmentVariables.size()]));
			request.setPorts(new int[] {8080, 9000});
			request.addRoute(8080, new String[] {guid + "." + BASE_ADDRESS, guid + "-8080." + BASE_ADDRESS});
			request.addRoute(9000, new String[] {guid + "-9000." + BASE_ADDRESS});
			receptorClient.createDesiredLRP(request);
		}

		@Override
		public void undeploy(ModuleDescriptor descriptor) {
			receptorClient.deleteDesiredLRP(guid(descriptor));
		}

		private String guid(ModuleDescriptor descriptor) {
			return "xd-" + descriptor.getGroup() + "-" + descriptor.getModuleName() + "-" + descriptor.getIndex();
		}

		private String path(ModuleDescriptor descriptor) {
			return descriptor.getGroup() + "." + descriptor.getType() + "." + descriptor.getModuleName() + "." + descriptor.getIndex();
		}

	}


	class InMemoryStreamDefinitionRepository
			implements StreamDefinitionRepository {

		private final Map<String, StreamDefinition> map = new ConcurrentHashMap<>();

		@Override
		public Iterable<StreamDefinition> findAll(Sort sort) {
			return map.values();
		}

		@Override
		public Page<StreamDefinition> findAll(Pageable pageable) {
			return new PageImpl<>(new ArrayList<>(map.values()));
		}

		@Override
		public <S extends StreamDefinition> S save(S entity) {
			map.put(entity.getName(), entity);
			return entity;
		}

		@Override
		public <S extends StreamDefinition> Iterable<S> save(Iterable<S> entities) {
			List<S> list = new ArrayList<>();
			for (S entity : entities) {
				list.add(save(entity));
			}
			return list;
		}

		@Override
		public StreamDefinition findOne(String s) {
			return map.get(s);
		}

		@Override
		public boolean exists(String s) {
			return map.containsKey(s);
		}

		@Override
		public Iterable<StreamDefinition> findAll() {
			return map.values();
		}

		@Override
		public Iterable<StreamDefinition> findAll(Iterable<String> strings) {
			throw new UnsupportedOperationException();
		}

		@Override
		public long count() {
			return map.size();
		}

		@Override
		public void delete(String s) {
			map.remove(s);
		}

		@Override
		public void delete(StreamDefinition entity) {
			map.remove(entity.getName());
		}

		@Override
		public void delete(Iterable<? extends StreamDefinition> entities) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void deleteAll() {
			map.clear();
		}

		@Override
		public Iterable<StreamDefinition> findAllInRange(String from, boolean fromInclusive, String to, boolean toInclusive) {
			throw new UnsupportedOperationException();
		}
	}

}
