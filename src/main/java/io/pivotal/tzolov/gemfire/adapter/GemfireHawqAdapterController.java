/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pivotal.tzolov.gemfire.adapter;

import java.io.IOException;
import java.io.Writer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GemfireHawqAdapterController {

	private GemfireHawqAdapterService adapterService;

	@Autowired
	public GemfireHawqAdapterController(GemfireHawqAdapterService adapterService) {
		this.adapterService = adapterService;
	}

	/**
	 * Run an unnamed (unidentified), ad-hoc query passed as a URL parameter.
	 * 
	 * @param oqlQuery
	 *            OQL query statement.
	 * @param responseWriter
	 *            HTTP response stream.
	 * @throws IOException
	 */
	@RequestMapping("/gemfire-api/v1/queries/adhoc")
	public void adhocQuery(@RequestParam(value = "q") String oqlQuery, Writer responseWriter) throws IOException {
		try {
			adapterService.runAdhocQuery(oqlQuery, responseWriter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Execute the specified named query passing in scalar values for query parameters in the POST body.
	 * 
	 * @param queryId
	 *            QueryID for named query.
	 * @param responseWriter
	 *            HTTP response stream.
	 * @throws IOException
	 */
	@RequestMapping(value = "/gemfire-api/v1/queries/{queryId}", method = RequestMethod.POST, consumes = "application/json")
	public void namedQuery(@PathVariable("queryId") String queryId, @RequestBody String requestBody, Writer responseWriter)
			throws IOException {
		try {
			adapterService.runNamedQuery(queryId, requestBody, responseWriter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
