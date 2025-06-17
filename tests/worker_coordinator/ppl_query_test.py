# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import io
import json
import unittest.mock as mock
from unittest import TestCase

from osbenchmark.worker_coordinator import runner
from tests import run_async, as_future


class PplQueryRunnerTests(TestCase):
    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_end')
    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_start')
    @mock.patch("opensearchpy.OpenSearch")
    @run_async
    async def test_ppl_query_with_defaults(self, opensearch, on_client_request_start, on_client_request_end):
        ppl_response = {
            "schema": [
                {"name": "timestamp", "type": "timestamp"},
                {"name": "count", "type": "long"}
            ],
            "datarows": [
                ["2023-01-01T00:00:00.000Z", 100],
                ["2023-01-02T00:00:00.000Z", 200]
            ],
            "total": 2,
            "size": 2,
            "status": 200
        }
        opensearch.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(ppl_response)))

        ppl_query_runner = runner.PplQuery()

        params = {
            "body": {
                "query": "source=my_index | stats count(*) by timestamp"
            }
        }

        async with ppl_query_runner:
            result = await ppl_query_runner(opensearch, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])

        opensearch.transport.perform_request.assert_called_once_with(
            "POST",
            "/_plugins/_ppl",
            params={},
            body=params["body"],
            headers={}
        )

    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_end')
    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_start')
    @mock.patch("opensearchpy.OpenSearch")
    @run_async
    async def test_ppl_query_with_detailed_results(self, opensearch, on_client_request_start, on_client_request_end):
        ppl_response = {
            "schema": [
                {"name": "timestamp", "type": "timestamp"},
                {"name": "count", "type": "long"}
            ],
            "datarows": [
                ["2023-01-01T00:00:00.000Z", 100],
                ["2023-01-02T00:00:00.000Z", 200]
            ],
            "total": 2,
            "size": 2,
            "status": 200
        }
        opensearch.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(ppl_response)))

        ppl_query_runner = runner.PplQuery()

        params = {
            "body": {
                "query": "source=my_index | stats count(*) by timestamp"
            },
            "detailed-results": True
        }

        async with ppl_query_runner:
            result = await ppl_query_runner(opensearch, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])
        self.assertEqual(2, result["hits"])
        self.assertEqual(2, result["size"])
        self.assertEqual(200, result["status"])

        opensearch.transport.perform_request.assert_called_once_with(
            "POST",
            "/_plugins/_ppl",
            params={},
            body=params["body"],
            headers={}
        )

    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_end')
    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_start')
    @mock.patch("opensearchpy.OpenSearch")
    @run_async
    async def test_ppl_query_with_request_params(self, opensearch, on_client_request_start, on_client_request_end):
        ppl_response = {
            "schema": [
                {"name": "timestamp", "type": "timestamp"},
                {"name": "count", "type": "long"}
            ],
            "datarows": [
                ["2023-01-01T00:00:00.000Z", 100],
                ["2023-01-02T00:00:00.000Z", 200]
            ],
            "total": 2,
            "size": 2,
            "status": 200
        }
        opensearch.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(ppl_response)))

        ppl_query_runner = runner.PplQuery()

        params = {
            "body": {
                "query": "source=my_index | stats count(*) by timestamp"
            },
            "request-params": {
                "format": "jdbc"
            }
        }

        async with ppl_query_runner:
            result = await ppl_query_runner(opensearch, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])

        opensearch.transport.perform_request.assert_called_once_with(
            "POST",
            "/_plugins/_ppl",
            params={"format": "jdbc"},
            body=params["body"],
            headers={}
        )

    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_end')
    @mock.patch('osbenchmark.client.RequestContextHolder.on_client_request_start')
    @mock.patch("opensearchpy.OpenSearch")
    @run_async
    async def test_ppl_query_with_headers(self, opensearch, on_client_request_start, on_client_request_end):
        ppl_response = {
            "schema": [
                {"name": "timestamp", "type": "timestamp"},
                {"name": "count", "type": "long"}
            ],
            "datarows": [
                ["2023-01-01T00:00:00.000Z", 100],
                ["2023-01-02T00:00:00.000Z", 200]
            ],
            "total": 2,
            "size": 2,
            "status": 200
        }
        opensearch.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(ppl_response)))

        ppl_query_runner = runner.PplQuery()

        params = {
            "body": {
                "query": "source=my_index | stats count(*) by timestamp"
            },
            "headers": {
                "Content-Type": "application/json"
            }
        }

        async with ppl_query_runner:
            result = await ppl_query_runner(opensearch, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])

        opensearch.transport.perform_request.assert_called_once_with(
            "POST",
            "/_plugins/_ppl",
            params={},
            body=params["body"],
            headers={"Content-Type": "application/json"}
        )