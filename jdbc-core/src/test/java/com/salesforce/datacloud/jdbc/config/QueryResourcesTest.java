/*
 * Copyright (c) 2024, Salesforce, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.datacloud.jdbc.config;

import static org.assertj.core.api.Assertions.assertThat;

import lombok.val;
import org.junit.jupiter.api.Test;

class QueryResourcesTest {

    @Test
    void getColumnsQuery() {
        val actual = QueryResources.getColumnsQueryText();
        assertThat(actual)
                .contains("SELECT n.nspname,")
                .contains("FROM pg_catalog.pg_namespace n")
                .contains("WHERE c.relkind in ('r', 'p', 'v', 'f', 'm')");
    }

    @Test
    void getSchemasQuery() {
        val actual = QueryResources.getSchemasQueryText();
        assertThat(actual)
                .contains("SELECT nspname")
                .contains("FROM pg_catalog.pg_namespace")
                .contains("WHERE nspname");
    }

    @Test
    void getTablesQuery() {
        val actual = QueryResources.getTablesQueryText();
        assertThat(actual)
                .contains("SELECT")
                .contains("FROM pg_catalog.pg_namespace")
                .contains("LEFT JOIN pg_catalog.pg_description d ON");
    }
}
