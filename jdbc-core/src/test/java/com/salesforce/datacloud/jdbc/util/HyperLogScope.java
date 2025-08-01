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
package com.salesforce.datacloud.jdbc.util;

import com.salesforce.datacloud.jdbc.hyper.HyperTestBase;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

/**
 * This is a helper class to allow easy interaction with the hyper_log() function to verify
 * driver functionality by introspecting server side logs.
 *
 * It works by injecting a unique workload name to allow to easily correlate individual queries.
 */
public class HyperLogScope implements AutoCloseable {
    private final String id = "test-log-scope-" + UUID.randomUUID().toString();
    private Connection connection = null;
    private Statement statement = null;

    /**
     * Use this property object to initialize a connection so that the workload name is
     * propagated to the server side logs.
     */
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("workload", id);
        return properties;
    }

    /**
     * Use this method to verify that a query was executed with the expected workload name.
     */
    public String formatQuery(String query) throws SQLException {
        return "WITH hyper_log AS (\n" + "SELECT * FROM hyper_log(current_session:=false,last_log_scope:=false) \n"
                + "WHERE ctx->'workload'->>'name' = '"
                + id + "' OR v->'headers'->>'x-hyperdb-workload' = '" + id + "'\n" + ") "
                + query;
    }

    /**
     * Execute a query on this log scope and return the result set. You can access the log entries by leveraging
     * the injected `hyper_log` CTE.
     *
     * @param query The query to execute.
     * @return The result set.
     * @throws SQLException If an error occurs.
     */
    public ResultSet executeQuery(String query) throws SQLException {
        // Ensure that we have a connection
        if (connection == null) {
            connection = HyperTestBase.getHyperQueryConnection(new Properties());
        }
        // Close the potential statement from previous calls
        if (statement != null) {
            statement.close();
        }

        statement = connection.createStatement();
        return statement.executeQuery(formatQuery(query));
    }

    @Override
    public void close() throws SQLException {
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
