//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2019 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
package de.esoco.storage.impl.jdbc;

import de.esoco.lib.json.JsonObject;

import de.esoco.storage.StorageDefinition;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_AUTO_IDENTITY_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DATATYPE_MAP;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_FUZZY_SEARCH_FUNCTION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_IDENTITIFIER_QUOTE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_LONG_AUTO_IDENTITY_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_QUERY_LIMIT_EXPRESSION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_QUERY_PAGING_EXPRESSION;

/**
 * A storage definition implementation for JDBC connections to SQL databases. It
 * contains {@link #create(String)} factory methods that create instances for
 * different JDBC connection lookup methods.
 *
 * @author eso
 */
public abstract class JdbcStorageDefinition extends StorageDefinition {

	private static final long serialVersionUID = 1L;

	static {
		JdbcRelationTypes.init();
	}

	/**
	 * Factory method that creates a new instance from a JDBC data source.
	 *
	 * @param dataSource The JDBC data source
	 * @return The new storage definition
	 */
	public static JdbcStorageDefinition create(DataSource dataSource) {
		return new JdbcDataSourceStorageDefinition(dataSource);
	}

	/**
	 * Factory method that creates a new instance from a JDBC connection URL.
	 *
	 * @param connectURL The URL for the JDBC connection
	 * @return The new storage definition
	 */
	public static JdbcDriverStorageDefinition create(String connectURL) {
		return new JdbcDriverStorageDefinition(connectURL, null);
	}

	/**
	 * Factory method that creates a new instance from a JDBC connection URL
	 * and
	 * connection properties.
	 *
	 * @param connectURL The URL for the JDBC connection
	 * @param properties The connection properties
	 * @return The new storage definition
	 */
	public static JdbcDriverStorageDefinition create(String connectURL,
		Properties properties) {
		return new JdbcDriverStorageDefinition(connectURL, properties);
	}

	/**
	 * Returns the database parameters for a certain JBDC connection.
	 *
	 * @param connection The database connection
	 * @return The database parameters or NULL to use default values
	 * @throws SQLException if accessing the connection fails
	 */
	@SuppressWarnings("boxing")
	protected Relatable getDatabaseParameters(Connection connection)
		throws SQLException {
		Relatable parameters = new RelatedObject();
		String fuzzyFunction = System.getProperty("storage.sql.fuzzy");

		if (fuzzyFunction == null) {
			fuzzyFunction = "soundex";
		}

		String databaseName =
			connection.getMetaData().getDatabaseProductName().toLowerCase();

		Map<Class<?>, String> datatypeMap = parameters.get(SQL_DATATYPE_MAP);

		if (databaseName.contains("postgres")) {
			fuzzyFunction = "dmetaphone";

			parameters.set(SQL_AUTO_IDENTITY_DATATYPE, "SERIAL");
			parameters.set(SQL_LONG_AUTO_IDENTITY_DATATYPE, "BIGSERIAL");
			parameters.set(SQL_QUERY_PAGING_EXPRESSION, null);
			parameters.set(SQL_QUERY_LIMIT_EXPRESSION, null);

			datatypeMap.put(String.class, "TEXT");
			datatypeMap.put(byte[].class, "BYTEA");
			datatypeMap.put(Map.class, "HSTORE");
			datatypeMap.put(JsonObject.class, "JSONB");
		} else if (databaseName.contains("mysql") ||
			databaseName.contains("mariadb")) {
			parameters.set(SQL_IDENTITIFIER_QUOTE, '`');
			datatypeMap.put(String.class, "TEXT");
		}

		parameters.set(SQL_FUZZY_SEARCH_FUNCTION, fuzzyFunction);

		return parameters;
	}
}
