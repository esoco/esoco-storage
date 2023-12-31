//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2015 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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

import de.esoco.storage.Storage;
import de.esoco.storage.StorageDefinition;
import de.esoco.storage.StorageException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A JDBC storage definition that uses the {@link DriverManager} to create
 * database connections.
 *
 * @author eso
 */
class JdbcDriverStorageDefinition extends JdbcStorageDefinition {

	private static final long serialVersionUID = 1L;

	private final String connectURL;

	private final Properties connectionProperties = new Properties();

	/**
	 * Creates a new instance with a certain JDBC connection URL and connection
	 * properties.
	 *
	 * @param connectURL The URL for the JDBC connection
	 * @param properties The connection properties
	 */
	JdbcDriverStorageDefinition(String connectURL, Properties properties) {
		if (connectURL == null) {
			throw new IllegalArgumentException("URL");
		}

		this.connectURL = connectURL;

		if (properties != null) {
			connectionProperties.putAll(properties);
		}
	}

	/**
	 * Two JDBC storage definitions are considered equal if their URLs, user
	 * names, and identity datatypes are equal.
	 *
	 * @see StorageDefinition#equals(Object)
	 */
	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}

		if (object == null || getClass() != object.getClass()) {
			return false;
		}

		JdbcDriverStorageDefinition other =
			(JdbcDriverStorageDefinition) object;

		return connectURL.equals(other.connectURL) &&
			connectionProperties.equals(other.connectionProperties);
	}

	/**
	 * @see StorageDefinition#hashCode()
	 */
	@Override
	public int hashCode() {
		return connectURL.hashCode() * 37 + connectionProperties.hashCode();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JdbcDriverStorageDefinition [connectURL=" + connectURL + "]";
	}

	/**
	 * Creates a new JDBC-specific storage instance from this definition.
	 *
	 * @see StorageDefinition#createStorage()
	 */
	@Override
	protected Storage createStorage() throws StorageException {
		try {
			Connection connection =
				DriverManager.getConnection(connectURL, connectionProperties);

			return new JdbcStorage(connection,
				getDatabaseParameters(connection));
		} catch (SQLException e) {
			throw new StorageException("Storage creation failed", e);
		}
	}
}
