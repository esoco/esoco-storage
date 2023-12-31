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

	private final String sConnectURL;

	private final Properties aConnectionProperties = new Properties();

	/**
	 * Creates a new instance with a certain JDBC connection URL and connection
	 * properties.
	 *
	 * @param sConnectURL The URL for the JDBC connection
	 * @param rProperties The connection properties
	 */
	JdbcDriverStorageDefinition(String sConnectURL, Properties rProperties) {
		if (sConnectURL == null) {
			throw new IllegalArgumentException("URL");
		}

		this.sConnectURL = sConnectURL;

		if (rProperties != null) {
			aConnectionProperties.putAll(rProperties);
		}
	}

	/**
	 * Two JDBC storage definitions are considered equal if their URLs, user
	 * names, and identity datatypes are equal.
	 *
	 * @see StorageDefinition#equals(Object)
	 */
	@Override
	public boolean equals(Object rObject) {
		if (this == rObject) {
			return true;
		}

		if (rObject == null || getClass() != rObject.getClass()) {
			return false;
		}

		JdbcDriverStorageDefinition rOther =
			(JdbcDriverStorageDefinition) rObject;

		return sConnectURL.equals(rOther.sConnectURL) &&
			aConnectionProperties.equals(rOther.aConnectionProperties);
	}

	/**
	 * @see StorageDefinition#hashCode()
	 */
	@Override
	public int hashCode() {
		return sConnectURL.hashCode() * 37 + aConnectionProperties.hashCode();
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "JdbcDriverStorageDefinition [sConnectURL=" + sConnectURL + "]";
	}

	/**
	 * Creates a new JDBC-specific storage instance from this definition.
	 *
	 * @see StorageDefinition#createStorage()
	 */
	@Override
	protected Storage createStorage() throws StorageException {
		try {
			Connection aConnection =
				DriverManager.getConnection(sConnectURL,
					aConnectionProperties);

			return new JdbcStorage(aConnection,
				getDatabaseParameters(aConnection));
		} catch (SQLException e) {
			throw new StorageException("Storage creation failed", e);
		}
	}
}
