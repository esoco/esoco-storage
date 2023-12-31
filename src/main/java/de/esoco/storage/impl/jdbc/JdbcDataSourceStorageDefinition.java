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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A JDBC storage definition that uses a {@link DataSource} to create database
 * connections.
 *
 * @author eso
 */
class JdbcDataSourceStorageDefinition extends JdbcStorageDefinition {

	private static final long serialVersionUID = 1L;

	private final DataSource dataSource;

	/**
	 * Creates a new instance with a particular JDBC data source.
	 *
	 * @param dataSource The JDBC data source
	 */
	JdbcDataSourceStorageDefinition(DataSource dataSource) {
		this.dataSource = dataSource;
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

		JdbcDataSourceStorageDefinition other =
			(JdbcDataSourceStorageDefinition) object;

		return dataSource.equals(other.dataSource);
	}

	/**
	 * @see StorageDefinition#hashCode()
	 */
	@Override
	public int hashCode() {
		return 37 * dataSource.hashCode();
	}

	/**
	 * Creates a new JDBC-specific storage instance from this definition.
	 *
	 * @see StorageDefinition#createStorage()
	 */
	@Override
	protected Storage createStorage() throws StorageException {
		try {
			Connection connection = dataSource.getConnection();

			JdbcStorage storage =
				new JdbcStorage(connection, getDatabaseParameters(connection));

			return storage;
		} catch (SQLException e) {
			throw new StorageException("Storage creation failed", e);
		}
	}
}
