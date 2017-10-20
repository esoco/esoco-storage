//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2017 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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

import de.esoco.storage.StorageDefinition;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.Properties;

import javax.sql.DataSource;

import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_AUTO_IDENTITY_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DATATYPE_MAP;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_FUZZY_SEARCH_FUNCTION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_IDENTITIFIER_QUOTE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_QUERY_LIMIT_EXPRESSION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_QUERY_PAGING_EXPRESSION;


/********************************************************************
 * A storage definition implementation for JDBC connections to SQL databases. It
 * contains {@link #create(String)} factory methods that create instances for
 * different JDBC connection lookup methods.
 *
 * @author eso
 */
public abstract class JdbcStorageDefinition extends StorageDefinition
{
	//~ Static fields/initializers ---------------------------------------------

	static
	{
		JdbcRelationTypes.init();
	}

	private static final long serialVersionUID = 1L;

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Factory method that creates a new instance from a JDBC data source.
	 *
	 * @param  rDataSource The JDBC data source
	 *
	 * @return The new storage definition
	 */
	public static JdbcStorageDefinition create(DataSource rDataSource)
	{
		return new JdbcDataSourceStorageDefinition(rDataSource);
	}

	/***************************************
	 * Factory method that creates a new instance from a JDBC connection URL.
	 *
	 * @param  sConnectURL The URL for the JDBC connection
	 *
	 * @return The new storage definition
	 */
	public static JdbcDriverStorageDefinition create(String sConnectURL)
	{
		return new JdbcDriverStorageDefinition(sConnectURL, null);
	}

	/***************************************
	 * Factory method that creates a new instance from a JDBC connection URL and
	 * connection properties.
	 *
	 * @param  sConnectURL The URL for the JDBC connection
	 * @param  rProperties The connection properties
	 *
	 * @return The new storage definition
	 */
	public static JdbcDriverStorageDefinition create(
		String	   sConnectURL,
		Properties rProperties)
	{
		return new JdbcDriverStorageDefinition(sConnectURL, rProperties);
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Returns the database parameters for a certain JBDC connection.
	 *
	 * @param  rConnection The database connection
	 *
	 * @return The database parameters or NULL to use default values
	 *
	 * @throws SQLException if accessing the connection fails
	 */
	@SuppressWarnings("boxing")
	Relatable getDatabaseParameters(Connection rConnection) throws SQLException
	{
		Relatable aParameters = new RelatedObject();

		String sDatabaseName =
			rConnection.getMetaData().getDatabaseProductName().toLowerCase();

		if (sDatabaseName.contains("postgres"))
		{
			aParameters.set(SQL_AUTO_IDENTITY_DATATYPE, "SERIAL");
			aParameters.set(SQL_FUZZY_SEARCH_FUNCTION, "dmetaphone");
			aParameters.set(SQL_QUERY_PAGING_EXPRESSION, null);
			aParameters.set(SQL_QUERY_LIMIT_EXPRESSION, null);
			aParameters.get(SQL_DATATYPE_MAP).put(String.class, "TEXT");
			aParameters.get(SQL_DATATYPE_MAP).put(byte[].class, "BYTEA");
		}
		else if (sDatabaseName.contains("mysql") ||
				 sDatabaseName.contains("mariadb"))
		{
			aParameters.set(SQL_IDENTITIFIER_QUOTE, '`');
			aParameters.set(SQL_FUZZY_SEARCH_FUNCTION, "soundex");
			aParameters.get(SQL_DATATYPE_MAP).put(String.class, "TEXT");
		}
		else if (sDatabaseName.contains("h2"))
		{
			aParameters.set(SQL_FUZZY_SEARCH_FUNCTION, "soundex");
		}

		String sFuzzyFunction = System.getProperty("storage.sql.fuzzy");

		if (sFuzzyFunction != null)
		{
			aParameters.set(SQL_FUZZY_SEARCH_FUNCTION, sFuzzyFunction);
		}

		return aParameters;
	}
}
