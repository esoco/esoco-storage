//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2018 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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

import de.esoco.lib.datatype.GenericEnum;
import de.esoco.lib.datatype.ObjectId;
import de.esoco.lib.datatype.Period;
import de.esoco.lib.logging.Log;
import de.esoco.lib.property.HasOrder;
import de.esoco.lib.text.TextUtil;

import de.esoco.storage.Query;
import de.esoco.storage.QueryPredicate;
import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.StorageMapping;
import de.esoco.storage.StorageRelationTypes;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelationType;
import org.obrel.type.MetaTypes;
import org.obrel.type.StandardTypes;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.STORAGE_DATATYPE;
import static de.esoco.storage.StorageRelationTypes.STORAGE_LENGTH;
import static de.esoco.storage.StorageRelationTypes.STORAGE_MAPPING;
import static de.esoco.storage.StorageRelationTypes.STORAGE_NAME;
import static de.esoco.storage.StorageRelationTypes.STORING;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_AUTO_IDENTITY_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_CHILD_COUNT_COLUMN;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_CREATE_STATEMENT;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DATATYPE_MAP;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DISABLE_CHILD_COUNTS;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_FUZZY_SEARCH_FUNCTION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_IDENTITIFIER_QUOTE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_LONG_AUTO_IDENTITY_DATATYPE;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_NAME;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_OMIT_NAMESPACE;

import static org.obrel.core.ObjectRelations.getRelatable;
import static org.obrel.type.MetaTypes.AUTOGENERATED;
import static org.obrel.type.MetaTypes.INDEXED;
import static org.obrel.type.MetaTypes.MANDATORY;
import static org.obrel.type.MetaTypes.MODIFIED;
import static org.obrel.type.MetaTypes.OBJECT_ID_ATTRIBUTE;
import static org.obrel.type.MetaTypes.PARENT_ATTRIBUTE;
import static org.obrel.type.MetaTypes.UNIQUE;


/********************************************************************
 * A storage implementation that uses a JDBC connection to store objects.
 *
 * @author eso
 */
public class JdbcStorage extends Storage
{
	//~ Static fields/initializers ---------------------------------------------

	private static final String DEFAULT_STRING_DATATYPE = "VARCHAR(1000)";

	private static final String INSERT_TEMPLATE =
		"INSERT INTO %s (%s) VALUES (%s)";

	private static final String UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE %s";

	private static final String DELETE_TEMPLATE =
		"DELETE FROM %s WHERE %s = %s";

	private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (%s)";

	private static final String DROP_TABLE_TEMPLATE = "DROP TABLE %s";

	private static final String PRIMARY_KEY_TEMPLATE = "PRIMARY KEY(%s),";

	private static final String FOREIGN_KEY_TEMPLATE =
		"FOREIGN KEY(%s) REFERENCES %s(%s),";

	private static final String INDEX_TEMPLATE =
		"CREATE INDEX idx_%1$s_%2$s ON \"%1$s\"(\"%2$s\")";

	private static final String CHILD_COUNT_PREFIX = "_cc_";

	private static final Map<Class<?>, String> STANDARD_SQL_DATATYPE_MAP;

	static
	{
		Map<Class<?>, String> aSqlDatatypeMap = new HashMap<Class<?>, String>();

		aSqlDatatypeMap.put(Byte.class, "TINYINT");
		aSqlDatatypeMap.put(byte.class, "TINYINT");
		aSqlDatatypeMap.put(byte[].class, "VARBINARY(%d)");
		aSqlDatatypeMap.put(Short.class, "SHORT");
		aSqlDatatypeMap.put(short.class, "SHORT");
		aSqlDatatypeMap.put(Integer.class, "INTEGER");
		aSqlDatatypeMap.put(int.class, "INTEGER");
		aSqlDatatypeMap.put(Long.class, "BIGINT");
		aSqlDatatypeMap.put(long.class, "BIGINT");
		aSqlDatatypeMap.put(Float.class, "REAL");
		aSqlDatatypeMap.put(float.class, "REAL");
		aSqlDatatypeMap.put(Double.class, "DOUBLE");
		aSqlDatatypeMap.put(double.class, "DOUBLE");
		aSqlDatatypeMap.put(Boolean.class, "BOOLEAN");
		aSqlDatatypeMap.put(boolean.class, "BOOLEAN");
		aSqlDatatypeMap.put(BigInteger.class, "DECIMAL(1000)");
		aSqlDatatypeMap.put(BigDecimal.class, "DECIMAL");
		aSqlDatatypeMap.put(String.class, "VARCHAR(%d)");
		aSqlDatatypeMap.put(Enum.class, "VARCHAR(255)");
		aSqlDatatypeMap.put(Class.class, "VARCHAR(511)");
		aSqlDatatypeMap.put(RelationType.class, "VARCHAR(511)");
		aSqlDatatypeMap.put(Period.class, "VARCHAR(255)");
		aSqlDatatypeMap.put(java.util.Date.class, "TIMESTAMP");
		aSqlDatatypeMap.put(java.sql.Timestamp.class, "TIMESTAMP");
		aSqlDatatypeMap.put(java.sql.Date.class, "DATE");
		aSqlDatatypeMap.put(java.sql.Time.class, "TIME");
		aSqlDatatypeMap.put(List.class, "VARCHAR(%d)");
		aSqlDatatypeMap.put(Set.class, "VARCHAR(%d)");
		aSqlDatatypeMap.put(Map.class, "VARCHAR(%d)");

		STANDARD_SQL_DATATYPE_MAP =
			Collections.unmodifiableMap(aSqlDatatypeMap);
	}

	private static int nNextId = 1;

	//~ Instance fields --------------------------------------------------------

	private Connection rConnection;
	private String     sDatabaseName;
	private String     sFuzzySearchFunction;
	private char	   cIdentifierQuote;

	private Map<Class<?>, String> aDatatypeMap = STANDARD_SQL_DATATYPE_MAP;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Creates a new instance for a particular JDBC connection URL. The
	 * database-specific storage parameters must be given as a {@link Relatable}
	 * object that contains the parameters as relations.
	 *
	 * @param  rConnection The JDBC connection
	 * @param  rParams     The database-specific storage parameters
	 *
	 * @throws SQLException If creating the connection fails
	 */
	@SuppressWarnings("boxing")
	JdbcStorage(Connection rConnection, Relatable rParams) throws SQLException
	{
		this.rConnection = rConnection;
		sDatabaseName    = rConnection.getMetaData().getDatabaseProductName();

		set(StandardTypes.OBJECT_ID, ObjectId.intId(nNextId++));

		rConnection.setAutoCommit(false);

		sFuzzySearchFunction = rParams.get(SQL_FUZZY_SEARCH_FUNCTION);
		cIdentifierQuote     = rParams.get(SQL_IDENTITIFIER_QUOTE);

		ObjectRelations.copyRelations(rParams, this, false);

		if (!rParams.get(SQL_DATATYPE_MAP).isEmpty())
		{
			aDatatypeMap =
				new HashMap<Class<?>, String>(STANDARD_SQL_DATATYPE_MAP);

			aDatatypeMap.putAll(rParams.get(SQL_DATATYPE_MAP));
			aDatatypeMap = Collections.unmodifiableMap(aDatatypeMap);
		}
	}

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Checks whether the given database contains a certain table.
	 *
	 * @param  rConnection The database connection to check
	 * @param  sTable      The name of the table
	 *
	 * @return TRUE if the table exists in the database
	 *
	 * @throws StorageException If the metadata query fails
	 */
	static boolean containsTable(Connection rConnection, String sTable)
		throws StorageException
	{
		try
		{
			DatabaseMetaData rMetaData = rConnection.getMetaData();

			ResultSet rTables = rMetaData.getTables(null, null, sTable, null);
			boolean   bExists = rTables.next();

			rTables.close();

			Log.debug(sTable + (bExists ? " exists" : " doesn't exist"));

			return bExists;
		}
		catch (SQLException e)
		{
			throw new StorageException("Could not access table metadata", e);
		}
	}

	/***************************************
	 * Internal method to format a certain SQL statement.
	 *
	 * @param  sFormat The format string for the statement
	 * @param  rArgs   The objects to be inserted into the statement
	 *
	 * @return The resulting statement string
	 */
	static String formatStatement(String sFormat, Object... rArgs)
	{
		return String.format(sFormat, rArgs);
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * @see Storage#commit()
	 */
	@Override
	public void commit() throws StorageException
	{
		try
		{
			rConnection.commit();
		}
		catch (SQLException e)
		{
			throw new StorageException("Commit failed", e);
		}
	}

	/***************************************
	 * @see Storage#deleteObject(Object)
	 */
	@Override
	public void deleteObject(Object rObject) throws StorageException
	{
		@SuppressWarnings("unchecked")
		StorageMapping<Object, Relatable, ?> rMapping =
			(StorageMapping<Object, Relatable, ?>) StorageManager.getMapping(rObject
																			 .getClass());

		Relatable rIdAttribute = rMapping.getIdAttribute();
		String    sTable	   = getSqlName(rMapping, true);
		String    sIdAttr	   = getSqlName(rIdAttribute, true);

		Object rId = rMapping.getAttributeValue(rObject, rIdAttribute);

		String sSql = formatStatement(DELETE_TEMPLATE, sTable, sIdAttr, rId);

		Log.debug(sSql);
		executeUpdate(sSql, null, null, false, false);
	}

	/***************************************
	 * Returns the JDBC connection that is used by this instance.
	 *
	 * @return The JDBC connection of this instance
	 */
	public final Connection getConnection()
	{
		return rConnection;
	}

	/***************************************
	 * Returns the fuzzy search function.
	 *
	 * @return The fuzzy search function
	 */
	public String getFuzzySearchFunction()
	{
		return sFuzzySearchFunction;
	}

	/***************************************
	 * @see Storage#getStorageImplementationName()
	 */
	@Override
	public String getStorageImplementationName()
	{
		return sDatabaseName;
	}

	/***************************************
	 * @see Storage#isValid()
	 */
	@Override
	public boolean isValid()
	{
		try
		{
			// not yet supported by Postgres: rConnection.isValid(0);
			return !rConnection.isClosed();
		}
		catch (SQLException e)
		{
			Log.warn("JdbcStorage.isValid() failed", e);

			return false;
		}
	}

	/***************************************
	 * @see Storage#query(QueryPredicate)
	 */
	@Override
	public <T> Query<T> query(QueryPredicate<T> rQueryPredicate)
		throws StorageException
	{
		return new JdbcQuery<T>(this, rQueryPredicate);
	}

	/***************************************
	 * @see Storage#rollback()
	 */
	@Override
	public void rollback() throws StorageException
	{
		try
		{
			rConnection.rollback();
		}
		catch (SQLException e)
		{
			throw new StorageException("Commit failed", e);
		}
	}

	/***************************************
	 * @see Object#toString()
	 */
	@Override
	public String toString()
	{
		return "JdbcStorage[" + rConnection + "]";
	}

	/***************************************
	 * @see Storage#close()
	 */
	@Override
	protected void close()
	{
		try
		{
			rConnection.rollback();
		}
		catch (SQLException e)
		{
			Log.warn("Closing JDBC connection failed", e);
		}

		try
		{
			rConnection.close();
		}
		catch (SQLException e)
		{
			Log.warn("Closing JDBC connection failed", e);
		}

		rConnection = null;
	}

	/***************************************
	 * {@inheritDoc}
	 */
	@Override
	protected boolean hasObjectStorage(StorageMapping<?, ?, ?> rMapping)
		throws StorageException
	{
		return containsTable(rConnection, getSqlName(rMapping, false));
	}

	/***************************************
	 * Implemented to create the database tables for the given mapping. If the
	 * mapping contains child-mappings the child tables will also be created.
	 * But the tables will only be created if the table for the top-level
	 * mapping doesn't exist already.
	 *
	 * @see Storage#initObjectStorage(StorageMapping)
	 */
	@Override
	protected void initObjectStorage(StorageMapping<?, ?, ?> rMapping)
		throws StorageException
	{
		if (!containsTable(rConnection, getSqlName(rMapping, false)))
		{
			String sCreateStatement = rMapping.get(SQL_CREATE_STATEMENT);

			if (sCreateStatement != null)
			{
				executeUpdate(sCreateStatement, null, null, false, false);
			}
			else
			{
				createTable(rMapping);
			}

			for (StorageMapping<?, ?, ?> rChildMapping :
				 rMapping.getChildMappings())
			{
				// create child tables, but only if not self-referencing
				if (rChildMapping != rMapping)
				{
					initObjectStorage(rChildMapping);
				}
			}
		}
	}

	/***************************************
	 * {@inheritDoc}
	 */
	@Override
	protected void removeObjectStorage(StorageMapping<?, ?, ?> rMapping)
		throws StorageException
	{
		if (containsTable(rConnection, getSqlName(rMapping, false)))
		{
			String sDropStatement =
				String.format(DROP_TABLE_TEMPLATE, getSqlName(rMapping, true));

			executeUpdate(sDropStatement, null, null, false, false);
		}
	}

	/***************************************
	 * Stores the hierarchy of a single object in the database. That means that
	 * the object and all of it's children will be stored. This method will be
	 * invoked recursively (from {@link #store(Object)}) for each level of child
	 * objects.
	 *
	 * @param  rObject The object to store
	 *
	 * @throws Exception If storing the object fails
	 */
	@Override
	protected void storeObject(Object rObject) throws Exception
	{
		Relatable rRelatable = getRelatable(rObject);
		boolean   bInsert    = !rRelatable.hasFlag(PERSISTENT);

		@SuppressWarnings("unchecked")
		StorageMapping<Object, Relatable, StorageMapping<?, Relatable, ?>> rMapping =
			(StorageMapping<Object, Relatable, StorageMapping<?, Relatable, ?>>)
			StorageManager.getMapping(rObject.getClass());

		// stores referenced objects first to make the generated IDs of new
		// objects available for storeAttributes()
		storeReferences(rRelatable, rMapping);

		if (needsToBeStored(rRelatable))
		{
			storeAttributes(rMapping, rObject, bInsert);
		}

		for (StorageMapping<?, Relatable, ?> rChildMapping :
			 rMapping.getChildMappings())
		{
			store(rMapping.getChildren(rObject, rChildMapping));
		}
	}

	/***************************************
	 * Returns the child count column.
	 *
	 * @param  rChildMapping The child count column
	 *
	 * @return The child count column
	 */
	String getChildCountColumn(StorageMapping<?, ?, ?> rChildMapping)
	{
		String sColumn = rChildMapping.get(SQL_CHILD_COUNT_COLUMN);

		if (sColumn == null)
		{
			sColumn = getSqlName(rChildMapping, true);
			sColumn =
				CHILD_COUNT_PREFIX + sColumn.substring(1, sColumn.length() - 1);

			rChildMapping.set(SQL_CHILD_COUNT_COLUMN, sColumn);
		}

		return sColumn;
	}

	/***************************************
	 * Returns the SQL-specific name for a particular object. This method first
	 * looks for a relation of the type {@link JdbcRelationTypes#SQL_NAME} that
	 * must contain the object's SQL representation.
	 *
	 * <p>If no such relation exists it tries to read a relation of the type
	 * {@link StorageRelationTypes#STORAGE_NAME} instead. If that exists neither
	 * the result of the object's toString() method will be used. The resulting
	 * name will be converted with {@link TextUtil#uppercaseIdentifier(String)}
	 * that will finally be converted to lower case to create the SQL
	 * identifier. It will then be stored and stored in a newly created SQL name
	 * relation.</p>
	 *
	 * @param  rObject The object to return the SQL name for
	 * @param  bQuoted TRUE to return the name in quotes for the current
	 *                 database system, FALSE to only return the SQL name
	 *
	 * @return The SQL name for the given object
	 */
	String getSqlName(Object rObject, boolean bQuoted)
	{
		Relatable rRelatable = getRelatable(rObject);
		String    sName		 = rRelatable.get(SQL_NAME);

		if (sName == null)
		{
			sName = rRelatable.get(STORAGE_NAME);

			if (sName == null)
			{
				sName = rObject.toString();
			}

			sName = TextUtil.uppercaseIdentifier(sName).toLowerCase();

			rRelatable.set(SQL_NAME, sName);
		}

		if (bQuoted && cIdentifierQuote != 0)
		{
			sName = cIdentifierQuote + sName + cIdentifierQuote;
		}

		return sName;
	}

	/***************************************
	 * Maps a datatype class to the corresponding SQL datatype string.
	 *
	 * @param  rDatatype The datatype class
	 *
	 * @return The SQL datatype string
	 */
	String mapSqlDatatype(Class<?> rDatatype)
	{
		String sSqlDatatype;

		if (rDatatype.isEnum())
		{
			rDatatype = Enum.class;
		}
		else if (List.class.isAssignableFrom(rDatatype))
		{
			rDatatype = List.class;
		}
		else if (Set.class.isAssignableFrom(rDatatype))
		{
			rDatatype = Set.class;
		}
		else if (Map.class.isAssignableFrom(rDatatype))
		{
			rDatatype = Map.class;
		}

		sSqlDatatype = aDatatypeMap.get(rDatatype);

		if (sSqlDatatype == null)
		{
			sSqlDatatype = DEFAULT_STRING_DATATYPE;
			Log.warnf("No datatype mapping for '%s', using '%s' as default",
					  rDatatype,
					  DEFAULT_STRING_DATATYPE);
		}

		return sSqlDatatype;
	}

	/***************************************
	 * Maps a certain value to a datatype that can be stored in a JDBC database.
	 * If no generic value mapping the mapping will be delegated to the method
	 * {@link StorageMapping#mapValue(Relatable, Object)} of the given mapping
	 * instance.
	 *
	 * @param  rMapping   The storage mapping to be used for the mapping
	 * @param  rAttribute The descriptor of the attribute to map the value for
	 * @param  rValue     The value to check for a possible conversion
	 *
	 * @return The resulting value, converted as necessary
	 *
	 * @throws StorageException If the mapping process fails in the storage
	 *                          mapping
	 */
	<T> Object mapValue(StorageMapping<T, Relatable, ?> rMapping,
						Object							rAttribute,
						Object							rValue)
		throws StorageException
	{
		Relatable rAttrRelatable = getRelatable(rAttribute);

		if (rValue instanceof Enum ||
			rValue instanceof GenericEnum ||
			rValue instanceof Period)
		{
			if (rValue instanceof HasOrder)
			{
				rValue =
					((HasOrder) rValue).ordinal() + "-" + rValue.toString();
			}
			else
			{
				rValue = rValue.toString();
			}
		}
		else if (rValue instanceof RelationType)
		{
			RelationType<?> rType = (RelationType<?>) rValue;

			rValue =
				rType.hasFlag(SQL_OMIT_NAMESPACE) ? rType.getSimpleName()
												  : rType.getName();
		}
		else if (rValue instanceof Class)
		{
			rValue = ((Class<?>) rValue).getName();
		}
		else
		{
			rValue = rMapping.mapValue(rAttrRelatable, rValue);
		}

		if (rValue != null)
		{
			if (rValue.getClass() == Date.class)
			{
				// map java.util.Date to java.sql.Timestamp to let JDBC drivers store
				// both time and date
				rValue = new Timestamp(((Date) rValue).getTime());
			}
			else if (rAttrRelatable.get(SQL_DATATYPE) ==
					 DEFAULT_STRING_DATATYPE)
			{
				// convert default datatype columns to string values
				rValue = rValue.toString();
			}
		}

		return rValue;
	}

	/***************************************
	 * Creates a SQL insert statement to be used as a prepared statement. The
	 * object parameter is needed to determine whether a auto-generated ID field
	 * needs to be included in the statement's column and value placeholder
	 * lists. If the object contains a valid ID value (i.e. > 0) the field will
	 * be included in the lists. If not, it will be omitted from the lists so
	 * that it can be generated automatically by the database.
	 *
	 * @param  rMapping     The object to create the statement for
	 * @param  bGeneratedId TRUE if the statement should be created for an
	 *                      automatically generated ID value
	 *
	 * @return The statement string
	 *
	 * @throws StorageException If creating the statement fails
	 */
	private String createInsertStatement(
		StorageMapping<Object, Relatable, ?> rMapping,
		boolean								 bGeneratedId)
		throws StorageException
	{
		int			  nSize		  = rMapping.getAttributes().size();
		StringBuilder aColumns    = new StringBuilder(nSize * 10);
		StringBuilder aParameters = new StringBuilder(nSize * 2);
		String		  sSql		  = null;

		for (Relatable rAttr : rMapping.getAttributes())
		{
			String sColumn = getSqlName(rAttr, true);

			if (rAttr.hasFlag(AUTOGENERATED) && bGeneratedId)
			{
				// omit generated column from statement;
			}
			else
			{
				aColumns.append(sColumn).append(',');
				aParameters.append("?,");
			}
		}

		if (aColumns.length() == 0)
		{
			throw new StorageException("No columns to insert: " + rMapping);
		}

		if (!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS))
		{
			for (StorageMapping<?, ?, ?> rChildMapping :
				 rMapping.getChildMappings())
			{
				aColumns.append(getChildCountColumn(rChildMapping));
				aColumns.append(',');
				aParameters.append("?,");
			}
		}

		aColumns.setLength(aColumns.length() - 1);
		aParameters.setLength(aParameters.length() - 1);

		sSql = getSqlName(rMapping, true);
		sSql = formatStatement(INSERT_TEMPLATE, sSql, aColumns, aParameters);

		return sSql;
	}

	/***************************************
	 * Creates a new table in the database of this storage instance if it
	 * doesn't exist already.
	 *
	 * @param  rMapping The storage mapping to create the table for
	 *
	 * @throws StorageException If creating the table fails
	 */
	private void createTable(StorageMapping<?, ?, ?> rMapping)
		throws StorageException
	{
		StringBuilder   aColumns	    = new StringBuilder();
		List<Relatable> aParentAttr     = new ArrayList<>();
		Set<Relatable>  aIndexedAttr    = new LinkedHashSet<>();
		String		    sTableName	    = getSqlName(rMapping, true);
		String		    sObjectIdColumn = null;

		for (Relatable rAttr : rMapping.getAttributes())
		{
			String sSqlName = getSqlName(rAttr, true);

			aColumns.append(sSqlName);
			aColumns.append(' ');
			aColumns.append(mapColumnDatatype(rMapping, rAttr));

			if (rAttr.hasFlag(UNIQUE))
			{
				aColumns.append(" UNIQUE");
			}

			if (rAttr.hasFlag(MANDATORY))
			{
				aColumns.append(" NOT NULL");
			}

			if (rAttr.hasFlag(INDEXED))
			{
				aIndexedAttr.add(rAttr);
			}

			aColumns.append(',');

			if (rAttr.hasFlag(OBJECT_ID_ATTRIBUTE))
			{
				sObjectIdColumn = sSqlName;
			}
			else if (rAttr.hasFlag(PARENT_ATTRIBUTE))
			{
				aParentAttr.add(rAttr);
			}
		}

		if (!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS))
		{
			for (StorageMapping<?, ?, ?> rChildMapping :
				 rMapping.getChildMappings())
			{
				aColumns.append(getChildCountColumn(rChildMapping));
				aColumns.append(" INTEGER,");
			}
		}

		if (sObjectIdColumn != null)
		{
			aColumns.append(formatStatement(PRIMARY_KEY_TEMPLATE,
											sObjectIdColumn));
		}

		for (Relatable rParentAttr : aParentAttr)
		{
			StorageMapping<?, ?, ?> rParentMapping =
				rParentAttr.get(STORAGE_MAPPING);

			aColumns.append(formatStatement(FOREIGN_KEY_TEMPLATE,
											getSqlName(rParentAttr, true),
											getSqlName(rParentMapping, true),
											getSqlName(rParentMapping
													   .getIdAttribute(),
													   true)));
		}

		aColumns.setLength(aColumns.length() - 1);

		String sSql =
			formatStatement(CREATE_TABLE_TEMPLATE, sTableName, aColumns);

		Log.debug(sSql);

		executeUpdate(sSql, null, null, false, false);

		if (aIndexedAttr.size() > 0)
		{
			String sTable = getSqlName(rMapping, false);

			for (Relatable rAttr : aIndexedAttr)
			{
				sSql =
					formatStatement(INDEX_TEMPLATE,
									sTable,
									getSqlName(rAttr, false));

				Log.debug(sSql);

				executeUpdate(sSql, null, null, false, false);
			}
		}
	}

	/***************************************
	 * Creates a SQL update statement to be used as a prepared statement.
	 *
	 * @param  rMapping rObject The object to create the statement for
	 *
	 * @return The statement string
	 *
	 * @throws StorageException If creating the statement fails
	 */
	private String createUpdateStatement(StorageMapping<?, ?, ?> rMapping)
		throws StorageException
	{
		int			  nSize     = rMapping.getAttributes().size();
		StringBuilder aColumns  = new StringBuilder(nSize * 10);
		StringBuilder aIdentity = new StringBuilder();

		for (Relatable rAttr : rMapping.getAttributes())
		{
			String sColumn = getSqlName(rAttr, true);

			if (rAttr.hasFlag(OBJECT_ID_ATTRIBUTE))
			{
				aIdentity.append(sColumn).append("=?");
			}
			else // if (!rAttr.hasFlag(PARENT_ATTRIBUTE))
			{
				aColumns.append(sColumn).append("=?,");
			}
		}

		if (aColumns.length() == 0 || aIdentity.length() == 0)
		{
			throw new StorageException("No columns or primary key for update: " +
									   rMapping);
		}

		if (!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS))
		{
			for (StorageMapping<?, ?, ?> rChildMapping :
				 rMapping.getChildMappings())
			{
				aColumns.append(getChildCountColumn(rChildMapping));
				aColumns.append("=?,");
			}
		}

		aColumns.setLength(aColumns.length() - 1);

		return formatStatement(UPDATE_TEMPLATE,
							   getSqlName(rMapping, true),
							   aColumns,
							   aIdentity);
	}

	/***************************************
	 * Internal method to prepare and execute a SQL statement for updating or
	 * inserting data. If the statement shall only be executed without
	 * parameters the storage mapping and object parameter must be NULL and the
	 * boolean flags must be FALSE .
	 *
	 * @param  sSql         The SQL statement to be executed
	 * @param  rMapping     The storage mapping if the statement needs
	 *                      parameters to be set or NULL for other statements
	 * @param  rObject      The object the statement is executed for or NULL for
	 *                      none
	 * @param  bInsert      TRUE for an insert statement, FALSE for an update
	 *                      statement
	 * @param  bGeneratedId TRUE if the statement will produce an automatically
	 *                      generated ID value
	 *
	 * @throws StorageException If the execution fails
	 */
	private void executeUpdate(
		String								 sSql,
		StorageMapping<Object, Relatable, ?> rMapping,
		Object								 rObject,
		boolean								 bInsert,
		boolean								 bGeneratedId)
		throws StorageException
	{
		PreparedStatement aStatement = null;

		try
		{
			aStatement = prepareStatement(sSql, bGeneratedId);

			if (rMapping != null)
			{
				setStatementParameters(aStatement,
									   rMapping,
									   rObject,
									   bInsert,
									   bGeneratedId);
			}

			aStatement.executeUpdate();

			if (bGeneratedId)
			{
				setGeneratedKey(aStatement, rMapping, rObject);
			}
		}
		catch (SQLException e)
		{
			String sMessage;

			if (rObject != null)
			{
				sMessage =
					String.format("SQL %s failed for %s (%s",
								  bInsert ? "insert" : "update",
								  rObject,
								  sSql);
			}
			else
			{
				sMessage = String.format("SQL statement failed: " + sSql);
			}

			Log.error(sMessage, e);
			throw new StorageException(sMessage, e);
		}
		finally
		{
			if (aStatement != null)
			{
				try
				{
					aStatement.close();
				}
				catch (Exception e)
				{
					Log.warn("Could not close statement", e);
				}
			}
		}
	}

	/***************************************
	 * Returns the auto id datatype from either the given storage mapping if it
	 * exits there or for this storage.
	 *
	 * @param  rMapping        The storage mapping to query first
	 * @param  rAutoIdAttrType The relation type to query the auto ID attribute
	 *                         with
	 *
	 * @return The auto ID datatype string
	 */
	private String getAutoIdDatatype(
		StorageMapping<?, ?, ?> rMapping,
		RelationType<String>    rAutoIdAttrType)
	{
		String sSqlDatatype;

		if (rMapping.hasRelation(rAutoIdAttrType))
		{
			sSqlDatatype = rMapping.get(rAutoIdAttrType);
		}
		else
		{
			sSqlDatatype = get(rAutoIdAttrType);
		}

		return sSqlDatatype;
	}

	/***************************************
	 * Maps an entity attribute definition to the corresponding SQL datatype
	 * string.
	 *
	 * @param  rMapping    The parent mapping of the given attribute
	 * @param  rColumnAttr The attribute for the column datatype to be mapped
	 *
	 * @return The corresponding SQL datatype string
	 *
	 * @throws StorageException If no matching SQL datatype can be found
	 */
	private String mapColumnDatatype(
		StorageMapping<?, ?, ?> rMapping,
		Relatable				rColumnAttr) throws StorageException
	{
		String sSqlDatatype = null;

		if (rColumnAttr.hasRelation(SQL_DATATYPE))
		{
			sSqlDatatype = rColumnAttr.get(SQL_DATATYPE);
		}
		else
		{
			if (rColumnAttr.hasRelation(STORAGE_DATATYPE))
			{
				Class<?> rDatatype = rColumnAttr.get(STORAGE_DATATYPE);

				if (rColumnAttr.hasFlag(AUTOGENERATED))
				{
					if (rDatatype == Long.class)
					{
						sSqlDatatype =
							getAutoIdDatatype(rMapping,
											  SQL_LONG_AUTO_IDENTITY_DATATYPE);
					}
					else
					{
						sSqlDatatype =
							getAutoIdDatatype(rMapping,
											  SQL_AUTO_IDENTITY_DATATYPE);
					}
				}
				else
				{
					sSqlDatatype = mapSqlDatatype(rDatatype);

					if (sSqlDatatype.indexOf('%') >= 0)
					{
						sSqlDatatype =
							String.format(sSqlDatatype,
										  rColumnAttr.get(STORAGE_LENGTH));
					}
				}
			}

			if (sSqlDatatype != null)
			{
				rColumnAttr.set(SQL_DATATYPE, sSqlDatatype);
			}
			else
			{
				throw new StorageException("No SQL datatype mapping for: " +
										   rColumnAttr);
			}
		}

		return sSqlDatatype;
	}

	/***************************************
	 * Checks the modification flag of the given relatable representation of an
	 * object to be persisted. This will not read (and therefore create) the
	 * {@link MetaTypes#MODIFIED} relation if it doesn't exist. If the relation
	 * doesn't exist that means the implementation doesn't provide modification
	 * tracking. In that case the object must always be stored and this method
	 * returns TRUE.
	 *
	 * @param  rRelatable The relatable object to check
	 *
	 * @return TRUE if the object has modified attributes
	 */
	private boolean needsToBeStored(Relatable rRelatable)
	{
		return !rRelatable.hasRelation(MODIFIED) ||
			   rRelatable.hasFlag(MODIFIED);
	}

	/***************************************
	 * Internal method to return a new prepared statement that has been created
	 * from the given SQL statement. Any occurring SQLException will be
	 * converted into a {@link StorageException}.
	 *
	 * @param  sSQL        The SQL statement to prepare
	 * @param  bReturnKeys TRUE if the statement will return auto-generated keys
	 *
	 * @return The prepared statement
	 *
	 * @throws StorageException If preparing the statement fails
	 */
	private PreparedStatement prepareStatement(String  sSQL,
											   boolean bReturnKeys)
		throws StorageException
	{
		try
		{
			int nReturnKeys =
				bReturnKeys ? Statement.RETURN_GENERATED_KEYS
							: Statement.NO_GENERATED_KEYS;

			return rConnection.prepareStatement(sSQL, nReturnKeys);
		}
		catch (SQLException e)
		{
			Log.error("Preparing statement failed: " + sSQL, e);
			throw new StorageException("Preparing statement failed", e);
		}
	}

	/***************************************
	 * This method sets a database-generated primary key value on the given
	 * object. This method must be invoked after the object has been inserted
	 * into the database with the given statement. It will only have an effect
	 * if the current connection supports querying of generated keys with the
	 * method {@link Statement#getGeneratedKeys()}. If the key couldn't be
	 * queried it will be set to -1.
	 *
	 * @param  rStatement The statement to query the generated key from
	 * @param  rMapping   The storage mapping for the given object
	 * @param  rObject    The object to set the generated key on
	 *
	 * @return The generated key value
	 *
	 * @throws StorageException If querying the generated key fails
	 */
	@SuppressWarnings("boxing")
	private Object setGeneratedKey(
		Statement							 rStatement,
		StorageMapping<Object, Relatable, ?> rMapping,
		Object								 rObject) throws StorageException
	{
		Relatable rIdAttribute  = rMapping.getIdAttribute();
		long	  nGeneratedKey = -1;

		try
		{
			ResultSet rKeyResult = null;

			if (rIdAttribute != null)
			{
				DatabaseMetaData rMetaData = rConnection.getMetaData();

				if (rMetaData.supportsGetGeneratedKeys())
				{
					rKeyResult = rStatement.getGeneratedKeys();
				}

				if (rKeyResult != null && rKeyResult.next())
				{
					nGeneratedKey = rKeyResult.getLong(1);
				}

				if (rIdAttribute.get(STORAGE_DATATYPE) == Long.class)
				{
					rMapping.setAttributeValue(rObject,
											   rIdAttribute,
											   nGeneratedKey);
				}
				else
				{
					rMapping.setAttributeValue(rObject,
											   rIdAttribute,
											   (int) nGeneratedKey);
				}

				Log.debugf("Generated key %d for %s", nGeneratedKey, rObject);
			}
		}
		catch (SQLException e)
		{
			throw new StorageException("Retrieving generated key failed", e);
		}

		return nGeneratedKey;
	}

	/***************************************
	 * Sets the parameters on a prepared statement from a certain object. The
	 * boolean parameters define which parameters shall be set, the object's
	 * attributes and/or it's identity attributes. If both parameters are TRUE
	 * the attributes will be set first.
	 *
	 * @param  rStatement The prepared statement
	 * @param  rMapping   The storage mapping for the given object
	 * @param  rObject    The object to read the parameter values from
	 * @param  bInsert    FALSE if the statement is for an update and therefore
	 *                    needs the object's identity attributes to be set
	 * @param  bIgnoreId  TRUE if the object's ID field shall be ignored in
	 *                    insert statements (typically because it is
	 *                    automatically generated)
	 *
	 * @throws SQLException     If setting a parameter fails
	 * @throws StorageException If no identity attribute could be found in
	 *                          update mode
	 */
	private <C extends StorageMapping<?, Relatable, ?>> void
	setStatementParameters(PreparedStatement					rStatement,
						   StorageMapping<Object, Relatable, C> rMapping,
						   Object								rObject,
						   boolean								bInsert,
						   boolean								bIgnoreId)
		throws SQLException, StorageException
	{
		List<Object> aParams		 = new ArrayList<Object>();
		Object		 rIdentityValue  = null;
		int			 nStatementIndex = 1;

		for (Relatable rAttr : rMapping.getAttributes())
		{
			Object  rParam    = rMapping.getAttributeValue(rObject, rAttr);
			boolean bSetParam = true;

			rParam = mapValue(rMapping, rAttr, rParam);

			if (rAttr.hasFlag(OBJECT_ID_ATTRIBUTE))
			{
				rIdentityValue = rParam;
				bSetParam	   = bInsert && !bIgnoreId;
			}

			if (bSetParam)
			{
				rStatement.setObject(nStatementIndex++, rParam);
				aParams.add(rParam);
			}
		}

		if (!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS))
		{
			for (C rChildMapping : rMapping.getChildMappings())
			{
				int nChildren =
					rMapping.getChildren(rObject, rChildMapping).size();

				Integer aChildCount = Integer.valueOf(nChildren);

				rStatement.setObject(nStatementIndex++, aChildCount);
				aParams.add(aChildCount);
			}
		}

		if (!bInsert)
		{
			if (rIdentityValue != null)
			{
				rStatement.setObject(nStatementIndex++, rIdentityValue);
				aParams.add(rIdentityValue);
			}
			else
			{
				throw new StorageException("No identity attribute defined in " +
										   rMapping);
			}
		}

		Log.debugf("StatementParams: %s", aParams);
	}

	/***************************************
	 * Internal method to create the insert or update statement for the
	 * attributes of an object.
	 *
	 * @param  rMapping The storage mapping of the object
	 * @param  rObject  The object to store the attributes of
	 * @param  bInsert  TRUE for insert and FALSE for update
	 *
	 * @throws StorageException If executing the JDBC statement fails
	 */
	private void storeAttributes(StorageMapping<Object, Relatable, ?> rMapping,
								 Object								  rObject,
								 boolean							  bInsert)
		throws StorageException
	{
		Relatable rIdAttribute = rMapping.getIdAttribute();
		boolean   bGeneratedId = false;
		String    sSql;

		if (bInsert)
		{
			if (rIdAttribute.hasFlag(AUTOGENERATED))
			{
				Object rId = rMapping.getAttributeValue(rObject, rIdAttribute);

				bGeneratedId = (rId == null ||
								((Number) rId).longValue() <= 0);
			}

			sSql = createInsertStatement(rMapping, bGeneratedId);
		}
		else
		{
			sSql = createUpdateStatement(rMapping);
		}

		Log.debug(sSql);
		executeUpdate(sSql, rMapping, rObject, bInsert, bGeneratedId);
	}

	/***************************************
	 * Stores the objects that are referenced by the argument object if they are
	 * modified and not part of the object's hierarchy.
	 *
	 * @param  rObject  The object to store the references of
	 * @param  rMapping The storage mapping of the object
	 *
	 * @throws StorageException If storing a reference fails
	 */
	private void storeReferences(
		Relatable							 rObject,
		StorageMapping<Object, Relatable, ?> rMapping) throws StorageException
	{
		for (Relatable rAttr : rMapping.getAttributes())
		{
			if (!rMapping.isHierarchyAttribute(rAttr))
			{
				@SuppressWarnings("unchecked")
				StorageMapping<Object, ?, ?> rReferenceMapping =
					(StorageMapping<Object, ?, ?>) rAttr.get(STORAGE_MAPPING);

				if (rReferenceMapping != null)
				{
					Object rReference =
						rMapping.getAttributeValue(rObject, rAttr);

					if (rReference != null)
					{
						Relatable rRefRelatable = getRelatable(rReference);

						if (!rRefRelatable.hasRelation(STORING) &&
							needsToBeStored(rRefRelatable))
						{
							rReferenceMapping.storeReference(rObject,
															 rReference);
						}
					}
				}
			}
		}
	}
}
