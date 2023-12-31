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
import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelationType;
import org.obrel.type.MetaTypes;
import org.obrel.type.StandardTypes;

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

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.REFERENCE_ATTRIBUTE;
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

/**
 * A storage implementation that uses a JDBC connection to store objects.
 *
 * @author eso
 */
public class JdbcStorage extends Storage {

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

	private static boolean childCountsEnabled = true;

	private static int nextId = 1;

	static {
		Map<Class<?>, String> sqlDatatypeMap = new HashMap<Class<?>, String>();

		sqlDatatypeMap.put(Byte.class, "TINYINT");
		sqlDatatypeMap.put(byte.class, "TINYINT");
		sqlDatatypeMap.put(byte[].class, "VARBINARY(%d)");
		sqlDatatypeMap.put(Short.class, "SHORT");
		sqlDatatypeMap.put(short.class, "SHORT");
		sqlDatatypeMap.put(Integer.class, "INTEGER");
		sqlDatatypeMap.put(int.class, "INTEGER");
		sqlDatatypeMap.put(Long.class, "BIGINT");
		sqlDatatypeMap.put(long.class, "BIGINT");
		sqlDatatypeMap.put(Float.class, "REAL");
		sqlDatatypeMap.put(float.class, "REAL");
		sqlDatatypeMap.put(Double.class, "DOUBLE");
		sqlDatatypeMap.put(double.class, "DOUBLE");
		sqlDatatypeMap.put(Boolean.class, "BOOLEAN");
		sqlDatatypeMap.put(boolean.class, "BOOLEAN");
		sqlDatatypeMap.put(BigInteger.class, "DECIMAL(1000)");
		sqlDatatypeMap.put(BigDecimal.class, "DECIMAL");
		sqlDatatypeMap.put(String.class, "VARCHAR(%d)");
		sqlDatatypeMap.put(Enum.class, "VARCHAR(255)");
		sqlDatatypeMap.put(Class.class, "VARCHAR(511)");
		sqlDatatypeMap.put(RelationType.class, "VARCHAR(511)");
		sqlDatatypeMap.put(Period.class, "VARCHAR(255)");
		sqlDatatypeMap.put(java.util.Date.class, "TIMESTAMP");
		sqlDatatypeMap.put(java.sql.Timestamp.class, "TIMESTAMP");
		sqlDatatypeMap.put(java.sql.Date.class, "DATE");
		sqlDatatypeMap.put(java.sql.Time.class, "TIME");
		sqlDatatypeMap.put(List.class, "VARCHAR(%d)");
		sqlDatatypeMap.put(Set.class, "VARCHAR(%d)");
		sqlDatatypeMap.put(Map.class, "VARCHAR(%d)");

		STANDARD_SQL_DATATYPE_MAP =
			Collections.unmodifiableMap(sqlDatatypeMap);
	}

	private Connection connection;

	private final String databaseName;

	private final String fuzzySearchFunction;

	private final char identifierQuote;

	private Map<Class<?>, String> datatypeMap = STANDARD_SQL_DATATYPE_MAP;

	/**
	 * Creates a new instance for a particular JDBC connection URL. The
	 * database-specific storage parameters must be given as a
	 * {@link Relatable}
	 * object that contains the parameters as relations.
	 *
	 * @param connection The JDBC connection
	 * @param params     The database-specific storage parameters
	 * @throws SQLException If creating the connection fails
	 */
	@SuppressWarnings("boxing")
	JdbcStorage(Connection connection, Relatable params) throws SQLException {
		this.connection = connection;
		databaseName = connection.getMetaData().getDatabaseProductName();

		set(StandardTypes.OBJECT_ID, ObjectId.intId(nextId++));

		connection.setAutoCommit(false);

		fuzzySearchFunction = params.get(SQL_FUZZY_SEARCH_FUNCTION);
		identifierQuote = params.get(SQL_IDENTITIFIER_QUOTE);

		ObjectRelations.copyRelations(params, this, false);

		if (!params.get(SQL_DATATYPE_MAP).isEmpty()) {
			datatypeMap =
				new HashMap<Class<?>, String>(STANDARD_SQL_DATATYPE_MAP);

			datatypeMap.putAll(params.get(SQL_DATATYPE_MAP));
			datatypeMap = Collections.unmodifiableMap(datatypeMap);
		}
	}

	/**
	 * Checks whether the given database contains a certain table.
	 *
	 * @param connection The database connection to check
	 * @param table      The name of the table
	 * @return TRUE if the table exists in the database
	 * @throws StorageException If the metadata query fails
	 */
	static boolean containsTable(Connection connection, String table)
		throws StorageException {
		try {
			DatabaseMetaData metaData = connection.getMetaData();

			ResultSet tables = metaData.getTables(null, null, table, null);
			boolean exists = tables.next();

			tables.close();

			Log.debug(table + (exists ? " exists" : " doesn't exist"));

			return exists;
		} catch (SQLException e) {
			throw new StorageException("Could not access table metadata", e);
		}
	}

	/**
	 * Internal method to format a certain SQL statement.
	 *
	 * @param format The format string for the statement
	 * @param args   The objects to be inserted into the statement
	 * @return The resulting statement string
	 */
	static String formatStatement(String format, Object... args) {
		return String.format(format, args);
	}

	/**
	 * Enables or disables the use of an additional and automatically managed
	 * child counts field. This can also be done individually by setting a
	 * static boolean property with the name "DISABLE_SQL_CHILD_COUNT" in an
	 * entity to true.
	 *
	 * @param enabled The new child counts enabled
	 */
	public static void setChildCountsEnabled(boolean enabled) {
		childCountsEnabled = enabled;
	}

	/**
	 * @see Storage#commit()
	 */
	@Override
	public void commit() throws StorageException {
		try {
			connection.commit();
		} catch (SQLException e) {
			throw new StorageException("Commit failed", e);
		}
	}

	/**
	 * @see Storage#deleteObject(Object)
	 */
	@Override
	public void deleteObject(Object object) throws StorageException {
		@SuppressWarnings("unchecked")
		StorageMapping<Object, Relatable, ?> mapping =
			(StorageMapping<Object, Relatable, ?>) StorageManager.getMapping(
				object.getClass());

		Relatable idAttribute = mapping.getIdAttribute();
		String table = getSqlName(mapping, true);
		String idAttr = getSqlName(idAttribute, true);

		Object id = mapping.getAttributeValue(object, idAttribute);

		String sql = formatStatement(DELETE_TEMPLATE, table, idAttr, id);

		Log.debug(sql);
		executeUpdate(sql, null, null, false, false);
	}

	/**
	 * Executes an arbitrary SQL update statement in this storage's connection.
	 *
	 * @param sql    The SQL update statement to execute
	 * @param params Optional parameters to be set on the prepared statement
	 * @throws StorageException If the statement execution fails
	 */
	public void executeUpdate(String sql, Object... params)
		throws StorageException {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			int paramIndex = 1;

			for (Object param : params) {
				statement.setObject(paramIndex++, param);
			}

			statement.executeUpdate();
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	/**
	 * Returns the JDBC connection that is used by this instance.
	 *
	 * @return The JDBC connection of this instance
	 */
	public final Connection getConnection() {
		return connection;
	}

	/**
	 * Returns the fuzzy search function.
	 *
	 * @return The fuzzy search function
	 */
	public String getFuzzySearchFunction() {
		return fuzzySearchFunction;
	}

	/**
	 * @see Storage#getStorageImplementationName()
	 */
	@Override
	public String getStorageImplementationName() {
		return databaseName;
	}

	/**
	 * @see Storage#isValid()
	 */
	@Override
	public boolean isValid() {
		try {
			// not yet supported by Postgres: connection.isValid(0);
			return !connection.isClosed();
		} catch (SQLException e) {
			Log.warn("JdbcStorage.isValid() failed", e);

			return false;
		}
	}

	/**
	 * @see Storage#query(QueryPredicate)
	 */
	@Override
	public <T> Query<T> query(QueryPredicate<T> queryPredicate)
		throws StorageException {
		return new JdbcQuery<T>(this, queryPredicate);
	}

	/**
	 * @see Storage#rollback()
	 */
	@Override
	public void rollback() throws StorageException {
		try {
			connection.rollback();
		} catch (SQLException e) {
			throw new StorageException("Commit failed", e);
		}
	}

	/**
	 * @see Object#toString()
	 */
	@Override
	public String toString() {
		return "JdbcStorage[" + connection + "]";
	}

	/**
	 * @see Storage#close()
	 */
	@Override
	protected void close() {
		try {
			connection.rollback();
		} catch (SQLException e) {
			Log.warn("Closing JDBC connection failed", e);
		}

		try {
			connection.close();
		} catch (SQLException e) {
			Log.warn("Closing JDBC connection failed", e);
		}

		connection = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean hasObjectStorage(StorageMapping<?, ?, ?> mapping)
		throws StorageException {
		return containsTable(connection, getSqlName(mapping, false));
	}

	/**
	 * Implemented to create the database tables for the given mapping. If the
	 * mapping contains child-mappings the child tables will also be created.
	 * But the tables will only be created if the table for the top-level
	 * mapping doesn't exist already.
	 *
	 * @see Storage#initObjectStorage(StorageMapping)
	 */
	@Override
	protected void initObjectStorage(StorageMapping<?, ?, ?> mapping)
		throws StorageException {
		if (!containsTable(connection, getSqlName(mapping, false))) {
			String createStatement = mapping.get(SQL_CREATE_STATEMENT);

			if (createStatement != null) {
				executeUpdate(createStatement, null, null, false, false);
			} else {
				createTable(mapping);
			}

			for (StorageMapping<?, ?, ?> childMapping :
				mapping.getChildMappings()) {
				// create child tables, but only if not self-referencing
				if (childMapping != mapping) {
					initObjectStorage(childMapping);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void removeObjectStorage(StorageMapping<?, ?, ?> mapping)
		throws StorageException {
		if (containsTable(connection, getSqlName(mapping, false))) {
			String dropStatement =
				String.format(DROP_TABLE_TEMPLATE, getSqlName(mapping, true));

			executeUpdate(dropStatement, null, null, false, false);
		}
	}

	/**
	 * Stores the hierarchy of a single object in the database. That means that
	 * the object and all of it's children will be stored. This method will be
	 * invoked recursively (from {@link #store(Object)}) for each level of
	 * child
	 * objects.
	 *
	 * @param object The object to store
	 * @throws Exception If storing the object fails
	 */
	@Override
	protected void storeObject(Object object) throws Exception {
		Relatable relatable = getRelatable(object);
		boolean insert = !relatable.hasFlag(PERSISTENT);

		@SuppressWarnings("unchecked")
		StorageMapping<Object, Relatable, StorageMapping<?, Relatable, ?>>
			mapping =
			(StorageMapping<Object, Relatable,
				StorageMapping<?, Relatable, ?>>) StorageManager.getMapping(
				object.getClass());

		// stores referenced objects first to make the generated IDs of new
		// objects available for storeAttributes()
		storeReferences(relatable, mapping);

		if (needsToBeStored(relatable)) {
			storeAttributes(mapping, object, insert);
		}

		for (StorageMapping<?, Relatable, ?> childMapping :
			mapping.getChildMappings()) {
			store(mapping.getChildren(object, childMapping));
		}
	}

	/**
	 * Returns the child count column.
	 *
	 * @param childMapping The child count column
	 * @return The child count column
	 */
	String getChildCountColumn(StorageMapping<?, ?, ?> childMapping) {
		String column = childMapping.get(SQL_CHILD_COUNT_COLUMN);

		if (column == null) {
			column = getSqlName(childMapping, true);
			column =
				CHILD_COUNT_PREFIX + column.substring(1, column.length() - 1);

			childMapping.set(SQL_CHILD_COUNT_COLUMN, column);
		}

		return column;
	}

	/**
	 * Returns the SQL-specific name for a particular object. This method first
	 * looks for a relation of the type {@link JdbcRelationTypes#SQL_NAME} that
	 * must contain the object's SQL representation.
	 *
	 * <p>If no such relation exists it tries to read a relation of the type
	 * {@link StorageRelationTypes#STORAGE_NAME} instead. If that exists
	 * neither
	 * the name will be generated from the object's toString() representation,
	 * camel case converted with
	 * {@link TextUtil#uppercaseIdentifier(String)} to
	 * words separated by underscores and then to lower case. Any name found
	 * will then be stored in {@link JdbcRelationTypes#SQL_NAME}.</p>
	 *
	 * @param object The object to return the SQL name for
	 * @param quoted TRUE to return the name in quotes for the current database
	 *               system, FALSE to only return the SQL name
	 * @return The SQL name for the given object
	 */
	String getSqlName(Object object, boolean quoted) {
		Relatable relatable = getRelatable(object);
		String name = relatable.get(SQL_NAME);

		if (name == null) {
			name = relatable.get(STORAGE_NAME);

			if (name == null) {
				name = TextUtil
					.uppercaseIdentifier(object.toString())
					.toLowerCase();
			}

			relatable.set(SQL_NAME, name);
		}

		if (quoted && identifierQuote != 0) {
			name = identifierQuote + name + identifierQuote;
		}

		return name;
	}

	/**
	 * Checks if the use of a child count field is enabled globally and for the
	 * given mapping.
	 *
	 * @param mapping The mapping to check if child counts are enabled globally
	 * @return TRUE if child counts are enabled
	 */
	final boolean isChildCountsEnabled(StorageMapping<?, ?, ?> mapping) {
		return childCountsEnabled && !mapping.hasFlag(SQL_DISABLE_CHILD_COUNTS);
	}

	/**
	 * Maps a datatype class to the corresponding SQL datatype string.
	 *
	 * @param datatype The datatype class
	 * @return The SQL datatype string
	 */
	String mapSqlDatatype(Class<?> datatype) {
		String sqlDatatype;

		if (datatype.isEnum()) {
			datatype = Enum.class;
		} else if (List.class.isAssignableFrom(datatype)) {
			datatype = List.class;
		} else if (Set.class.isAssignableFrom(datatype)) {
			datatype = Set.class;
		} else if (Map.class.isAssignableFrom(datatype)) {
			datatype = Map.class;
		}

		sqlDatatype = datatypeMap.get(datatype);

		if (sqlDatatype == null) {
			sqlDatatype = DEFAULT_STRING_DATATYPE;
			Log.warnf("No datatype mapping for '%s', using '%s' as default",
				datatype, DEFAULT_STRING_DATATYPE);
		}

		return sqlDatatype;
	}

	/**
	 * Maps a certain value to a datatype that can be stored in a JDBC
	 * database.
	 * If no generic value mapping the mapping will be delegated to the method
	 * {@link StorageMapping#mapValue(Relatable, Object)} of the given mapping
	 * instance.
	 *
	 * @param mapping   The storage mapping to be used for the mapping
	 * @param attribute The descriptor of the attribute to map the value for
	 * @param value     The value to check for a possible conversion
	 * @return The resulting value, converted as necessary
	 * @throws StorageException If the mapping process fails in the storage
	 *                          mapping
	 */
	<T> Object mapValue(StorageMapping<T, Relatable, ?> mapping,
		Object attribute, Object value) throws StorageException {
		Relatable attrRelatable = getRelatable(attribute);

		if (value instanceof Enum || value instanceof GenericEnum ||
			value instanceof Period) {
			if (value instanceof HasOrder) {
				value = ((HasOrder) value).ordinal() + "-" + value;
			} else {
				value = value.toString();
			}
		} else if (value instanceof RelationType) {
			RelationType<?> type = (RelationType<?>) value;

			value = type.hasFlag(SQL_OMIT_NAMESPACE) ?
			        type.getSimpleName() :
			        type.getName();
		} else if (value instanceof Class) {
			value = ((Class<?>) value).getName();
		} else {
			value = mapping.mapValue(attrRelatable, value);
		}

		if (value != null) {
			if (value.getClass() == Date.class) {
				// map java.util.Date to java.sql.Timestamp to let JDBC
				// drivers store
				// both time and date
				value = new Timestamp(((Date) value).getTime());
			} else if (attrRelatable.get(SQL_DATATYPE) ==
				DEFAULT_STRING_DATATYPE) {
				// convert default datatype columns to string values
				value = value.toString();
			}
		}

		return value;
	}

	/**
	 * Creates a SQL insert statement to be used as a prepared statement. The
	 * object parameter is needed to determine whether a auto-generated ID
	 * field
	 * needs to be included in the statement's column and value placeholder
	 * lists. If the object contains a valid ID value (i.e. > 0) the field will
	 * be included in the lists. If not, it will be omitted from the lists so
	 * that it can be generated automatically by the database.
	 *
	 * @param mapping     The object to create the statement for
	 * @param generatedId TRUE if the statement should be created for an
	 *                    automatically generated ID value
	 * @return The statement string
	 * @throws StorageException If creating the statement fails
	 */
	private String createInsertStatement(
		StorageMapping<Object, Relatable, ?> mapping, boolean generatedId)
		throws StorageException {
		int size = mapping.getAttributes().size();
		StringBuilder columns = new StringBuilder(size * 10);
		StringBuilder parameters = new StringBuilder(size * 2);
		String sql = null;

		for (Relatable attr : mapping.getAttributes()) {
			String column = getSqlName(attr, true);

			if (attr.hasFlag(AUTOGENERATED) && generatedId) {
				// omit generated column from statement;
			} else {
				columns.append(column).append(',');
				parameters.append("?,");
			}
		}

		if (columns.length() == 0) {
			throw new StorageException("No columns to insert: " + mapping);
		}

		if (isChildCountsEnabled(mapping)) {
			for (StorageMapping<?, ?, ?> childMapping :
				mapping.getChildMappings()) {
				columns.append(getChildCountColumn(childMapping));
				columns.append(',');
				parameters.append("?,");
			}
		}

		columns.setLength(columns.length() - 1);
		parameters.setLength(parameters.length() - 1);

		sql = getSqlName(mapping, true);
		sql = formatStatement(INSERT_TEMPLATE, sql, columns, parameters);

		return sql;
	}

	/**
	 * Creates a new table in the database of this storage instance if it
	 * doesn't exist already.
	 *
	 * @param mapping The storage mapping to create the table for
	 * @throws StorageException If creating the table fails
	 */
	private void createTable(StorageMapping<?, ?, ?> mapping)
		throws StorageException {
		StringBuilder columns = new StringBuilder();
		List<Relatable> referenceAttrs = new ArrayList<>();
		Set<Relatable> indexedAttrs = new LinkedHashSet<>();
		String tableName = getSqlName(mapping, true);
		String objectIdColumn = null;

		for (Relatable attr : mapping.getAttributes()) {
			String sqlName = getSqlName(attr, true);

			columns.append(sqlName);
			columns.append(' ');
			columns.append(mapColumnDatatype(mapping, attr));

			if (attr.hasFlag(UNIQUE)) {
				columns.append(" UNIQUE");
			}

			if (attr.hasFlag(MANDATORY)) {
				columns.append(" NOT NULL");
			}

			if (attr.hasFlag(INDEXED)) {
				indexedAttrs.add(attr);
			}

			columns.append(',');

			if (attr.hasFlag(OBJECT_ID_ATTRIBUTE)) {
				objectIdColumn = sqlName;
			} else if (attr.hasFlag(PARENT_ATTRIBUTE) ||
				attr.hasFlag(REFERENCE_ATTRIBUTE)) {
				referenceAttrs.add(attr);
			}
		}

		if (isChildCountsEnabled(mapping)) {
			for (StorageMapping<?, ?, ?> childMapping :
				mapping.getChildMappings()) {
				columns.append(getChildCountColumn(childMapping));
				columns.append(" INTEGER,");
			}
		}

		if (objectIdColumn != null) {
			columns.append(
				formatStatement(PRIMARY_KEY_TEMPLATE, objectIdColumn));
		}

		for (Relatable referenceAttr : referenceAttrs) {
			StorageMapping<?, ?, ?> targetMapping =
				referenceAttr.get(STORAGE_MAPPING);

			// exclude parent attributes to prevent recursion
			if (!referenceAttr.hasFlag(PARENT_ATTRIBUTE)) {
				initObjectStorage(targetMapping);
			}

			columns.append(formatStatement(FOREIGN_KEY_TEMPLATE,
				getSqlName(referenceAttr, true),
				getSqlName(targetMapping, true),
				getSqlName(targetMapping.getIdAttribute(), true)));
		}

		columns.setLength(columns.length() - 1);

		String sql = formatStatement(CREATE_TABLE_TEMPLATE, tableName,
			columns);

		Log.debug(sql);

		executeUpdate(sql, null, null, false, false);

		if (indexedAttrs.size() > 0) {
			String table = getSqlName(mapping, false);

			for (Relatable attr : indexedAttrs) {
				sql = formatStatement(INDEX_TEMPLATE, table,
					getSqlName(attr, false));

				Log.debug(sql);

				executeUpdate(sql, null, null, false, false);
			}
		}
	}

	/**
	 * Creates a SQL update statement to be used as a prepared statement.
	 *
	 * @param mapping rObject The object to create the statement for
	 * @return The statement string
	 * @throws StorageException If creating the statement fails
	 */
	private String createUpdateStatement(StorageMapping<?, ?, ?> mapping)
		throws StorageException {
		int size = mapping.getAttributes().size();
		StringBuilder columns = new StringBuilder(size * 10);
		StringBuilder identity = new StringBuilder();

		for (Relatable attr : mapping.getAttributes()) {
			String column = getSqlName(attr, true);

			if (attr.hasFlag(OBJECT_ID_ATTRIBUTE)) {
				identity.append(column).append("=?");
			} else // if (!attr.hasFlag(PARENT_ATTRIBUTE))
			{
				columns.append(column).append("=?,");
			}
		}

		if (columns.length() == 0 || identity.length() == 0) {
			throw new StorageException(
				"No columns or primary key for update: " + mapping);
		}

		if (isChildCountsEnabled(mapping)) {
			for (StorageMapping<?, ?, ?> childMapping :
				mapping.getChildMappings()) {
				columns.append(getChildCountColumn(childMapping));
				columns.append("=?,");
			}
		}

		columns.setLength(columns.length() - 1);

		return formatStatement(UPDATE_TEMPLATE, getSqlName(mapping, true),
			columns, identity);
	}

	/**
	 * Internal method to prepare and execute a SQL statement for updating or
	 * inserting data. If the statement shall only be executed without
	 * parameters the storage mapping and object parameter must be NULL and the
	 * boolean flags must be FALSE .
	 *
	 * @param sql         The SQL statement to be executed
	 * @param mapping     The storage mapping if the statement needs parameters
	 *                    to be set or NULL for other statements
	 * @param object      The object the statement is executed for or NULL for
	 *                    none
	 * @param insert      TRUE for an insert statement, FALSE for an update
	 *                    statement
	 * @param generatedId TRUE if the statement will produce an automatically
	 *                    generated ID value
	 * @throws StorageException If the execution fails
	 */
	private void executeUpdate(String sql,
		StorageMapping<Object, Relatable, ?> mapping, Object object,
		boolean insert, boolean generatedId) throws StorageException {
		try (PreparedStatement statement = prepareStatement(sql,
			generatedId)) {
			if (mapping != null) {
				setStatementParameters(statement, mapping, object, insert,
					generatedId);
			}

			statement.executeUpdate();

			if (generatedId) {
				setGeneratedKey(statement, mapping, object);
			}
		} catch (SQLException e) {
			String message;

			if (object != null) {
				message = String.format("SQL %s failed for %s (%s",
					insert ? "insert" : "update", object, sql);
			} else {
				message = String.format("SQL statement failed: " + sql);
			}

			Log.error(message, e);
			throw new StorageException(message, e);
		}
	}

	/**
	 * Returns the auto id datatype from either the given storage mapping if it
	 * exits there or for this storage.
	 *
	 * @param mapping        The storage mapping to query first
	 * @param autoIdAttrType The relation type to query the auto ID attribute
	 *                       with
	 * @return The auto ID datatype string
	 */
	private String getAutoIdDatatype(StorageMapping<?, ?, ?> mapping,
		RelationType<String> autoIdAttrType) {
		String sqlDatatype;

		if (mapping.hasRelation(autoIdAttrType)) {
			sqlDatatype = mapping.get(autoIdAttrType);
		} else {
			sqlDatatype = get(autoIdAttrType);
		}

		return sqlDatatype;
	}

	/**
	 * Maps an entity attribute definition to the corresponding SQL datatype
	 * string.
	 *
	 * @param mapping    The parent mapping of the given attribute
	 * @param columnAttr The attribute for the column datatype to be mapped
	 * @return The corresponding SQL datatype string
	 * @throws StorageException If no matching SQL datatype can be found
	 */
	private String mapColumnDatatype(StorageMapping<?, ?, ?> mapping,
		Relatable columnAttr) throws StorageException {
		String sqlDatatype = null;

		if (columnAttr.hasRelation(SQL_DATATYPE)) {
			sqlDatatype = columnAttr.get(SQL_DATATYPE);
		} else {
			if (columnAttr.hasRelation(STORAGE_DATATYPE)) {
				Class<?> datatype = columnAttr.get(STORAGE_DATATYPE);

				if (columnAttr.hasFlag(AUTOGENERATED)) {
					if (datatype == Long.class) {
						sqlDatatype = getAutoIdDatatype(mapping,
							SQL_LONG_AUTO_IDENTITY_DATATYPE);
					} else {
						sqlDatatype = getAutoIdDatatype(mapping,
							SQL_AUTO_IDENTITY_DATATYPE);
					}
				} else {
					sqlDatatype = mapSqlDatatype(datatype);

					if (sqlDatatype.indexOf('%') >= 0) {
						sqlDatatype = String.format(sqlDatatype,
							columnAttr.get(STORAGE_LENGTH));
					}
				}
			}

			if (sqlDatatype != null) {
				columnAttr.set(SQL_DATATYPE, sqlDatatype);
			} else {
				throw new StorageException(
					"No SQL datatype mapping for: " + columnAttr);
			}
		}

		return sqlDatatype;
	}

	/**
	 * Checks the modification flag of the given relatable representation of an
	 * object to be persisted. This will not read (and therefore create) the
	 * {@link MetaTypes#MODIFIED} relation if it doesn't exist. If the relation
	 * doesn't exist that means the implementation doesn't provide modification
	 * tracking. In that case the object must always be stored and this method
	 * returns TRUE.
	 *
	 * @param relatable The relatable object to check
	 * @return TRUE if the object has modified attributes
	 */
	private boolean needsToBeStored(Relatable relatable) {
		return !relatable.hasRelation(MODIFIED) || relatable.hasFlag(MODIFIED);
	}

	/**
	 * Internal method to return a new prepared statement that has been created
	 * from the given SQL statement. Any occurring SQLException will be
	 * converted into a {@link StorageException}.
	 *
	 * @param sQL        The SQL statement to prepare
	 * @param returnKeys TRUE if the statement will return auto-generated keys
	 * @return The prepared statement
	 * @throws StorageException If preparing the statement fails
	 */
	private PreparedStatement prepareStatement(String sQL, boolean returnKeys)
		throws StorageException {
		try {
			int returnKeysMode = returnKeys ?
			                     Statement.RETURN_GENERATED_KEYS :
			                     Statement.NO_GENERATED_KEYS;

			return connection.prepareStatement(sQL, returnKeysMode);
		} catch (SQLException e) {
			Log.error("Preparing statement failed: " + sQL, e);
			throw new StorageException("Preparing statement failed", e);
		}
	}

	/**
	 * This method sets a database-generated primary key value on the given
	 * object. This method must be invoked after the object has been inserted
	 * into the database with the given statement. It will only have an effect
	 * if the current connection supports querying of generated keys with the
	 * method {@link Statement#getGeneratedKeys()}. If the key couldn't be
	 * queried it will be set to -1.
	 *
	 * @param statement The statement to query the generated key from
	 * @param mapping   The storage mapping for the given object
	 * @param object    The object to set the generated key on
	 * @return The generated key value
	 * @throws StorageException If querying the generated key fails
	 */
	@SuppressWarnings("boxing")
	private Object setGeneratedKey(Statement statement,
		StorageMapping<Object, Relatable, ?> mapping, Object object)
		throws StorageException {
		Relatable idAttribute = mapping.getIdAttribute();
		long generatedKey = -1;

		try {
			ResultSet keyResult = null;

			if (idAttribute != null) {
				DatabaseMetaData metaData = connection.getMetaData();

				if (metaData.supportsGetGeneratedKeys()) {
					keyResult = statement.getGeneratedKeys();
				}

				if (keyResult != null && keyResult.next()) {
					generatedKey = keyResult.getLong(1);
				}

				if (idAttribute.get(STORAGE_DATATYPE) == Long.class) {
					mapping.setAttributeValue(object, idAttribute,
						generatedKey);
				} else {
					mapping.setAttributeValue(object, idAttribute,
						(int) generatedKey);
				}

				Log.debugf("Generated key %d for %s", generatedKey, object);
			}
		} catch (SQLException e) {
			throw new StorageException("Retrieving generated key failed", e);
		}

		return generatedKey;
	}

	/**
	 * Sets the parameters on a prepared statement from a certain object. The
	 * boolean parameters define which parameters shall be set, the object's
	 * attributes and/or it's identity attributes. If both parameters are TRUE
	 * the attributes will be set first.
	 *
	 * @param statement The prepared statement
	 * @param mapping   The storage mapping for the given object
	 * @param object    The object to read the parameter values from
	 * @param insert    FALSE if the statement is for an update and therefore
	 *                  needs the object's identity attributes to be set
	 * @param ignoreId  TRUE if the object's ID field shall be ignored in
	 *                     insert
	 *                  statements (typically because it is automatically
	 *                  generated)
	 * @throws SQLException     If setting a parameter fails
	 * @throws StorageException If no identity attribute could be found in
	 *                          update mode
	 */
	private <C extends StorageMapping<?, Relatable, ?>> void setStatementParameters(
		PreparedStatement statement,
		StorageMapping<Object, Relatable, C> mapping, Object object,
		boolean insert, boolean ignoreId)
		throws SQLException, StorageException {
		List<Object> params = new ArrayList<Object>();
		Object identityValue = null;
		int paramIndex = 1;

		for (Relatable attr : mapping.getAttributes()) {
			Object param = mapping.getAttributeValue(object, attr);
			boolean setParam = true;

			param = mapValue(mapping, attr, param);

			if (attr.hasFlag(OBJECT_ID_ATTRIBUTE)) {
				identityValue = param;
				setParam = insert && !ignoreId;
			}

			if (setParam) {
				statement.setObject(paramIndex++, param);
				params.add(param);
			}
		}

		if (isChildCountsEnabled(mapping)) {
			for (C childMapping : mapping.getChildMappings()) {
				int children =
					mapping.getChildren(object, childMapping).size();

				Integer childCount = Integer.valueOf(children);

				statement.setObject(paramIndex++, childCount);
				params.add(childCount);
			}
		}

		if (!insert) {
			if (identityValue != null) {
				statement.setObject(paramIndex++, identityValue);
				params.add(identityValue);
			} else {
				throw new StorageException(
					"No identity attribute defined in " + mapping);
			}
		}

		Log.debugf("StatementParams: %s", params);
	}

	/**
	 * Internal method to create the insert or update statement for the
	 * attributes of an object.
	 *
	 * @param mapping The storage mapping of the object
	 * @param object  The object to store the attributes of
	 * @param insert  TRUE for insert and FALSE for update
	 * @throws StorageException If executing the JDBC statement fails
	 */
	private void storeAttributes(StorageMapping<Object, Relatable, ?> mapping,
		Object object, boolean insert) throws StorageException {
		Relatable idAttribute = mapping.getIdAttribute();
		boolean generatedId = false;
		String sql;

		if (insert) {
			if (idAttribute.hasFlag(AUTOGENERATED)) {
				Object id = mapping.getAttributeValue(object, idAttribute);

				generatedId = (id == null || ((Number) id).longValue() <= 0);
			}

			sql = createInsertStatement(mapping, generatedId);
		} else {
			sql = createUpdateStatement(mapping);
		}

		Log.debug(sql);
		executeUpdate(sql, mapping, object, insert, generatedId);
	}

	/**
	 * Stores the objects that are referenced by the argument object if they
	 * are
	 * modified and not part of the object's hierarchy.
	 *
	 * @param object  The object to store the references of
	 * @param mapping The storage mapping of the object
	 * @throws StorageException If storing a reference fails
	 */
	private void storeReferences(Relatable object,
		StorageMapping<Object, Relatable, ?> mapping) throws StorageException {
		for (Relatable attr : mapping.getAttributes()) {
			if (!mapping.isHierarchyAttribute(attr)) {
				@SuppressWarnings("unchecked")
				StorageMapping<Object, ?, ?> referenceMapping =
					(StorageMapping<Object, ?, ?>) attr.get(STORAGE_MAPPING);

				if (referenceMapping != null) {
					Object reference = mapping.getAttributeValue(object, attr);

					if (reference != null) {
						Relatable refRelatable = getRelatable(reference);

						if (!refRelatable.hasRelation(STORING) &&
							needsToBeStored(refRelatable)) {
							referenceMapping.storeReference(object, reference);
						}
					}
				}
			}
		}
	}
}
