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

import de.esoco.storage.StorageRelationTypes;

import java.util.Map;

import org.obrel.core.Annotations.RelationTypeNamespace;
import org.obrel.core.RelationType;
import org.obrel.core.RelationTypes;

import static org.obrel.core.RelationTypes.newDefaultValueType;
import static org.obrel.core.RelationTypes.newFlagType;
import static org.obrel.core.RelationTypes.newMapType;
import static org.obrel.core.RelationTypes.newType;


/********************************************************************
 * Contains JDBC specific relation type declarations.
 *
 * @author eso
 */
@RelationTypeNamespace("de.esoco.storage")
public class JdbcRelationTypes
{
	//~ Static fields/initializers ---------------------------------------------

	/**
	 * Contains the name to be used in SQL statements for the element on which
	 * it is set. This property will have precedence over a more generic storage
	 * name that may have been set as {@link StorageRelationTypes#STORAGE_NAME}.
	 */
	public static final RelationType<String> SQL_NAME = newType();

	/**
	 * The SQL-specific datatype for CREATE TABLE statements. This is used by
	 * the method {@link JdbcStorage#initObjectStorage(StorageMapping)}.
	 */
	public static final RelationType<String> SQL_DATATYPE = newType();

	/**
	 * Contains the expression that is necessary to declare the identity
	 * datatype in CREATE TABLE statements of a certain database. This is used
	 * by the method {@link JdbcStorage#initObjectStorage(StorageMapping)}. The
	 * default value is the standard SQL expression 'INTEGER AUTO_INCREMENT'.
	 */
	public static final RelationType<String> SQL_AUTO_IDENTITY_DATATYPE =
		newDefaultValueType("INTEGER AUTO_INCREMENT");

	/**
	 * The character that is used to quote identifiers in SQL statement.
	 * Defaults to the quotation mark (").
	 */
	@SuppressWarnings("boxing")
	public static final RelationType<Character> SQL_IDENTITIFIER_QUOTE =
		newDefaultValueType('"');

	/**
	 * The SQL expression that can be used to limit the number and offset of
	 * rows in a query to support paging through the queried data. If set the
	 * expression will be appended to queries that have an ORDER BY clause. It
	 * must be parseable by {@link String#format(String, Object...)} and accept
	 * two integer parameters. The first will be the query count limit and the
	 * second the query offset. The default value is 'LIMIT %d OFFSET %d'. To
	 * disable paging support this parameter must be set to NULL.
	 */
	public static final RelationType<String> SQL_QUERY_PAGING_EXPRESSION =
		newDefaultValueType("LIMIT %d OFFSET %d");

	/**
	 * A mapping from datatype classes to the corresponding SQL datatype
	 * declarations.
	 */
	public static final RelationType<Map<Class<?>, String>> SQL_DATATYPE_MAP =
		newMapType(false);

	/**
	 * Contains the name to be used in SQL statements to store the child count
	 * of a certain child attribute.
	 */
	public static final RelationType<String> SQL_CHILD_COUNT_COLUMN = newType();

	/**
	 * A flag that indicates that the generation of child count columns should
	 * be suppressed. For performance reasons this should only be done for
	 * legacy tables that cannot be modified.
	 */
	public static final RelationType<Boolean> SQL_DISABLE_CHILD_COUNTS =
		newType();

	/**
	 * A flag that can be used on objects with namespaces (e.g. RelationTypes)
	 * to signal that the namespace should be omitted when storing the object
	 * name.
	 */
	public static final RelationType<Boolean> SQL_OMIT_NAMESPACE =
		newFlagType();

	/**
	 * Contains an optional create statement to be set one the storage mapping
	 * for {@link JdbcStorage#initObjectStorage(Class)}. If set the storage will
	 * be initialized with the given statement.
	 */
	public static final RelationType<String> SQL_CREATE_STATEMENT = newType();

	/** The name of the SQL function to be used for fuzzy searches. */
	public static final RelationType<String> SQL_FUZZY_SEARCH_FUNCTION =
		newType();

	/** Internal flag to mark child queries. */
	static final RelationType<Boolean> JDBC_CHILD_QUERY = newType();

	static
	{
		RelationTypes.init(JdbcRelationTypes.class);
	}

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Private, only static use.
	 */
	private JdbcRelationTypes()
	{
	}

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Package-internal method to initialize the relation types that are defined
	 * in this class.
	 */
	static void init()
	{
	}
}
