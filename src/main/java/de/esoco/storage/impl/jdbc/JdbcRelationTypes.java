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

import de.esoco.storage.StorageRelationTypes;

import java.util.Map;

import org.obrel.core.Annotations.RelationTypeNamespace;
import org.obrel.core.RelationType;
import org.obrel.core.RelationTypeModifier;
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

	// ~ Static fields/initializers
	// ---------------------------------------------

	/**
	 * Contains the name to be used in SQL statements for the element on which
	 * it is set. This property will have precedence over a more generic storage
	 * name that may have been set as {@link StorageRelationTypes#STORAGE_NAME}.
	 */
	public static final RelationType<String> SQL_NAME = newType();

	/**
	 * The SQL-specific datatype for CREATE TABLE statements. This is used by
	 * the method {@link
	 * JdbcStorage#initObjectStorage(de.esoco.storage.StorageMapping)}.
	 */
	public static final RelationType<String> SQL_DATATYPE = newType();

	/**
	 * Contains the expression that is necessary to declare an integer identity
	 * datatype in CREATE TABLE statements. This is used by {@link JdbcStorage}
	 * when initializing the object storage. The default value is the standard
	 * SQL expression 'INTEGER AUTO_INCREMENT'.
	 */
	public static final RelationType<String> SQL_AUTO_IDENTITY_DATATYPE =
		newDefaultValueType("INTEGER AUTO_INCREMENT");

	/**
	 * Contains the expression that is necessary to declare a long integer
	 * identity datatype in CREATE TABLE statements. This is used by {@link
	 * JdbcStorage} when initializing the object storage. The default value is
	 * the standard SQL expression 'BIGINT AUTO_INCREMENT'.
	 */
	public static final RelationType<String> SQL_LONG_AUTO_IDENTITY_DATATYPE =
		newDefaultValueType("BIGINT AUTO_INCREMENT");

	/**
	 * The character that is used to quote identifiers in SQL statement.
	 * Defaults to the quotation mark (").
	 */
	@SuppressWarnings("boxing")
	public static final RelationType<Character> SQL_IDENTITIFIER_QUOTE =
		newDefaultValueType('"');

	/**
	 * The SQL expression that can be used to set the offset of the first in a
	 * query to support paging through the queried data. If set the expression
	 * will be appended to queries that have an ORDER BY clause. It must be
	 * parseable by {@link String#format(String, Object...)} with a single
	 * integer variable (%d) for the query offset. If not set the relation will
	 * be initialized to the standard SQL OFFSET clause. To disable paging
	 * support this parameter must be set to NULL.
	 */
	public static final RelationType<String> SQL_QUERY_PAGING_EXPRESSION =
		newDefaultValueType("OFFSET %d");

	/**
	 * The SQL expression that can be used to limit the number of rows returned
	 * by a query. If set it will be used for paging queries in conjunction with
	 * the relation {@link #SQL_QUERY_PAGING_EXPRESSION}.It must be parseable by
	 * {@link String#format(String, Object...)} with a single integer variable
	 * (%d) for the query limit. If not set the relation will be initialized to
	 * the standard SQL LIMIT clause. To disable paging support this parameter
	 * must be set to NULL.
	 */
	public static final RelationType<String> SQL_QUERY_LIMIT_EXPRESSION =
		newDefaultValueType("LIMIT %d");

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
	 * This is just an alias for {@link
	 * RelationTypes#newType(org.obrel.core.RelationTypeModifier...)} which
	 * exists as a semantic complement to {@link #column(String,
	 * RelationTypeModifier...)}.
	 *
	 * @param  rModifiers The optional relation type modifiers modifiers
	 *
	 * @return The new relation type
	 */
	public static <T> RelationType<T> column(RelationTypeModifier... rModifiers)
	{
		return newType(rModifiers);
	}

	/***************************************
	 * Creates a new relation type for a certain SQL column. This is just a
	 * shortcut for annotating the relation type with {@link #SQL_NAME} but also
	 * makes the generic declaration simpler due to the peculiarities of Java's
	 * generic type system.
	 *
	 * <p>If additional parameters (like an initialization function) need to be
	 * provided to the relation type declaration a standard factory method with
	 * a standard annotation needs to be used instead (with a more verbose
	 * generic declaration).</p>
	 *
	 * @param  sColumnName The SQL column name
	 * @param  rModifiers  The optional relation type modifiers modifiers
	 *
	 * @return The new relation type
	 */
	public static <T> RelationType<T> column(
		String					sColumnName,
		RelationTypeModifier... rModifiers)
	{
		RelationType<T> aColumnType = newType(rModifiers);

		return aColumnType.annotate(SQL_NAME, sColumnName);
	}

	// ~ Static methods
	// ---------------------------------------------------------

	/***************************************
	 * Package-internal method to initialize the relation types that are defined
	 * in this class.
	 */
	static void init()
	{
	}
}
