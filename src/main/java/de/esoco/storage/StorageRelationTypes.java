//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2016 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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
package de.esoco.storage;

import de.esoco.lib.expression.Function;

import de.esoco.storage.impl.jdbc.JdbcRelationTypes;

import org.obrel.core.Annotations.RelationTypeNamespace;
import org.obrel.core.RelationType;
import org.obrel.core.RelationTypes;

import static org.obrel.core.RelationTypes.newDefaultValueType;
import static org.obrel.core.RelationTypes.newFlagType;
import static org.obrel.core.RelationTypes.newIntType;
import static org.obrel.core.RelationTypes.newType;


/********************************************************************
 * This class contains relation types for storage implementations. Besides
 * generic types it also defines some types for certain storage implementations,
 * i.e. mainly for relations with metadata for the SQL/JDBC implementation.
 *
 * @author eso
 */
@RelationTypeNamespace("de.esoco.storage")
public class StorageRelationTypes
{
	//~ Static fields/initializers ---------------------------------------------

	/**
	 * A flag that is mainly used internally by storage implementations to
	 * indicate that a object is currently being stored. This may also be used
	 * by storage-related application code to detect whether an object is made
	 * persistent, although there may be storage implementations that do not
	 * support this flag.
	 */
	public static final RelationType<Boolean> STORING = newFlagType();

	/**
	 * A flag to designate objects that already exist in a storage. It will be
	 * set by storage implementations on objects that have been stored in or
	 * read from the underlying storage. The flag is final because a persistent
	 * object is considered to be associated with the corresponding object for
	 * it's full lifetime. Therefore, to store an object as new in the same or a
	 * different storage a copy of the original object must be created.
	 */
	public static final RelationType<Boolean> PERSISTENT = newFlagType();

	/**
	 * Contains the generic storage name of an element. This name will be used
	 * to represent an object in a storage if the storage implementation
	 * supports such naming and if no property that is more specific to the
	 * respective storage implementation is set on the object (like {@link
	 * JdbcRelationTypes#SQL_NAME}).
	 */
	public static final RelationType<String> STORAGE_NAME = newType();

	/**
	 * This property can be set on some storage elements to define the datatype
	 * to be used for storage operations. For example, it must exist on
	 * attribute definitions returned by {@link StorageMapping} implementations
	 * if the method {@link Storage#initObjectStorage(StorageMapping)} is used
	 * to initialize some storage implementations (e.g. a JDBC-based storage).
	 */
	public static final RelationType<Class<?>> STORAGE_DATATYPE = newType();

	/**
	 * Defines the (maximum) length of a storage field. It depends on the
	 * storage implementation and field datatype mapping if and how this
	 * attribute is evaluated. If it is not the default value (2048) will be
	 * used.
	 */
	public static final RelationType<Integer> STORAGE_LENGTH = newIntType(2048);

	/** Contains a reference to the storage definition of a storage. */
	public static final RelationType<StorageDefinition> STORAGE_DEFINITION =
		newType();

	/** Contains the storage mapping for the object where it has been set on. */
	public static final RelationType<StorageMapping<?, ?, ?>> STORAGE_MAPPING =
		newType();

	/**
	 * Defines the maximum depth which a query should descend in the graph of an
	 * object. If this relation is not set the complete hierarchy of an object
	 * will be fetched. A value of zero means that no children should be fetched
	 * at all. This relation can be set on many storage-related objects as shown
	 * below. The priority is from top to bottom, i.e. the topmost relations
	 * override the lower (more generic) ones:
	 *
	 * <ol>
	 *   <li>on a query result</li>
	 *   <li>on a query</li>
	 *   <li>on a query predicate</li>
	 *   <li>on the criteria predicate of a query predicate</li>
	 *   <li>on a storage instance as the default for all of it's queries</li>
	 *   <li>on a storage definition as the default for all storages of that
	 *     definition</li>
	 * </ol>
	 *
	 * <p>Attention: some storage implementations may only support setting the
	 * query depth on a subset of these levels. Please check the documentation
	 * of the used implementation for such limitations.</p>
	 *
	 * <p>The default value is {@link Integer#MAX_VALUE} to indicate an
	 * unlimited query depth. Implementations that cannot handle this value
	 * should check for the existence of the relation instead.</p>
	 */
	public static final RelationType<Integer> QUERY_DEPTH =
		newIntType(Integer.MAX_VALUE);

	/**
	 * Can be set on a storage query to define the position of the first record
	 * to be returned by the query. Has a default value of 0 (zero).
	 */
	@SuppressWarnings("boxing")
	public static final RelationType<Integer> QUERY_OFFSET =
		newDefaultValueType(0);

	/**
	 * Can be set on a storage query to limit the number of records that will be
	 * returned by the query.
	 */
	public static final RelationType<Integer> QUERY_LIMIT = newType();

	/** A generic relation to a storage {@link Query}. */
	public static final RelationType<Query<?>> STORAGE_QUERY = newType();

	/** A generic relation to a storage {@link QueryResult query result}. */
	public static final RelationType<QueryResult<?>> STORAGE_QUERY_RESULT =
		newType();

	/**
	 * The size of a storage query. This relation can be used to store the size
	 * of a storage query. It is not filled automatically by the storage query.
	 */
	public static final RelationType<Integer> STORAGE_QUERY_SIZE = newType();

	/**
	 * References an arbitrary {@link QueryPredicate} that can be used to
	 * perform storage queries.
	 */
	public static final RelationType<QueryPredicate<?>> STORAGE_QUERY_PREDICATE =
		newType();

	/**
	 * References an arbitrary {@link Function} that is used in a storage
	 * context.
	 */
	public static final RelationType<Function<?, ?>> STORAGE_FUNCTION =
		newType();

	/**
	 * A flag to mark queries for child objects that should not automatically
	 * resolve parent references.
	 */
	public static final RelationType<Boolean> IS_CHILD_QUERY = newFlagType();

	static
	{
		RelationTypes.init(StorageRelationTypes.class);
	}

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Private, only static use.
	 */
	private StorageRelationTypes()
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
