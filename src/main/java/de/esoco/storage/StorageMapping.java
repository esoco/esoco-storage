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
package de.esoco.storage;

import de.esoco.lib.expression.Predicate;

import de.esoco.storage.impl.jdbc.JdbcRelationTypes;

import java.util.Collection;
import java.util.List;

import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;
import org.obrel.type.MetaTypes;

/**
 * Interface that defines the methods for a specific mapping from an object type
 * to it's storage representation. Only a single mapping instance can exist for
 * a certain type. All information that is specific to a particular storage
 * implementation (e.g. like SQL column or table names) must be set as relations
 * on the mapping or on the attributes. It is recommended that a relation of the
 * type {@link StorageRelationTypes#STORAGE_NAME} on a mapping instance is
 * always set.
 *
 * <p>The generic type parameters define the types of the mapped type (T), the
 * attribute descriptors (A) and of referenced child mappings (C). The attribute
 * descriptors are required to be relatable objects so that relations with
 * storage-specific metadata (e.g. {@link JdbcRelationTypes#SQL_NAME}) can be
 * set on them. Again it is recommended that at least a relation containing the
 * storage name exists on attribute descriptors. Attributes may also have a
 * relation of type {@link StorageRelationTypes#STORAGE_DATATYPE} that contains
 * the datatype to be used for storage operations, especially for the method
 * {@link Storage#initObjectStorage(StorageMapping)}.</p>
 *
 * <p>Storage mappings must extend the class {@link RelatedObject} to provide
 * the functionality from the {@link Relatable} interface. Relations can and
 * must be used to set meta-informations that are either valid for all storages
 * or specific to certain storage implementations. For example, a relation of
 * the type {@link StorageRelationTypes#STORAGE_NAME} on the mapping instance
 * defines the storage name of objects described by the mapping while a relation
 * of the type {@link JdbcRelationTypes#SQL_NAME} would override it and define
 * their name in SQL statements.</p>
 *
 * <p>Implementations of storage objects and/or their mapping may implement
 * support for the tracking of attribute modifications to allow storage
 * implementations to optimize access to the storage back-end. To do so it must
 * set the flag relation {@link MetaTypes#MODIFIED} to TRUE on objects when they
 * have been modified. Storages will then automatically reset this flag to FALSE
 * after the object modifications have been made persistent. But the flag will
 * only be evaluated if it exists, otherwise objects will always be stored to
 * support objects without modification tracking. Therefore implementations that
 * track attribute changes must also set the flag to FALSE on new objects to
 * make the storage framework recognize the modification tracking support.</p>
 *
 * <p>The attributes of an object with a modification flag that is set to FALSE
 * will not be stored during a database update. But the flags of all children of
 * the object will still be evaluated recursively, thus storing only modified
 * objects in a hierarchy of object.</p>
 *
 * <p>All collections returned by this interface must not be modified by
 * storage implementations and may therefore be returned as unmodifiable
 * collections if the implementation prefers to do so.</p>
 *
 * @author eso
 */
public interface StorageMapping<T, A extends Relatable,
	C extends StorageMapping<?, A, ?>>
	extends Relatable {

	/**
	 * Checks whether a certain attribute value is valid for the attribute
	 * datatype and performs the necessary conversions if possible.
	 *
	 * @param rAttribute The attribute descriptor
	 * @param rValue     The attribute value to be checked (may be NULL)
	 * @return The attribute value, converted if necessary
	 * @throws IllegalArgumentException If the given value is invalid for the
	 *                                  attribute
	 * @throws StorageException         If accessing storage data for a
	 *                                  conversion fails
	 */
	public Object checkAttributeValue(A rAttribute, Object rValue)
		throws StorageException;

	/**
	 * This method must be implemented to return a new instance of the type
	 * that
	 * is described by this mapping. The argument is a list of the attribute
	 * values to be set in the new object. The list order will be the same in
	 * which the attribute descriptors are returned by
	 * {@link #getAttributes()}.
	 * The list may be empty but must never be NULL.
	 *
	 * @param rAttributeValues A list containing the attribute values
	 * @param bAsChild         TRUE if the object is read as a child of another
	 *                         object by the storage
	 * @return A new instance of the type described by this mapping,
	 * initialized
	 * with the given attribute values
	 * @throws StorageException If creating the object fails
	 */
	public T createObject(List<?> rAttributeValues, boolean bAsChild)
		throws StorageException;

	/**
	 * Returns the datatype of a certain attribute.
	 *
	 * @param rAttribute The attribute descriptor
	 * @return The attribute datatype
	 */
	public Class<?> getAttributeDatatype(A rAttribute);

	/**
	 * Returns the value of a certain attribute from the given object.
	 *
	 * @param rObject    The object to query the value from
	 * @param rAttribute The descriptor of the attribute to return the value of
	 * @return The attribute value (may be NULL)
	 * @throws StorageException Implementations may throw an storage exception
	 *                          if a storage operation is required to access an
	 *                          attribute value and fails
	 */
	public Object getAttributeValue(T rObject, A rAttribute)
		throws StorageException;

	/**
	 * Returns a collection of the attribute descriptors in this mapping.
	 *
	 * @return A collection of this mapping's attribute descriptors
	 */
	public Collection<A> getAttributes();

	/**
	 * Returns a collection of the storage mappings for child elements of the
	 * objects that are described by this mapping.
	 *
	 * @return A collection containing the child mappings
	 */
	public Collection<C> getChildMappings();

	/**
	 * Returns a collection of the child elements from the given object that
	 * are
	 * described by a certain mapping.
	 *
	 * @param rObject       The object to return the children of
	 * @param rChildMapping The mapping that describes the type of the children
	 * @return A collection containing the child elements (may be empty but
	 * will
	 * never be null)
	 */
	public Collection<?> getChildren(T rObject, C rChildMapping);

	/**
	 * Returns a predicate with default criteria for the mapped type or a
	 * subclass of it. Can be implemented by subclasses to implement a
	 * hierarchy
	 * of persisted classes that share the same storage area (e.g. a SQL table)
	 * or to filter certain data (e.g. legacy or historical data) from queries.
	 *
	 * <p>The default implementation always returns NULL which is ignored.</p>
	 *
	 * @param rType The (sub-type) that is actually queried
	 * @return The default criteria for the given type or NULL for none
	 */
	default public Predicate<T> getDefaultCriteria(Class<? extends T> rType) {
		return null;
	}

	/**
	 * Returns the ID attribute of this mapping.
	 *
	 * @return The ID attribute type
	 */
	public A getIdAttribute();

	/**
	 * Returns the class of the type that is described by this mapping.
	 *
	 * @return The mapped type
	 */
	public Class<T> getMappedType();

	/**
	 * Returns the parent attribute that is associated with a certain parent
	 * mapping.
	 *
	 * @param rParentMapping The parent mapping to return the attribute for
	 * @return The matching parent attribute or NULL if none could be found
	 */
	public A getParentAttribute(StorageMapping<?, ?, ?> rParentMapping);

	/**
	 * Initializes the parent-child relation of a list of child elements.
	 *
	 * @param rObject       The parent object to initialize the children of
	 * @param rChildren     The list of child elements
	 * @param rChildMapping The mapping that describes the child type
	 */
	public void initChildren(T rObject, List<?> rChildren, C rChildMapping);

	/**
	 * Checks whether the mapped type is allowed to be deleted from a storage.
	 *
	 * @return The delete allowed
	 */
	public boolean isDeleteAllowed();

	/**
	 * Returns TRUE if the given attribute defines a part of an objects
	 * hierarchy.
	 *
	 * @param rAttribute The attribute to check
	 * @return TRUE for a hierarchy attribute
	 */
	public boolean isHierarchyAttribute(A rAttribute);

	/**
	 * This method performs a mapping of attribute values which cannot be
	 * stored
	 * directly by a storage implementation (e.g. JDBC-based storages). The
	 * returned value must be an object that is more suitable for being
	 * persisted in such storages (like an identifier string instead of a
	 * complete entity). If no mapping is necessary or possible the argument
	 * should be returned unchanged. NULL values are allowed and should simply
	 * be returned too.
	 *
	 * <p>The first argument contains the attribute descriptor for the given
	 * value. It can be used to perform attribute-specific mappings, e.g. if
	 * the
	 * result depends on the value of another attribute or the attribute type.
	 * </p>
	 *
	 * @param rAttribute The descriptor of the attribute to map
	 * @param rValue     The value to be mapped (may be NULL)
	 * @return The mapped value
	 * @throws StorageException Implementations may throw an storage exception
	 *                          if they cannot map a value or need to perform
	 *                          other storage operations for the mapping
	 */
	public Object mapValue(A rAttribute, Object rValue) throws StorageException;

	/**
	 * Sets the value of a certain attribute on the given object.
	 *
	 * @param rObject    The object to set the value on
	 * @param rAttribute The descriptor of the attribute to be set
	 * @param rValue     The attribute value
	 */
	public void setAttributeValue(T rObject, A rAttribute, Object rValue);

	/**
	 * Sets the children of a parent to a list of child elements. The given
	 * list
	 * MUST be used directly by the implementation because it may be a special
	 * object that implements storage access or similar.
	 *
	 * @param rObject       The parent object to set the children of
	 * @param rChildren     The list of child elements
	 * @param rChildMapping The mapping that describes the child type
	 */
	public void setChildren(T rObject, List<?> rChildren, C rChildMapping);

	/**
	 * May be invoked by a storage implementation to store a modified object of
	 * this mapping that is referenced by another object before that object is
	 * stored.
	 *
	 * @param rSourceObject     The object from which the reference originates
	 * @param rReferencedObject The referenced object to store
	 * @throws StorageException If storing the reference fails
	 */
	public void storeReference(Relatable rSourceObject, T rReferencedObject)
		throws StorageException;
}
