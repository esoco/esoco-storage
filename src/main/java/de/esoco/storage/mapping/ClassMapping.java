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
package de.esoco.storage.mapping;

import de.esoco.lib.logging.Log;

import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.StorageMapping;
import de.esoco.storage.StorageRelationTypes;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;

import static de.esoco.storage.StorageRelationTypes.STORAGE_MAPPING;
import static de.esoco.storage.StorageRelationTypes.STORAGE_NAME;

import static org.obrel.type.MetaTypes.OBJECT_ID_ATTRIBUTE;
import static org.obrel.type.MetaTypes.PARENT_ATTRIBUTE;

/**
 * A storage mapping implementation that maps a class to storages. It uses
 * reflection to analyze the structure of the class and creates a corresponding
 * storage mapping. The analysis will only be performed on the first access to a
 * corresponding method. Therefore instances of this class can also be used as a
 * mere class identifier for storage implementations that don't need a mapping
 * (like object oriented databases).
 *
 * <p>To be mapped by this class the structure of a class must meet the
 * following requirements:</p>
 *
 * <ul>
 *   <li>Field names must follow the camel case pattern that is typical for Java
 *     code. They may have a single-letter prefix which will be omitted from the
 *     name that is used for storage fields.</li>
 *   <li>The primary key field must have the name "id" (with an optional prefix
 *     as described before). If it's datatype is integer it will be mapped to an
 *     auto-increment database field.</li>
 *   <li>Classes that are used as child elements of other mapped classes must
 *     have a field "parent" that references an object of the parent type. It
 *     will be mapped to an integer field that must be mappable to an integer ID
 *     field in the parent.</li>
 *   <li>Child fields must be of a collection type and they must not be NULL. If
 *     no children are available the collection must be empty.</li>
 * </ul>
 *
 * @author eso
 */
public class ClassMapping<T>
	extends AbstractStorageMapping<T, FieldDescriptor, ClassMapping<?>> {

	private static final int IGNORED_MODIFIERS =
		Modifier.TRANSIENT | Modifier.STATIC | Modifier.VOLATILE;

	private final Class<T> rMappedType;

	private FieldDescriptor rIdAttribute;

	private FieldDescriptor rParentAttribute;

	private List<FieldDescriptor> aFieldDescriptors =
		new ArrayList<FieldDescriptor>();

	private Map<ClassMapping<?>, FieldDescriptor> aChildFields =
		new LinkedHashMap<ClassMapping<?>, FieldDescriptor>();

	/**
	 * Creates a new class-based storage mapping for a particular class. The
	 * simple name of the class (i.e. the name without package) will be set
	 * unchanged as the {@link StorageRelationTypes#STORAGE_NAME} relation of
	 * this mapping and can be changed afterwards if necessary.
	 *
	 * <p>To be usable for all storage implementations the given class must
	 * have a public constructor without parameters so that new objects can be
	 * created by the {@link #createObject(List, boolean)} method. If no such
	 * constructor exists an {@link IllegalStateException} will be thrown when
	 * the method is invoked.</p>
	 *
	 * @param rClass The type to create the mapping for
	 * @throws IllegalArgumentException If an element of the given class cannot
	 *                                  be mapped
	 */
	public ClassMapping(Class<T> rClass) {
		rMappedType = rClass;

		// store this mapping before initializing to prevent recursion when
		// child mappings access the mapping of their parent
		ObjectRelations.getRelatable(rClass).set(STORAGE_MAPPING, this);

		analyzeFields();
		set(STORAGE_NAME, rMappedType.getSimpleName());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T createObject(List<?> rAttributeValues, boolean bAsChild) {
		try {
			T aObject = rMappedType.newInstance();
			int nValueIndex = 0;

			for (FieldDescriptor rField : aFieldDescriptors) {
				Object rValue = rAttributeValues.get(nValueIndex++);

				if (!rField.hasFlag(PARENT_ATTRIBUTE)) {
					rValue = checkAttributeValue(rField, rValue);
					rField.setFieldValue(aObject, rValue);
				}
			}

			return aObject;
		} catch (Exception e) {
			throw new IllegalStateException(
				"Could not create instance of " + rMappedType, e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<?> getAttributeDatatype(FieldDescriptor rField) {
		return rField.getField().getType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object getAttributeValue(T rObject, FieldDescriptor rField)
		throws StorageException {
		Object rValue = rField.getFieldValue(rObject);

		// for references to other objects replace the object with it's ID
		if (rValue != null && rField.hasRelation(STORAGE_MAPPING)) {
			@SuppressWarnings("unchecked")
			StorageMapping<Object, Relatable, ?> rMapping =
				(StorageMapping<Object, Relatable, ?>) rField.get(
					STORAGE_MAPPING);

			rValue =
				rMapping.getAttributeValue(rValue, rMapping.getIdAttribute());
		}

		return rValue;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<FieldDescriptor> getAttributes() {
		return aFieldDescriptors;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<ClassMapping<?>> getChildMappings() {
		return aChildFields.keySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<?> getChildren(T rObject,
		ClassMapping<?> rChildMapping) {
		return (Collection<?>) aChildFields
			.get(rChildMapping)
			.getFieldValue(rObject);
	}

	/**
	 * Returns the descriptor for a certain field.
	 *
	 * @param sFieldName The name of the field
	 * @return The field descriptor or NULL if no such field exists
	 */
	public FieldDescriptor getFieldDescriptor(String sFieldName) {
		for (FieldDescriptor rFieldDescriptor : aFieldDescriptors) {
			if (rFieldDescriptor.getField().getName().equals(sFieldName)) {
				return rFieldDescriptor;
			}
		}

		return null;
	}

	/**
	 * @see StorageMapping#getIdAttribute()
	 */
	@Override
	public FieldDescriptor getIdAttribute() {
		return rIdAttribute;
	}

	/**
	 * @see StorageMapping#getMappedType()
	 */
	@Override
	public Class<T> getMappedType() {
		return rMappedType;
	}

	/**
	 * @see StorageMapping#getParentAttribute(StorageMapping)
	 */
	@Override
	public FieldDescriptor getParentAttribute(
		StorageMapping<?, ?, ?> rParentMapping) {
		FieldDescriptor rResult = null;

		if (rParentAttribute != null &&
			rParentAttribute.get(STORAGE_MAPPING) == rParentMapping) {
			rResult = rParentAttribute;
		}

		return rResult;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initChildren(T rParent, List<?> rChildren,
		ClassMapping<?> rChildMapping) {
		for (Object rChild : rChildren) {
			rChildMapping.rParentAttribute.setFieldValue(rChild, rParent);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isDeleteAllowed() {
		// TODO: check field or annotation for delete option
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isHierarchyAttribute(FieldDescriptor rAttribute) {
		return rAttribute.hasFlag(PARENT_ATTRIBUTE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setAttributeValue(T rObject, FieldDescriptor rField,
		Object rValue) {
		rField.setFieldValue(rObject, rValue);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setChildren(T rParent, List<?> rChildren,
		ClassMapping<?> rChildMapping) {
		aChildFields.get(rChildMapping).setFieldValue(rParent, rChildren);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return String.format("ClassMapping[%s]", rMappedType.getSimpleName());
	}

	/**
	 * Analyzes the fields of the mapped class and fills the internal
	 * collections accordingly. Fields that are static, transient, or volatile
	 * will be ignored.
	 *
	 * @throws IllegalArgumentException If a field cannot be mapped
	 */
	private void analyzeFields() {
		for (Field rField : rMappedType.getDeclaredFields()) {
			int nModifiers = rField.getModifiers();

			if ((nModifiers & IGNORED_MODIFIERS) == 0) {
				FieldDescriptor aFieldDescriptor = new FieldDescriptor(rField);

				if (aFieldDescriptor.hasFlag(OBJECT_ID_ATTRIBUTE)) {
					rIdAttribute = aFieldDescriptor;
				} else if (aFieldDescriptor.hasFlag(PARENT_ATTRIBUTE)) {
					rParentAttribute = aFieldDescriptor;
				}

				if (Collection.class.isAssignableFrom(rField.getType())) {
					ClassMapping<?> rChildMapping = createChildMapping(rField);

					aChildFields.put(rChildMapping, aFieldDescriptor);
					Log.debug(
						String.format("Child Mapping: %s,%s", rChildMapping,
							aFieldDescriptor));
				} else {
					aFieldDescriptors.add(aFieldDescriptor);
					Log.debug(
						String.format("Field Mapping: %s", aFieldDescriptor));
				}
			}
		}

		aFieldDescriptors = Collections.unmodifiableList(aFieldDescriptors);
		aChildFields = Collections.unmodifiableMap(aChildFields);
	}

	/**
	 * Creates and returns the mapping for the elements of a given
	 * collection-type field.
	 *
	 * @param rField The field to create the mapping for
	 * @return The mapping instance for the given field
	 * @throws IllegalArgumentException If the field cannot be mapped to a
	 * child
	 *                                  collection
	 */
	private ClassMapping<?> createChildMapping(Field rField) {
		assert Collection.class.isAssignableFrom(rField.getType());

		Type rGenericType = rField.getGenericType();
		Class<?> rElementType = null;

		if (rGenericType instanceof ParameterizedType) {
			Type[] rTypes =
				((ParameterizedType) rGenericType).getActualTypeArguments();

			if (rTypes.length == 1 && rTypes[0] instanceof Class<?>) {
				rElementType = (Class<?>) rTypes[0];
			}
		}

		if (rElementType == null) {
			throw new IllegalArgumentException(
				"Could not determine element type of " + rField.getName());
		}

		ClassMapping<?> rChildMapping =
			(ClassMapping<?>) StorageManager.getMapping(rElementType);

		return rChildMapping;
	}
}
