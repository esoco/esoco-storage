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
import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;

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

	private final Class<T> mappedType;

	private FieldDescriptor idAttribute;

	private FieldDescriptor parentAttribute;

	private List<FieldDescriptor> fieldDescriptors =
		new ArrayList<FieldDescriptor>();

	private Map<ClassMapping<?>, FieldDescriptor> childFields =
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
	 * @param mappedType The type class to create the mapping for
	 * @throws IllegalArgumentException If an element of the given class cannot
	 *                                  be mapped
	 */
	public ClassMapping(Class<T> mappedType) {
		this.mappedType = mappedType;

		// store this mapping before initializing to prevent recursion when
		// child mappings access the mapping of their parent
		ObjectRelations.getRelatable(mappedType).set(STORAGE_MAPPING, this);

		analyzeFields();
		set(STORAGE_NAME, mappedType.getSimpleName());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T createObject(List<?> attributeValues, boolean asChild) {
		try {
			T object = mappedType.newInstance();
			int valueIndex = 0;

			for (FieldDescriptor field : fieldDescriptors) {
				Object value = attributeValues.get(valueIndex++);

				if (!field.hasFlag(PARENT_ATTRIBUTE)) {
					value = checkAttributeValue(field, value);
					field.setFieldValue(object, value);
				}
			}

			return object;
		} catch (Exception e) {
			throw new IllegalStateException(
				"Could not create instance of " + mappedType, e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Class<?> getAttributeDatatype(FieldDescriptor field) {
		return field.getField().getType();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object getAttributeValue(T object, FieldDescriptor field)
		throws StorageException {
		Object value = field.getFieldValue(object);

		// for references to other objects replace the object with it's ID
		if (value != null && field.hasRelation(STORAGE_MAPPING)) {
			@SuppressWarnings("unchecked")
			StorageMapping<Object, Relatable, ?> mapping =
				(StorageMapping<Object, Relatable, ?>) field.get(
					STORAGE_MAPPING);

			value = mapping.getAttributeValue(value, mapping.getIdAttribute());
		}

		return value;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<FieldDescriptor> getAttributes() {
		return fieldDescriptors;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<ClassMapping<?>> getChildMappings() {
		return childFields.keySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<?> getChildren(T object, ClassMapping<?> childMapping) {
		return (Collection<?>) childFields
			.get(childMapping)
			.getFieldValue(object);
	}

	/**
	 * Returns the descriptor for a certain field.
	 *
	 * @param fieldName The name of the field
	 * @return The field descriptor or NULL if no such field exists
	 */
	public FieldDescriptor getFieldDescriptor(String fieldName) {
		for (FieldDescriptor fieldDescriptor : fieldDescriptors) {
			if (fieldDescriptor.getField().getName().equals(fieldName)) {
				return fieldDescriptor;
			}
		}

		return null;
	}

	/**
	 * @see StorageMapping#getIdAttribute()
	 */
	@Override
	public FieldDescriptor getIdAttribute() {
		return idAttribute;
	}

	/**
	 * @see StorageMapping#getMappedType()
	 */
	@Override
	public Class<T> getMappedType() {
		return mappedType;
	}

	/**
	 * @see StorageMapping#getParentAttribute(StorageMapping)
	 */
	@Override
	public FieldDescriptor getParentAttribute(
		StorageMapping<?, ?, ?> parentMapping) {
		FieldDescriptor result = null;

		if (parentAttribute != null &&
			parentAttribute.get(STORAGE_MAPPING) == parentMapping) {
			result = parentAttribute;
		}

		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initChildren(T parent, List<?> children,
		ClassMapping<?> childMapping) {
		for (Object child : children) {
			childMapping.parentAttribute.setFieldValue(child, parent);
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
	public boolean isHierarchyAttribute(FieldDescriptor attribute) {
		return attribute.hasFlag(PARENT_ATTRIBUTE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setAttributeValue(T object, FieldDescriptor field,
		Object value) {
		field.setFieldValue(object, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setChildren(T parent, List<?> children,
		ClassMapping<?> childMapping) {
		childFields.get(childMapping).setFieldValue(parent, children);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return String.format("ClassMapping[%s]", mappedType.getSimpleName());
	}

	/**
	 * Analyzes the fields of the mapped class and fills the internal
	 * collections accordingly. Fields that are static, transient, or volatile
	 * will be ignored.
	 *
	 * @throws IllegalArgumentException If a field cannot be mapped
	 */
	private void analyzeFields() {
		for (Field field : mappedType.getDeclaredFields()) {
			int modifiers = field.getModifiers();

			if ((modifiers & IGNORED_MODIFIERS) == 0) {
				FieldDescriptor fieldDescriptor = new FieldDescriptor(field);

				if (fieldDescriptor.hasFlag(OBJECT_ID_ATTRIBUTE)) {
					idAttribute = fieldDescriptor;
				} else if (fieldDescriptor.hasFlag(PARENT_ATTRIBUTE)) {
					parentAttribute = fieldDescriptor;
				}

				if (Collection.class.isAssignableFrom(field.getType())) {
					ClassMapping<?> childMapping = createChildMapping(field);

					childFields.put(childMapping, fieldDescriptor);
					Log.debug(
						String.format("Child Mapping: %s,%s", childMapping,
							fieldDescriptor));
				} else {
					fieldDescriptors.add(fieldDescriptor);
					Log.debug(
						String.format("Field Mapping: %s", fieldDescriptor));
				}
			}
		}

		fieldDescriptors = Collections.unmodifiableList(fieldDescriptors);
		childFields = Collections.unmodifiableMap(childFields);
	}

	/**
	 * Creates and returns the mapping for the elements of a given
	 * collection-type field.
	 *
	 * @param field The field to create the mapping for
	 * @return The mapping instance for the given field
	 * @throws IllegalArgumentException If the field cannot be mapped to a
	 * child
	 *                                  collection
	 */
	private ClassMapping<?> createChildMapping(Field field) {
		assert Collection.class.isAssignableFrom(field.getType());

		Type genericType = field.getGenericType();
		Class<?> elementType = null;

		if (genericType instanceof ParameterizedType) {
			Type[] types =
				((ParameterizedType) genericType).getActualTypeArguments();

			if (types.length == 1 && types[0] instanceof Class<?>) {
				elementType = (Class<?>) types[0];
			}
		}

		if (elementType == null) {
			throw new IllegalArgumentException(
				"Could not determine element type of " + field.getName());
		}

		ClassMapping<?> childMapping =
			(ClassMapping<?>) StorageManager.getMapping(elementType);

		return childMapping;
	}
}
