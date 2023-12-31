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
package de.esoco.storage.mapping;

import de.esoco.lib.datatype.Period;
import de.esoco.lib.expression.Conversions;
import de.esoco.lib.logging.Log;
import de.esoco.lib.manage.TransactionManager;
import de.esoco.lib.property.HasOrder;
import de.esoco.lib.reflect.ReflectUtil;

import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.StorageMapping;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;
import java.util.Map;

import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;
import org.obrel.core.RelationType;

import static de.esoco.lib.expression.Conversions.parseCollection;
import static de.esoco.lib.expression.Conversions.parseMap;

import static org.obrel.type.MetaTypes.ELEMENT_DATATYPE;
import static org.obrel.type.MetaTypes.KEY_DATATYPE;
import static org.obrel.type.MetaTypes.ORDERED;
import static org.obrel.type.MetaTypes.VALUE_DATATYPE;

/**
 * An abstract base class for storage mapping implementations. Contains some
 * generic helper methods for the implementation of mappings.
 *
 * @author eso
 */
public abstract class AbstractStorageMapping<T, A extends Relatable,
	C extends StorageMapping<?, A, ?>>
	extends RelatedObject implements StorageMapping<T, A, C> {

	/**
	 * Implemented to perform value conversions to some standard datatypes.
	 * This
	 * especially includes string parsing and primitive datatype wrapping. The
	 * supported string conversions are
	 *
	 * <ul>
	 *   <li>Datatype {@link Class}: {@link Class#forName(String)}</li>
	 *   <li>Datatype {@link RelationType}: {@link
	 *     RelationType#valueOf(String)}</li>
	 *   <li>Enums: {@link Enum#valueOf(Class, String)} with handling of {@link
	 *     HasOrder} prefixes</li>
	 *   <li>Datatype {@link Period}: {@link Period#valueOf(String)}</li>
	 *   <li>Datatype {@link Collection}: {@link
	 *     Conversions#parseCollection(String, Class, Class, boolean)}</li>
	 *   <li>Datatype {@link Map}: {@link Conversions#parseMap(String, Class,
	 *     Class, Class, boolean)}</li>
	 *   <li>Any datatype that has either a constructor with a String parameter
	 *     or a valueOf(String) method.</li>
	 * </ul>
	 *
	 * @see StorageMapping#checkAttributeValue(Relatable, Object)
	 */
	@Override
	public Object checkAttributeValue(A attribute, Object value)
		throws StorageException {
		if (value != null) {
			Class<?> datatype = getAttributeDatatype(attribute);

			if (datatype != String.class) {
				if (datatype.isPrimitive()) {
					datatype = ReflectUtil.getWrapperType(datatype);
				}

				if (value instanceof String) {
					value =
						parseStringValue(attribute, datatype, (String) value);
				} else if (datatype == Long.class && value instanceof Number) {
					value = ((Number) value).longValue();
				} else if (datatype == BigInteger.class &&
					value instanceof BigDecimal) {
					// large integer attributes may be stored as decimal values
					// without a fraction (e.g. SQL NUMERIC type)
					value = ((BigDecimal) value).toBigIntegerExact();
				}
			}

			Class<?> valueType = value.getClass();

			if (!datatype.isAssignableFrom(valueType)) {
				String message =
					String.format("Attribute type mismatch: %s (expected: %s)",
						valueType, datatype);

				throw new IllegalArgumentException(message);
			}
		}

		return value;
	}

	/**
	 * Base implementation that converts collections and maps to strings by
	 * invoking {@link Conversions#asString(Object)}. All other values will be
	 * returned unchanged.
	 *
	 * @see StorageMapping#mapValue(Relatable, Object)
	 */
	@Override
	public Object mapValue(A attribute, Object value) throws StorageException {
		if (value instanceof Collection || value instanceof Map) {
			value = Conversions.asString(value);
		}

		return value;
	}

	/**
	 * Default implementation that stores the referenced object inside a
	 * transaction.
	 *
	 * @see StorageMapping#storeReference(Relatable, Object)
	 */
	@Override
	public void storeReference(Relatable sourceObject, T referencedObject)
		throws StorageException {
		TransactionManager.begin();

		try {
			// TODO: determine the correct storage if not registered for class
			Storage storage =
				StorageManager.getStorage(referencedObject.getClass());

			TransactionManager.addTransactionElement(storage);

			storage.store(referencedObject);
			TransactionManager.commit();
		} catch (Exception e) {
			try {
				TransactionManager.rollback();
			} catch (Exception rollback) {
				Log.error("Transaction rollback failed", rollback);
			}

			if (e instanceof StorageException) {
				throw (StorageException) e;
			} else {
				throw new StorageException(e);
			}
		}
	}

	/**
	 * Parses a value into the corresponding datatype (if possible).
	 *
	 * @param attribute The attribute to parse the string for
	 * @param datatype  The attribute datatype
	 * @param value     The value to parse
	 * @return The parsed value or the original input string if parsing was not
	 * successful
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object parseStringValue(A attribute, Class<?> datatype,
		String value) {
		Object result;

		if (datatype == Class.class) {
			try {
				result = Class.forName(value);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e);
			}
		} else if (RelationType.class.isAssignableFrom(datatype)) {
			result = RelationType.valueOf(value);

			if (result == null) {
				throw new IllegalStateException(
					"Undefined RelationType " + value);
			}
		} else if (datatype.isEnum()) {
			if (HasOrder.class.isAssignableFrom(datatype)) {
				value = value.substring(value.indexOf('-') + 1);
			}

			result = Enum.valueOf((Class<Enum>) datatype, value.toUpperCase());
		} else if (datatype == Period.class) {
			result = Period.valueOf(value);
		} else if (Collection.class.isAssignableFrom(datatype)) {
			result =
				parseCollection(value, (Class<Collection<Object>>) datatype,
					(Class<Object>) attribute.get(ELEMENT_DATATYPE),
					attribute.hasFlag(ORDERED));
		} else if (Map.class.isAssignableFrom(datatype)) {
			result = parseMap(value, (Class<Map<Object, Object>>) datatype,
				(Class<Object>) attribute.get(KEY_DATATYPE),
				(Class<Object>) attribute.get(VALUE_DATATYPE),
				attribute.hasFlag(ORDERED));
		} else {
			result = tryInvokeParseMethod(datatype, value);
		}

		return result;
	}

	/**
	 * Tries to parse a value for a certain datatype from a string by either
	 * invoking a constructor of the datatype class with a string argument or,
	 * if that is not possible or fails a valueOf(String) method.
	 *
	 * @param datatype The target datatype
	 * @param value    The value to parse
	 * @return The parsed value or the input value if the parsing is not
	 * possible
	 */
	private Object tryInvokeParseMethod(Class<?> datatype, String value) {
		Object[] args = new Object[] { value };
		Object parsedValue = value;

		try {
			parsedValue = ReflectUtil.newInstance(datatype, args, null);
		} catch (Exception e) {
			try {
				parsedValue =
					ReflectUtil.invokePublic(datatype, "valueOf", args, null);
			} catch (Exception e2) {
				// just ignore and return the original value
			}
		}

		return parsedValue;
	}
}
