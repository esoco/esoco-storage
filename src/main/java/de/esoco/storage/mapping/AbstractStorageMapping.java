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


/********************************************************************
 * An abstract base class for storage mapping implementations. Contains some
 * generic helper methods for the implementation of mappings.
 *
 * @author eso
 */
public abstract class AbstractStorageMapping<T, A extends Relatable,
											 C extends StorageMapping<?, A, ?>>
	extends RelatedObject implements StorageMapping<T, A, C>
{
	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Implemented to perform conversions from string values to some standard
	 * datatypes. The supported string conversions are
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
	 * </ul>
	 *
	 * @see StorageMapping#checkAttributeValue(Relatable, Class, Object)
	 */
	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object checkAttributeValue(A rAttribute, Object rValue)
		throws StorageException
	{
		if (rValue != null)
		{
			Class<?> rDatatype = getAttributeDatatype(rAttribute);

			if (rValue instanceof String)
			{
				String sValue = (String) rValue;

				if (rDatatype == Class.class)
				{
					try
					{
						rValue = Class.forName(sValue);
					}
					catch (ClassNotFoundException e)
					{
						throw new IllegalStateException(e);
					}
				}
				else if (RelationType.class.isAssignableFrom(rDatatype))
				{
					rValue = RelationType.valueOf(sValue);

					if (rValue == null)
					{
						throw new IllegalStateException("Undefined RelationType " +
														sValue);
					}
				}
				else if (rDatatype.isEnum())
				{
					if (HasOrder.class.isAssignableFrom(rDatatype))
					{
						sValue = sValue.substring(sValue.indexOf('-') + 1);
					}

					rValue = Enum.valueOf((Class<Enum>) rDatatype, sValue);
				}
				else if (rDatatype == Period.class)
				{
					rValue = Period.valueOf(sValue);
				}
				else if (Collection.class.isAssignableFrom(rDatatype))
				{
					rValue =
						parseCollection(sValue,
										(Class<Collection<Object>>) rDatatype,
										(Class<Object>) rAttribute.get(ELEMENT_DATATYPE),
										rAttribute.hasFlag(ORDERED));
				}
				else if (Map.class.isAssignableFrom(rDatatype))
				{
					rValue =
						parseMap(sValue,
								 (Class<Map<Object, Object>>) rDatatype,
								 (Class<Object>) rAttribute.get(KEY_DATATYPE),
								 (Class<Object>) rAttribute.get(VALUE_DATATYPE),
								 rAttribute.hasFlag(ORDERED));
				}
			}
			else if (rDatatype == Integer.class && rValue instanceof Long)
			{
				// map long values to integer if value is in the integer range
				// this is a fix for MySQL which produces long IDs in views
				// under some circumstances

				long nLong = ((Long) rValue).longValue();

				if (nLong <= Integer.MAX_VALUE && nLong >= Integer.MIN_VALUE)
				{
					rValue = Integer.valueOf((int) nLong);
				}
			}

			Class<?> rValueType = rValue.getClass();

			if (rDatatype.isPrimitive())
			{
				rDatatype = ReflectUtil.getWrapperType(rDatatype);
			}

			if (!rDatatype.isAssignableFrom(rValueType))
			{
				String sMessage =
					String.format("Attribute type mismatch: %s (expected: %s)",
								  rValueType,
								  rDatatype);

				throw new IllegalArgumentException(sMessage);
			}
		}

		return rValue;
	}

	/***************************************
	 * Base implementation that converts collections and maps to strings by
	 * invoking {@link Conversions#asString(Object)}. All other values will be
	 * returned unchanged.
	 *
	 * @see StorageMapping#mapValue(Relatable, Object)
	 */
	@Override
	public Object mapValue(A rAttribute, Object rValue) throws StorageException
	{
		if (rValue instanceof Collection || rValue instanceof Map)
		{
			rValue = Conversions.asString(rValue);
		}

		return rValue;
	}

	/***************************************
	 * Default implementation that stores the referenced object inside a
	 * transaction.
	 *
	 * @see StorageMapping#storeReference(Relatable, Object)
	 */
	@Override
	public void storeReference(Relatable rSourceObject, T rReferencedObject)
		throws StorageException
	{
		TransactionManager.begin();

		try
		{
			// TODO: determine the correct storage if not registered for class
			Storage rStorage =
				StorageManager.getStorage(rReferencedObject.getClass());

			TransactionManager.addTransactionElement(rStorage);

			rStorage.store(rReferencedObject);
			TransactionManager.commit();
		}
		catch (Exception e)
		{
			try
			{
				TransactionManager.rollback();
			}
			catch (Exception eRollback)
			{
				Log.error("Transaction rollback failed", eRollback);
			}

			if (e instanceof StorageException)
			{
				throw (StorageException) e;
			}
			else
			{
				throw new StorageException(e);
			}
		}
	}
}