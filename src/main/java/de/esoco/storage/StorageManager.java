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
package de.esoco.storage;

import de.esoco.lib.logging.Log;
import de.esoco.lib.logging.LogRecord;

import de.esoco.storage.mapping.ClassMapping;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;
import org.obrel.core.RelationType;
import org.obrel.type.MetaTypes;
import org.obrel.type.StandardTypes;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.QUERY_DEPTH;
import static de.esoco.storage.StorageRelationTypes.STORAGE_DEFINITION;
import static de.esoco.storage.StorageRelationTypes.STORAGE_MAPPING;
import static de.esoco.storage.StorageRelationTypes.STORING;


/********************************************************************
 * A static factory class that provides methods to create, access, and manage
 * storage instances. This includes a thread-based storage cache for the
 * efficient use of storages by client and server applications. To acquire a
 * storage application code must invoke {@link #getStorage(Object)}. The
 * argument to this method is a key object for which a storage definition must
 * have been registered (see below). The returned value is a storage instance
 * for the given key and the calling thread. Therefore separate application
 * threads will receive different storage objects to allow the parallel use of
 * the the same storage without explicit effort for the application.
 *
 * <p>If an application explicitly needs a separate storage that is not shared
 * by other method invocations in the same thread it should invoke the method
 * {@link #newStorage(Object)} which returns a new storage for a key similar to
 * {@link #getStorage(Object)}. The storage instances returned by these method
 * will not be managed by the framework, so that must be done by the application
 * itself.</p>
 *
 * <p>Any storage instance that has been obtained from one of the above methods
 * and is no longer needed must always be released to the manager by invoking
 * the method {@link Storage#release()}. This will allow the storage manager to
 * re-use storage instances, even if they had been created explicitly. Storages
 * should always only be held by application code as long as they are really
 * needed. A good rule of thumb is that the code block that acquired a storage
 * reference should also release it.</p>
 *
 * <p>The storages that are available from the manager are defined through the
 * method {@link #registerStorage(StorageDefinition, Object...)} by registering
 * a storage definition with a corresponding key object. The key can be an
 * arbitrary object and it's meaning is completely application dependent. An
 * application could use, for example, the classes of the objects stored as the
 * keys to associated storage definitions. Or it may use specific constants like
 * strings or enums to identify different storage areas. An application may also
 * (or alternatively) register a default storage definition through the method
 * {@link #setDefaultStorage(StorageDefinition)} which will then be used for all
 * storage keys that have no been associated with a specific storage definition.
 * </p>
 *
 * @author eso
 */
public class StorageManager
{
	//~ Static fields/initializers ---------------------------------------------

	// internal key to put the default storage in a thread map
	private static final Object DEFAULT_STORAGE = "DEFAULT_STORAGE";

	private static Map<Class<?>, MappingFactory<?>> aMappingFactoryRegistry =
		new LinkedHashMap<Class<?>, MappingFactory<?>>();

	private static Map<Object, StorageDefinition> aStorageDefinitionRegistry =
		new HashMap<Object, StorageDefinition>();

	private static ThreadLocal<Map<StorageDefinition, Storage>> aThreadStorages;

	private static RelatedObject aStorageMetaData = new RelatedObject();

	private static final boolean DEBUG_OUTPUT = false;

	static
	{
		StorageRelationTypes.init();

		aThreadStorages =
			new ThreadLocal<Map<StorageDefinition, Storage>>()
			{
				@Override
				protected Map<StorageDefinition, Storage> initialValue()
				{
					return new HashMap<StorageDefinition, Storage>();
				}
			};
	}

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Private, only static use.
	 */
	private StorageManager()
	{
	}

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Converts a string constraint to the corresponding SQL constraint (for a
	 * SQL LIKE statement) by replacing '*' with '%' and '_' with '?'.
	 *
	 * @param  sConstraint The original constraint string
	 *
	 * @return The converted string
	 */
	public static String convertToSqlConstraint(String sConstraint)
	{
		sConstraint = sConstraint.replaceAll("\\*", "%");
		sConstraint = sConstraint.replaceAll("\\?", "_");

		return sConstraint;
	}

	/***************************************
	 * Returns the storage mapping for a certain datatype class. If no type has
	 * been registered for the type a new instance of {@link ClassMapping} will
	 * be returned.
	 *
	 * @param  rType The class to return the mapping for mapping
	 *
	 * @return The storage mapping for the given type
	 */
	@SuppressWarnings("unchecked")
	public static <T> StorageMapping<T, ?, ?> getMapping(Class<T> rType)
	{
		StorageMapping<T, ?, ?> rMapping =
			(StorageMapping<T, ?, ?>) ObjectRelations.getRelatable(rType)
													 .get(STORAGE_MAPPING);

		if (rMapping == null)
		{
			for (Entry<Class<?>, MappingFactory<?>> rEntry :
				 aMappingFactoryRegistry.entrySet())
			{
				if (rEntry.getKey().isAssignableFrom(rType))
				{
					rMapping =
						((MappingFactory<T>) rEntry.getValue()).createMapping(rType);

					break;
				}
			}

			if (rMapping == null)
			{
				rMapping = new ClassMapping<T>(rType);
			}
		}

		return rMapping;
	}

	/***************************************
	 * Returns a storage instance from a storage definition that has been
	 * registered for a certain key. If no specific storage definition can be
	 * found an instance of the default storage will be returned (if the default
	 * storage definition has been set).
	 *
	 * <p>This method should normally be preferred over the direct creation of
	 * storages with the method {@link #newStorage(Object)} because it performs
	 * a thread-based caching of storages and thus provides a way to access a
	 * storage over multiple stack levels without the requirement to pass
	 * storages by parameter. After user the storage should be released as soon
	 * as possible by invoking the method {@link #releaseStorage(Storage)}.</p>
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param  rKey The key to return a storage instance for
	 *
	 * @return A storage instance from the storage definition associated with
	 *         the given key
	 *
	 * @throws StorageException If neither a specific nor a default storage
	 *                          definition is available for the given key or if
	 *                          creating the storage fails
	 */
	public static Storage getStorage(Object rKey) throws StorageException
	{
		Map<StorageDefinition, Storage> rStorageMap = aThreadStorages.get();

		StorageDefinition rDefinition = checkStorageDefinition(rKey);
		Storage			  rStorage    = rStorageMap.get(rDefinition);

		if (rStorage == null || !rStorage.isValid())
		{
			rStorage = createStorage(rDefinition);
			rStorageMap.put(rDefinition, rStorage);
			rStorage.set(MetaTypes.MANAGED);
		}
		else
		{
			rStorage.nUsageCount++;
		}

		if (DEBUG_OUTPUT)
		{
			debugOutStorageAccess("GET", rStorage);
		}

		return rStorage;
	}

	/***************************************
	 * Returns the storage definition for a certain key. Returns the storage
	 * definition for a certain key that has been registered with the method
	 * {@link #registerStorage(StorageDefinition, Object...)}. If no such
	 * definition can be found and a default storage definition has been set
	 * with the method {@link #setDefaultStorage(StorageDefinition)} the default
	 * will be returned.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param  rKey The storage key
	 *
	 * @return Either the matching storage definition, the default definition,
	 *         or NULL if none of these has been set
	 */
	public static StorageDefinition getStorageDefinition(Object rKey)
	{
		StorageDefinition rDefinition;

		if (rKey instanceof StorageDefinition)
		{
			rDefinition = (StorageDefinition) rKey;
		}
		else if (aStorageDefinitionRegistry.containsKey(rKey))
		{
			rDefinition = aStorageDefinitionRegistry.get(rKey);
		}
		else
		{
			rDefinition = aStorageDefinitionRegistry.get(DEFAULT_STORAGE);
		}

		return rDefinition;
	}

	/***************************************
	 * Checks whether a certain object is already persistent in a storage. This
	 * will be true if the object has been stored in or retrieved from a storage
	 * previously.
	 *
	 * @param  rObject The object to check for persistence
	 *
	 * @return TRUE if the object is already persistent in a storage
	 */
	public static boolean isPersistent(Object rObject)
	{
		Relatable rObjectRelatable = ObjectRelations.getRelatable(rObject);

		return rObjectRelatable.hasFlag(PERSISTENT) ||
			   rObjectRelatable.hasFlag(STORING);
	}

	/***************************************
	 * Returns a new storage instance from a storage definition that has been
	 * registered for a certain key. If no specific storage definition can be
	 * found an instance of the default storage will be returned (if the default
	 * definition has been set).
	 *
	 * <p>This method always returns a newly created storage instance. Therefore
	 * applications should in general prefer to call {@link #getStorage(Object)}
	 * instead to allow the implementation to cache storage instance. Only if a
	 * new instance is explicitly needed (e.g. to perform storage operations
	 * separately from the default storage instance) this method should be used.
	 * The management of the returned storage instance is completely up to the
	 * calling code. When the returned storage is no longer needed it must be
	 * released by invoking the method {@link #releaseStorage(Storage)}.</p>
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param  rKey The key to return a storage instance for
	 *
	 * @return A new storage instance from the storage definition associated
	 *         with the given key
	 *
	 * @throws StorageException If neither a specific nor a default storage
	 *                          definition is available
	 */
	@SuppressWarnings("boxing")
	public static Storage newStorage(Object rKey) throws StorageException
	{
		StorageDefinition rDefinition = checkStorageDefinition(rKey);
		Storage			  aStorage    = createStorage(rDefinition);

		aStorage.set(MetaTypes.MANAGED, false);

		if (DEBUG_OUTPUT)
		{
			debugOutStorageAccess("NEW", aStorage);
		}

		return aStorage;
	}

	/***************************************
	 * Registers a storage mapping factory for a certain base class. This
	 * factory will then be used to create storage mappings for the given class
	 * and all it's subclasses.
	 *
	 * @param rBaseClass The base class to register the factory for
	 * @param rFactory   The factor to register
	 */
	public static <T> void registerMappingFactory(
		Class<T>		  rBaseClass,
		MappingFactory<T> rFactory)
	{
		aMappingFactoryRegistry.put(rBaseClass, rFactory);
	}

	/***************************************
	 * Registers a storage type by associating it's definition with certain keys
	 * for the lookup of storages. If a storage is queried through either of the
	 * {@link #getStorage(Object)} or {@link #newStorage(Object)} methods the
	 * storage definition associated with the given keys will be used to create
	 * a new storage if necessary. If no storage definition has been associated
	 * with a certain key the definition that has been set by means of the
	 * method {@link #setDefaultStorage(StorageDefinition)} will be used.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param  rDefinition The storage definition instance to register
	 * @param  rKeys       The keys to associate the definition with (must not
	 *                     be empty)
	 *
	 * @throws IllegalArgumentException If either argument is NULL or if rKeys
	 *                                  is empty
	 */
	public static void registerStorage(
		StorageDefinition rDefinition,
		Object... 		  rKeys)
	{
		if (rDefinition == null || rKeys == null || rKeys.length == 0)
		{
			throw new IllegalArgumentException("Arguments must not be NULL or empty");
		}

		for (Object rKey : rKeys)
		{
			aStorageDefinitionRegistry.put(rKey, rDefinition);
		}
	}

	/***************************************
	 * Sets the definition for the default storage. This storage definition will
	 * be used by the method {@link #getStorage(Object)} for all keys for which
	 * no specific storage definition has been registered through the method
	 * {@link #registerStorage(StorageDefinition, Object...)}.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param rDefinition The new default storage definition
	 */
	public static void setDefaultStorage(StorageDefinition rDefinition)
	{
		registerStorage(rDefinition, DEFAULT_STORAGE);
	}

	/***************************************
	 * Sets the storage meta data.
	 *
	 * @param rType  The new storage meta data
	 * @param rValue The new storage meta data
	 */
	public static <T> void setStorageMetaData(RelationType<T> rType, T rValue)
	{
		aStorageMetaData.set(rType, rValue);
	}

	/***************************************
	 * Performs a shutdown of the storage manager and frees all allocated
	 * resources.
	 */
	public static void shutdown()
	{
		aThreadStorages = null;
	}

	/***************************************
	 * Returns the storage definition for a certain key if it exists.
	 *
	 * @param  rKey The key to check the definition for
	 *
	 * @return The storage definition for the given key
	 *
	 * @throws StorageException If storage definition exists for the given key
	 */
	static StorageDefinition checkStorageDefinition(Object rKey)
		throws StorageException
	{
		StorageDefinition rDefinition = getStorageDefinition(rKey);

		if (rDefinition == null)
		{
			throw new StorageException("No storage definition for key " + rKey);
		}

		return rDefinition;
	}

	/***************************************
	 * Returns a new storage instance for a particular storage definition. This
	 * method always creates a new storage instance and in general the methods
	 * {@link #getStorage(Object)} and {@link #releaseStorage(Storage)} should
	 * be preferred by applications because they can perform caching of storage
	 * instances.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param  rDefinition The storage definition
	 *
	 * @return A new storage instance
	 *
	 * @throws StorageException If creating the storage fails
	 */
	static Storage createStorage(StorageDefinition rDefinition)
		throws StorageException
	{
		Storage aStorage = rDefinition.createStorage();

		ObjectRelations.copyRelations(aStorageMetaData, aStorage, false);
		aStorage.set(STORAGE_DEFINITION, rDefinition);

		if (rDefinition.hasRelation(QUERY_DEPTH))
		{
			aStorage.set(QUERY_DEPTH, rDefinition.get(QUERY_DEPTH));
		}

		return aStorage;
	}

	/***************************************
	 * Releases a storage instance that has previously been acquired from the
	 * storage manager. Will be invoked internally by {@link Storage#release()}.
	 *
	 * @param  rStorage The storage to release
	 *
	 * @throws StorageException If releasing the storage fails
	 */
	static void releaseStorage(Storage rStorage)
	{
		if (DEBUG_OUTPUT)
		{
			debugOutStorageAccess("RELEASE", rStorage);
		}

		if (--rStorage.nUsageCount == 0)
		{
			if (rStorage.hasFlag(MetaTypes.MANAGED))
			{
				aThreadStorages.get().remove(rStorage.get(STORAGE_DEFINITION));
			}

			rStorage.close();

			if (DEBUG_OUTPUT)
			{
				debugOutStorageAccess("CLOSE", rStorage);
			}
		}
	}

	/***************************************
	 * Debug helper method to log stack locations of storage access.
	 *
	 * @param sInfo    The info string to log
	 * @param rStorage nUsageCount
	 */
	@SuppressWarnings({ "boxing" })
	private static void debugOutStorageAccess(String sInfo, Storage rStorage)
	{
		StackTraceElement[] rStackTrace =
			Thread.currentThread().getStackTrace();

		int nStackOverhead =
			LogRecord.getStackOverhead(StorageManager.class.getPackage(),
									   rStackTrace);

		StackTraceElement rLocation = rStackTrace[nStackOverhead];

		Log.infof("%s STORAGE %s[Usage %d] from %s.%s[%d]\n",
				  sInfo,
				  rStorage.get(StandardTypes.OBJECT_ID),
				  rStorage.nUsageCount,
				  rLocation.getClassName(),
				  rLocation.getMethodName(),
				  rLocation.getLineNumber());
	}

	//~ Inner Interfaces -------------------------------------------------------

	/********************************************************************
	 * The interface for classes that create storage mappings.
	 *
	 * @author eso
	 */
	public static interface MappingFactory<T>
	{
		//~ Methods ------------------------------------------------------------

		/***************************************
		 * Must be implemented to create a new storage mapping for a certain
		 * type of storage object.
		 *
		 * @param  rType The class of the storage object
		 *
		 * @return A new storage mapping for the given type
		 */
		public StorageMapping<T, ?, ?> createMapping(Class<T> rType);
	}
}
