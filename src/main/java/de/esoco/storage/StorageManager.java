//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2018 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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
import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;
import org.obrel.core.RelationType;
import org.obrel.type.MetaTypes;
import org.obrel.type.StandardTypes;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.QUERY_DEPTH;
import static de.esoco.storage.StorageRelationTypes.STORAGE_DEFINITION;
import static de.esoco.storage.StorageRelationTypes.STORAGE_MAPPING;
import static de.esoco.storage.StorageRelationTypes.STORING;

/**
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
 * storage keys that have no been associated with a specific storage
 * definition.
 * </p>
 *
 * @author eso
 */
public class StorageManager {

	// internal key to put the default storage in a thread map
	private static final Object DEFAULT_STORAGE = "DEFAULT_STORAGE";

	private static final boolean DEBUG_OUTPUT = false;

	private static final Map<Class<?>, MappingFactory<?>> mappingFactoryRegistry =
		new LinkedHashMap<Class<?>, MappingFactory<?>>();

	private static final Map<Object, StorageDefinition> storageDefinitionRegistry =
		new HashMap<Object, StorageDefinition>();

	private static ThreadLocal<Map<StorageDefinition, Storage>> threadStorages;

	private static final RelatedObject storageMetaData = new RelatedObject();

	static {
		StorageRelationTypes.init();

		threadStorages = new ThreadLocal<Map<StorageDefinition, Storage>>() {
			@Override
			protected Map<StorageDefinition, Storage> initialValue() {
				return new HashMap<StorageDefinition, Storage>();
			}
		};
	}

	/**
	 * Private, only static use.
	 */
	private StorageManager() {
	}

	/**
	 * Returns the storage definition for a certain key if it exists.
	 *
	 * @param key The key to check the definition for
	 * @return The storage definition for the given key
	 * @throws StorageException If storage definition exists for the given key
	 */
	static StorageDefinition checkStorageDefinition(Object key)
		throws StorageException {
		StorageDefinition definition = getStorageDefinition(key);

		if (definition == null) {
			throw new StorageException("No storage definition for key " + key);
		}

		return definition;
	}

	/**
	 * Converts a string constraint to the corresponding SQL constraint (for a
	 * SQL LIKE statement) by replacing '*' with '%' and '_' with '?'.
	 *
	 * @param constraint The original constraint string
	 * @return The converted string
	 */
	public static String convertToSqlConstraint(String constraint) {
		constraint = constraint.replaceAll("\\*", "%");
		constraint = constraint.replaceAll("\\?", "_");

		return constraint;
	}

	/**
	 * Returns a new storage instance for a particular storage definition. This
	 * method always creates a new storage instance and in general the methods
	 * {@link #getStorage(Object)} and {@link #releaseStorage(Storage)} should
	 * be preferred by applications because they can perform caching of storage
	 * instances.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param definition The storage definition
	 * @return A new storage instance
	 * @throws StorageException If creating the storage fails
	 */
	static Storage createStorage(StorageDefinition definition)
		throws StorageException {
		Storage storage = definition.createStorage();

		ObjectRelations.copyRelations(storageMetaData, storage, false);
		storage.set(STORAGE_DEFINITION, definition);

		if (definition.hasRelation(QUERY_DEPTH)) {
			storage.set(QUERY_DEPTH, definition.get(QUERY_DEPTH));
		}

		return storage;
	}

	/**
	 * Debug helper method to log stack locations of storage access.
	 *
	 * @param info    The info string to log
	 * @param storage usageCount
	 */
	@SuppressWarnings({ "boxing" })
	private static void debugOutStorageAccess(String info, Storage storage) {
		StackTraceElement[] stackTrace =
			Thread.currentThread().getStackTrace();

		int stackOverhead =
			LogRecord.getStackOverhead(StorageManager.class.getPackage(),
				stackTrace);

		StackTraceElement location = stackTrace[stackOverhead];

		Log.infof("%s STORAGE %s[Usage %d] from %s.%s[%d]\n", info,
			storage.get(StandardTypes.OBJECT_ID), storage.usageCount,
			location.getClassName(), location.getMethodName(),
			location.getLineNumber());
	}

	/**
	 * Returns the storage mapping for a certain datatype class. If no type has
	 * been registered for the type a new instance of {@link ClassMapping} will
	 * be returned.
	 *
	 * @param type The class to return the mapping for mapping
	 * @return The storage mapping for the given type
	 */
	@SuppressWarnings("unchecked")
	public static <T> StorageMapping<T, ?, ?> getMapping(Class<T> type) {
		StorageMapping<T, ?, ?> mapping =
			(StorageMapping<T, ?, ?>) ObjectRelations
				.getRelatable(type)
				.get(STORAGE_MAPPING);

		if (mapping == null) {
			for (Entry<Class<?>, MappingFactory<?>> entry :
				mappingFactoryRegistry.entrySet()) {
				if (entry.getKey().isAssignableFrom(type)) {
					mapping =
						((MappingFactory<T>) entry.getValue()).createMapping(
							type);

					break;
				}
			}

			if (mapping == null) {
				mapping = new ClassMapping<T>(type);
			}
		}

		return mapping;
	}

	/**
	 * Returns the storage mapping factory for a certain base class.
	 *
	 * @param baseClass The base class to register the factory for
	 * @return The mapping factory or NULL if none has been registered yet
	 * @see #registerMappingFactory(Class, MappingFactory)
	 */
	@SuppressWarnings("unchecked")
	public static <T> MappingFactory<T> getMappingFactory(Class<T> baseClass) {
		return (MappingFactory<T>) mappingFactoryRegistry.get(baseClass);
	}

	/**
	 * Returns a storage instance from a storage definition that has been
	 * registered for a certain key. If no specific storage definition can be
	 * found an instance of the default storage will be returned (if the
	 * default
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
	 * @param key The key to return a storage instance for
	 * @return A storage instance from the storage definition associated with
	 * the given key
	 * @throws StorageException If neither a specific nor a default storage
	 *                          definition is available for the given key or if
	 *                          creating the storage fails
	 */
	public static Storage getStorage(Object key) throws StorageException {
		Map<StorageDefinition, Storage> storageMap = threadStorages.get();

		StorageDefinition definition = checkStorageDefinition(key);
		Storage storage = storageMap.get(definition);

		if (storage == null || !storage.isValid()) {
			storage = createStorage(definition);
			storageMap.put(definition, storage);
			storage.set(MetaTypes.MANAGED);
		} else {
			storage.usageCount++;
		}

		if (DEBUG_OUTPUT) {
			debugOutStorageAccess("GET", storage);
		}

		return storage;
	}

	/**
	 * Returns the storage definition for a certain key. Returns the storage
	 * definition for a certain key that has been registered with the method
	 * {@link #registerStorage(StorageDefinition, Object...)}. If no such
	 * definition can be found and a default storage definition has been set
	 * with the method {@link #setDefaultStorage(StorageDefinition)} the
	 * default
	 * will be returned.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param key The storage key
	 * @return Either the matching storage definition, the default definition,
	 * or NULL if none of these has been set
	 */
	public static StorageDefinition getStorageDefinition(Object key) {
		StorageDefinition definition;

		if (key instanceof StorageDefinition) {
			definition = (StorageDefinition) key;
		} else if (storageDefinitionRegistry.containsKey(key)) {
			definition = storageDefinitionRegistry.get(key);
		} else {
			definition = storageDefinitionRegistry.get(DEFAULT_STORAGE);
		}

		return definition;
	}

	/**
	 * Checks whether a certain object is already persistent in a storage. This
	 * will be true if the object has been stored in or retrieved from a
	 * storage
	 * previously.
	 *
	 * @param object The object to check for persistence
	 * @return TRUE if the object is already persistent in a storage
	 */
	public static boolean isPersistent(Object object) {
		Relatable objectRelatable = ObjectRelations.getRelatable(object);

		return objectRelatable.hasFlag(PERSISTENT) ||
			objectRelatable.hasFlag(STORING);
	}

	/**
	 * Returns a new storage instance from a storage definition that has been
	 * registered for a certain key. If no specific storage definition can be
	 * found an instance of the default storage will be returned (if the
	 * default
	 * definition has been set).
	 *
	 * <p>This method always returns a newly created storage instance.
	 * Therefore applications should in general prefer to call
	 * {@link #getStorage(Object)} instead to allow the implementation to cache
	 * storage instance. Only if a new instance is explicitly needed (e.g. to
	 * perform storage operations separately from the default storage instance)
	 * this method should be used. The management of the returned storage
	 * instance is completely up to the calling code. When the returned storage
	 * is no longer needed it must be released by invoking the method
	 * {@link #releaseStorage(Storage)}.</p>
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param key The key to return a storage instance for
	 * @return A new storage instance from the storage definition associated
	 * with the given key
	 * @throws StorageException If neither a specific nor a default storage
	 *                          definition is available
	 */
	@SuppressWarnings("boxing")
	public static Storage newStorage(Object key) throws StorageException {
		StorageDefinition definition = checkStorageDefinition(key);
		Storage storage = createStorage(definition);

		storage.set(MetaTypes.MANAGED, false);

		if (DEBUG_OUTPUT) {
			debugOutStorageAccess("NEW", storage);
		}

		return storage;
	}

	/**
	 * Registers a storage mapping factory for a certain base class. This
	 * factory will then be used to create storage mappings for the given class
	 * and all it's subclasses.
	 *
	 * @param baseClass The base class to register the factory for
	 * @param factory   The factor to register
	 */
	public static <T> void registerMappingFactory(Class<T> baseClass,
		MappingFactory<T> factory) {
		mappingFactoryRegistry.put(baseClass, factory);
	}

	/**
	 * Registers a storage type by associating it's definition with certain
	 * keys
	 * for the lookup of storages. If a storage is queried through either of
	 * the
	 * {@link #getStorage(Object)} or {@link #newStorage(Object)} methods the
	 * storage definition associated with the given keys will be used to create
	 * a new storage if necessary. If no storage definition has been associated
	 * with a certain key the definition that has been set by means of the
	 * method {@link #setDefaultStorage(StorageDefinition)} will be used.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param definition The storage definition instance to register
	 * @param keys       The keys to associate the definition with (must not be
	 *                   empty)
	 * @throws IllegalArgumentException If either argument is NULL or if
	 * keys is
	 *                                  empty
	 */
	public static void registerStorage(StorageDefinition definition,
		Object... keys) {
		if (definition == null || keys == null || keys.length == 0) {
			throw new IllegalArgumentException(
				"Arguments must not be NULL or empty");
		}

		for (Object key : keys) {
			storageDefinitionRegistry.put(key, definition);
		}
	}

	/**
	 * Releases a storage instance that has previously been acquired from the
	 * storage manager. Will be invoked internally by
	 * {@link Storage#release()}.
	 *
	 * @param storage The storage to release
	 */
	static void releaseStorage(Storage storage) {
		if (DEBUG_OUTPUT) {
			debugOutStorageAccess("RELEASE", storage);
		}

		if (--storage.usageCount == 0) {
			if (storage.hasFlag(MetaTypes.MANAGED)) {
				threadStorages.get().remove(storage.get(STORAGE_DEFINITION));
			}

			storage.close();

			if (DEBUG_OUTPUT) {
				debugOutStorageAccess("CLOSE", storage);
			}
		}
	}

	/**
	 * Sets the definition for the default storage. This storage definition
	 * will
	 * be used by the method {@link #getStorage(Object)} for all keys for which
	 * no specific storage definition has been registered through the method
	 * {@link #registerStorage(StorageDefinition, Object...)}.
	 *
	 * <p>See the class documentation for more information about the storage
	 * management.</p>
	 *
	 * @param definition The new default storage definition
	 */
	public static void setDefaultStorage(StorageDefinition definition) {
		registerStorage(definition, DEFAULT_STORAGE);
	}

	/**
	 * Sets the storage meta data.
	 *
	 * @param type  The new storage meta data
	 * @param value The new storage meta data
	 */
	public static <T> void setStorageMetaData(RelationType<T> type, T value) {
		storageMetaData.set(type, value);
	}

	/**
	 * Performs a shutdown of the storage manager and frees all allocated
	 * resources.
	 */
	public static void shutdown() {
		Map<StorageDefinition, Storage> storageMap = threadStorages.get();

		if (storageMap != null) {
			// release storage of main application thread
			storageMap.values().forEach(Storage::release);
		}

		threadStorages = null;
	}

	/**
	 * The interface for classes that create storage mappings.
	 *
	 * @author eso
	 */
	public interface MappingFactory<T> {

		/**
		 * Must be implemented to create a new storage mapping for a certain
		 * type of storage object.
		 *
		 * @param type The class of the storage object
		 * @return A new storage mapping for the given type
		 */
		StorageMapping<T, ?, ?> createMapping(Class<T> type);
	}
}
