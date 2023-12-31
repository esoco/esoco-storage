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

import de.esoco.lib.logging.Log;
import de.esoco.lib.manage.Releasable;
import de.esoco.lib.manage.Transactional;

import java.util.Collection;

import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.STORING;

import static org.obrel.type.MetaTypes.MODIFIED;

/**
 * This is the base class for all storage implementations.
 *
 * @author eso
 */
public abstract class Storage extends RelatedObject
	implements Transactional, Releasable {

	/**
	 * The name of the system property to check for the global disabling of any
	 * storage delete functionality.
	 */
	public static final String PROPERTY_DELETE_DISABLED =
		"esoco.storage.disable_delete";

	int nUsageCount = 1;

	/**
	 * Method from the {@link Transactional} interface that commits the
	 * currently active storage transaction. Only has an effect if the storage
	 * implementation supports transactions.
	 *
	 * @throws StorageException If the commit fails
	 */
	@Override
	public abstract void commit() throws StorageException;

	/**
	 * Deletes a particular object from the storage.
	 *
	 * @param rObject The object to delete
	 * @throws StorageException If deleting the object fails
	 */
	public final void delete(Object rObject) throws StorageException {
		checkStorageDeleteEnabled(
			StorageManager.getMapping(rObject.getClass()));
		deleteObject(rObject);
	}

	/**
	 * Returns the implementation-specific name of this storage instance.
	 *
	 * @return The storage implementation name
	 */
	public abstract String getStorageImplementationName();

	/**
	 * Checks whether this storage has already been initialized for a
	 * particular
	 * object type.
	 *
	 * @param rStoredType The object type to check for initialization
	 * @return TRUE if the object storage has been initialized
	 * @throws StorageException If querying the storage implementation fails
	 * @see #initObjectStorage(Class)
	 * @see #removeObjectStorage(Class)
	 */
	public final boolean hasObjectStorage(Class<?> rStoredType)
		throws StorageException {
		return hasObjectStorage(StorageManager.getMapping(rStoredType));
	}

	/**
	 * Initializes this storage for a particular object type to allow
	 * storing or
	 * retrieving objects of this type. What effect this method has depends on
	 * the actual storage implementation. For example, a JDBC storage may
	 * create
	 * a new database table on invocation while other implementations may not
	 * need such an initialization (in which case this method will not have any
	 * effect). If necessary this call will also initialize the storage for
	 * child object types of the given type.
	 *
	 * <p>For long-term storages this method only needs to be invoked once for
	 * a particular object type. Additional invocations with the same object
	 * type will not harm but may need additional test and should therefore be
	 * avoided if possible. If necessary the call should occur during
	 * application initialization only.</p>
	 *
	 * @param rStoredType The object type to initialize the storage for
	 * @throws StorageException If the initialization fails
	 * @see #hasObjectStorage(Class)
	 * @see #removeObjectStorage(Class)
	 */
	public final void initObjectStorage(Class<?> rStoredType)
		throws StorageException {
		initObjectStorage(StorageManager.getMapping(rStoredType));
	}

	/**
	 * Checks whether the storage is still valid for further use. Some
	 * implementations may have timeouts or are based on network connections
	 * that are closed and can therefore return FALSE in such a case to
	 * indicate
	 * that a new storage instance must be used.
	 *
	 * @return TRUE if the storage is valid for further storage operations
	 */
	public abstract boolean isValid();

	/**
	 * Returns a query for a certain object type and (optional) query criteria.
	 * The query is defined with an instance of {@link QueryPredicate} which
	 * contains the mapping of the objects to be queried as well as the query
	 * criteria.
	 *
	 * @param rQueryPredicate The predicate that defines the query
	 * @return A query object matching the given query predicate
	 * @throws StorageException If creating the query fails
	 */
	public abstract <T> Query<T> query(QueryPredicate<T> rQueryPredicate)
		throws StorageException;

	/**
	 * Must be called to release this storage, indicating that it won't be used
	 * any longer by the calling party. Any further invocation of a method on
	 * the storage afterwards may cause an exception. Releasing a storage
	 * doesn't necessarily mean that the storage will also be closed. It may be
	 * re-used by a connection-pooling mechanism.
	 *
	 * <p>Storage implementations may override this method to perform
	 * additional maintenance on a release but it is necessary that they always
	 * invoke this superclass method. In most cases it should be more
	 * appropriate to perform the cleanup in the implementation of the
	 * {@link #close()} method which is invoked by the storage management.</p>
	 *
	 * @see Releasable#release()
	 */
	@Override
	public void release() {
		StorageManager.releaseStorage(this);
	}

	/**
	 * Removes the storage data and/or structures for a particular object type.
	 * What effect this method has depends on the actual storage
	 * implementation.
	 * For example, a JDBC storage may drop the associated database table on
	 * invocation while other implementations may simply ignore this call.
	 * Invocations of this method if the storage hasn't been initialized for
	 * this object type yet will be ignored.
	 *
	 * <p>For safety reasons and other than the initialization this call will
	 * NOT remove the storages for subordinate types of the the given type
	 * if it
	 * has such. This is intentional to prevent the accidental cascaded
	 * deletion
	 * of storage data. If an application wants to remove a hierarchy of object
	 * storages it must invoke this method explicitly for each type.</p>
	 *
	 * @param rStoredType The object type to initialize the storage for
	 * @throws StorageException If the initialization fails
	 * @see #initObjectStorage(Class)
	 * @see #hasObjectStorage(Class)
	 */
	public void removeObjectStorage(Class<?> rStoredType)
		throws StorageException {
		StorageMapping<?, ?, ?> rMapping =
			StorageManager.getMapping(rStoredType);

		checkStorageDeleteEnabled(rMapping);
		removeObjectStorage(rMapping);
	}

	/**
	 * Method from the {@link Transactional} interface that performs a rollback
	 * of the currently active storage transaction. Only has an effect if the
	 * storage implementation supports transactions.
	 *
	 * <p><b>Attention:</b> This method call affects the storage only, but will
	 * not reset any objects from the storage that have been modified by the
	 * application.</p>
	 *
	 * @throws StorageException If the rollback fails
	 */
	@Override
	public abstract void rollback() throws StorageException;

	/**
	 * Stores a particular object or a collection of objects. If the argument
	 * object is a collection, all it's elements will be stored successively in
	 * the order in which they appear in the collection. Any objects that have
	 * been stored in or retrieved from this storage before will be updated.
	 * Invokes the abstract method {@link #storeObject(Object)} which must be
	 * implemented by subclasses. If a storage implementation provides a more
	 * efficient way to store collections it may also override this method.
	 *
	 * @param rObject The object or collection of objects to store
	 * @throws StorageException If storing an object fails
	 */
	public final void store(Object rObject) throws StorageException {
		if (rObject instanceof Collection<?>) {
			storeCollection((Collection<?>) rObject);
		} else {
			storeSingleObject(rObject);
		}
	}

	/**
	 * Checks whether deleting from this storage is enabled in the current
	 * context and for a given type. If not an exception will be thrown.
	 *
	 * @param rMapping The mapping of the type to be deleted from the storage
	 * @throws StorageException If deleting is not enabled
	 */
	protected void checkStorageDeleteEnabled(StorageMapping<?, ?, ?> rMapping)
		throws StorageException {
		if (!rMapping.isDeleteAllowed()) {
			throw new StorageException(String.format("Delete not enabled for ",
				rMapping.getMappedType()));
		} else if (Boolean.getBoolean(PROPERTY_DELETE_DISABLED)) {
			throw new StorageException("Delete globally disabled");
		}
	}

	/**
	 * Must be implemented to close the storage. This method should normally
	 * not
	 * throw exceptions, therefore no storage exception is declared. Only a
	 * failure that would cause the storage to be left in an inconsistent state
	 * should be signaled with a corresponding runtime exception.
	 *
	 * <p>Applications are not supposed to call this method but must use the
	 * method {@link #release()} instead. The closing of storages is handled
	 * internally by the storage manager.</p>
	 */
	protected abstract void close();

	/**
	 * Must be implemented by subclasses to delete an object from the storage.
	 *
	 * @param rObject The object to delete from the storage
	 * @throws StorageException If deleting the object fails
	 */
	protected abstract void deleteObject(Object rObject)
		throws StorageException;

	/**
	 * Must be implemented by subclasses to check whether a storage has already
	 * been initialized for a certain storage mapping.
	 *
	 * @see #hasObjectStorage(Class)
	 */
	protected abstract boolean hasObjectStorage(
		StorageMapping<?, ?, ?> rMapping) throws StorageException;

	/**
	 * Must be implemented by subclasses to initialize a storage for a certain
	 * storage mapping and all it's subordinate types if applicable. Additional
	 * invocations with the same object type should be tolerated by
	 * implementations with as few overhead as possible. If the mapping has
	 * subordinate mappings these should also be initialized in the storage.
	 *
	 * @see #initObjectStorage(Class)
	 */
	protected abstract void initObjectStorage(StorageMapping<?, ?, ?> rMapping)
		throws StorageException;

	/**
	 * Must be implemented by subclasses to remove the storage data and/or
	 * structures for a certain storage mapping. Invocation if no object
	 * storage
	 * exists should be ignored. If the mapping has subordinate mappings these
	 * MUST NOT be removed from the storage, only the designated mapping.
	 *
	 * @see #removeObjectStorage(Class)
	 */
	protected abstract void removeObjectStorage(
		StorageMapping<?, ?, ?> rMapping) throws StorageException;

	/**
	 * Stores a collection of objects. The default implementation invokes the
	 * method stores each collection element as a single object. It can be
	 * overridden buy subclasses that provide a more efficient way to store
	 * multiple objects.
	 *
	 * @param rObjects The collection of objects to store
	 * @throws StorageException If storing an object fails
	 */
	protected void storeCollection(Collection<?> rObjects)
		throws StorageException {
		for (Object rElement : rObjects) {
			storeSingleObject(rElement);
		}
	}

	/**
	 * This method must be implemented by subclasses to store a single object.
	 *
	 * @param rObject The object to store
	 * @throws Exception Any exception may be thrown if storing the object
	 *                   fails
	 */
	protected abstract void storeObject(Object rObject) throws Exception;

	/**
	 * Internal method to execute the storing of a single object.
	 *
	 * @param rObject The object to store
	 * @throws StorageException If storing the object fails
	 */
	private void storeSingleObject(Object rObject) throws StorageException {
		Relatable rObjectRelatable = ObjectRelations.getRelatable(rObject);

		rObjectRelatable.set(STORING);

		try {
			storeObject(rObject);

			// PERSISTENT is final, therefore only set if it doesn't exist
			if (!rObjectRelatable.hasRelation(PERSISTENT)) {
				rObjectRelatable.set(PERSISTENT);
			}

			// only reset MODIFIED flag if relation exists, i.e. object
			// supports
			// modification tracking
			if (rObjectRelatable.hasRelation(MODIFIED)) {
				rObjectRelatable.set(MODIFIED, Boolean.FALSE);
			}

			// After store handler must be invoked AFTER setting flags!
			if (rObject instanceof AfterStoreHandler) {
				((AfterStoreHandler) rObject).afterStore();
			}
		} catch (Exception e) {
			if (e instanceof StorageException) {
				throw (StorageException) e;
			} else {
				String sMessage = "Store failed: " + rObject;

				Log.error(sMessage, e);
				throw new StorageException(sMessage, e);
			}
		} finally {
			rObjectRelatable.deleteRelation(STORING);
		}
	}
}
