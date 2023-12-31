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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link List} implementation that receives it's content from a storage
 * query. The list elements will only be queried when the list is accessed for
 * the first time.
 *
 * @author eso
 */
public class QueryList<T> extends AbstractList<T> {

	private final StorageDefinition storageDefinition;

	private final QueryPredicate<T> queryPredicate;

	private int size = -1;

	private ElementInitializer<T> initializer = null;

	private ArrayList<T> elements = null;

	private boolean queried = false;

	/**
	 * Creates a new instance. This no-argument constructor exists for
	 * compliance with the collections API specification and will create an
	 * empty list without a query that can be modified freely.
	 */
	public QueryList() {
		storageDefinition = null;
		queryPredicate = null;
		elements = new ArrayList<T>();
		queried = true;
	}

	/**
	 * Creates a new instance.
	 *
	 * @param definition  The definition of the storage to query the list
	 *                    elements from
	 * @param childQuery  The query to read the list elements from
	 * @param size        The size of the list after querying or -1 if unknown
	 * @param initializer The initializer for the queried list elements
	 */
	public QueryList(StorageDefinition definition,
		QueryPredicate<T> childQuery,
		int size, ElementInitializer<T> initializer) {
		this.storageDefinition = definition;
		this.queryPredicate = childQuery;
		this.size = size;
		this.initializer = initializer;
	}

	/**
	 * @see AbstractList#add(int, Object)
	 */
	@Override
	public void add(int index, T element) {
		query();

		elements.add(index, element);
	}

	/**
	 * @see AbstractList#get(int)
	 */
	@Override
	public T get(int index) {
		query();

		return elements.get(index);
	}

	/**
	 * Returns the underlying storage query of this list.
	 *
	 * @return The query
	 */
	public final QueryPredicate<T> getQueryPredicate() {
		return queryPredicate;
	}

	/**
	 * Checks whether this list has already been queried.
	 *
	 * @return TRUE if this list has been queried already
	 */
	public final boolean isQueried() {
		return queried;
	}

	/**
	 * Queries the elements of this list if it hasn't been done already. The
	 * query is normally performed automatically and this method normally
	 * doesn't need to be invoked by application code.
	 *
	 * @throws StorageRuntimeException If performing the query fails
	 */
	public synchronized void query() throws StorageRuntimeException {
		if (!queried) {
			Storage storage = null;

			try {
				storage = StorageManager.getStorage(storageDefinition);

				Query<T> query = storage.query(queryPredicate);

				try {
					QueryResult<T> result = query.execute();
					ArrayList<T> resultObjects = new ArrayList<T>();

					while (result.hasNext()) {
						resultObjects.add(result.next());
					}

					if (initializer != null) {
						initializer.initElements(resultObjects);
					}

					elements = resultObjects;
					size = elements.size();
					queried = true;
				} finally {
					query.close();
				}
			} catch (StorageException e) {
				throw new StorageRuntimeException(e);
			} finally {
				if (storage != null) {
					storage.release();
				}
			}
		}
	}

	/**
	 * @see AbstractList#remove(int)
	 */
	@Override
	public T remove(int index) {
		query();

		return elements.remove(index);
	}

	/**
	 * @see AbstractList#set(int, Object)
	 */
	@Override
	public T set(int index, T element) {
		query();

		return elements.set(index, element);
	}

	/**
	 * @see AbstractList#size()
	 */
	@Override
	public int size() {
		if (size < 0) {
			query();
		}

		return elements != null ? elements.size() : size;
	}

	/**
	 * A simple interface that provides a method to initialize the list
	 * elements
	 * after they have been queried.
	 *
	 * @author eso
	 */
	public interface ElementInitializer<T> {

		/**
		 * Will be invoked immediately after the list elements have been
		 * queried.
		 *
		 * @param elements The list element
		 */
		void initElements(List<T> elements);
	}
}
