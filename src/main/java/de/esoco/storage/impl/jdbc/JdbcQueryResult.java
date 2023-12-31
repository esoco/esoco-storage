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
package de.esoco.storage.impl.jdbc;

import de.esoco.lib.logging.Log;
import de.esoco.lib.manage.Closeable;
import de.esoco.storage.QueryList;
import de.esoco.storage.QueryList.ElementInitializer;
import de.esoco.storage.QueryPredicate;
import de.esoco.storage.QueryResult;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageMapping;
import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.QUERY_DEPTH;
import static de.esoco.storage.StorageRelationTypes.STORAGE_DEFINITION;

/**
 * Implementation of the {@link QueryResult} interface that is backed by a JDBC
 * {@link ResultSet}.
 */
class JdbcQueryResult<T> extends RelatedObject implements QueryResult<T> {

	private final JdbcStorage storage;

	private final StorageMapping<T, Relatable, ?> mapping;

	private final ResultSet resultSet;

	private final boolean isChildQuery;

	private int offset = 0;

	private boolean offsetRelative = false;

	private boolean hasNext = false;

	/**
	 * Creates a new QueryResult from a JDBC prepared statement.
	 *
	 * @param storage        The storage to perform the query on
	 * @param storageMapping The storage mapping
	 * @param resultSet      queryStatement The JDBC statement to execute
	 * @param offset         The initial offset to position the result at
	 * @param isChildQuery   TRUE if this is a query for child objects
	 */
	JdbcQueryResult(JdbcStorage storage,
		StorageMapping<T, Relatable, ?> storageMapping, ResultSet resultSet,
		int offset, boolean isChildQuery) throws SQLException {
		this.storage = storage;
		this.mapping = storageMapping;
		this.resultSet = resultSet;
		this.offset = offset + 1; // +1 for 1-based JDBC indexing
		this.isChildQuery = isChildQuery;
	}

	/**
	 * @see Closeable#close()
	 */
	@Override
	public void close() {
		try {
			resultSet.close();
		} catch (SQLException e) {
			Log.error("Closing ResultSet failed", e);
		}
	}

	/**
	 * @see QueryResult#hasNext()
	 */
	@Override
	public boolean hasNext() throws StorageException {
		try {
			if (offset != 0) {
				if (offsetRelative) {
					hasNext = resultSet.relative(offset);
				} else {
					hasNext = resultSet.absolute(offset);
				}

				offset = 0;
			} else {
				hasNext = resultSet.next();
			}
		} catch (SQLException e) {
			throw new StorageException(e);
		}

		return hasNext;
	}

	/**
	 * @see QueryResult#next()
	 */
	@Override
	public T next() throws StorageException {
		T result = null;

		if (hasNext) {
			try {
				int resultSize = resultSet.getMetaData().getColumnCount();

				boolean useChildCounts = storage.isChildCountsEnabled(mapping);

				int childMappings = mapping.getChildMappings().size();
				int columns =
					useChildCounts ? resultSize - childMappings : resultSize;

				List<Object> values = new ArrayList<Object>(columns);
				int[] childCounts = null;

				for (int i = 1; i <= columns; i++) {
					Object value = resultSet.getObject(i);

					values.add(value);
				}

				if (useChildCounts) {
					childCounts = new int[childMappings];

					for (int i = 0; i < childMappings; i++) {
						childCounts[i] = resultSet.getInt(++columns);
					}
				}

				result = mapping.createObject(values, isChildQuery);

				Relatable resultRelatable =
					ObjectRelations.getRelatable(result);

				// QUERY_DEPTH will be Integer.MAX_VALUE if not set
				int queryDepth = get(QUERY_DEPTH).intValue();

				// read children until query depth is zero, but only if
				// parent object is not marked as persistent already
				// which also means that is has been read completely
				// and is returned from a cache
				if (!resultRelatable.hasFlag(PERSISTENT)) {
					resultRelatable.set(PERSISTENT);

					if (queryDepth > 0) {
						readChildren(mapping, result, queryDepth - 1,
							childCounts);
					}
				}

				Log.debugf("QueryResult: %s", result);

				return result;
			} catch (SQLException e) {
				throw new StorageException(e);
			}
		}

		return result;
	}

	/**
	 * @see java.util.Iterator#remove()
	 */
	public void remove() {
		throw new UnsupportedOperationException("Remove not supported");
	}

	/**
	 * @see QueryResult#setPosition(int, boolean)
	 */
	@Override
	public void setPosition(int index, boolean relative) {
		// convert absolute 0-based index to 1-based JDBC offset
		offset = relative ? index : index >= 0 ? index + 1 : index;

		offsetRelative = relative;
	}

	/**
	 * Reads all children of the given parent object from this result's storage
	 * and adds them to the parent object.
	 *
	 * @param parentMapping The storage mapping for the parent object
	 * @param parent        The parent object to read the children of
	 * @param depth         The depth up to which children of the child objects
	 *                      shall be fetched
	 * @param childCounts   An array containing the number of children for each
	 *                      child mapping
	 * @throws StorageException If reading the children fails
	 */
	private <C extends StorageMapping<?, Relatable, ?>> void readChildren(
		final StorageMapping<T, Relatable, C> parentMapping, final T parent,
		int depth, int[] childCounts) throws StorageException {
		Collection<C> childMappings = parentMapping.getChildMappings();
		int childMappingCount = childMappings.size();

		if (childMappingCount > 0) {
			int currentMapping = 0;
			Object parentId = parentMapping.getAttributeValue(parent,
				parentMapping.getIdAttribute());

			for (final C mapping : childMappings) {
				int childCount =
					(childCounts != null ? childCounts[currentMapping++] : -1);

				if (childCount != 0) {
					@SuppressWarnings("unchecked")
					StorageMapping<Object, Relatable, ?> childMapping =
						(StorageMapping<Object, Relatable, ?>) mapping;

					QueryPredicate<Object> childQuery =
						JdbcQuery.createChildQueryPredicate(parentMapping,
							childMapping, parentId, depth);

					ElementInitializer<Object> initializer =
						new ElementInitializer<Object>() {
							@Override
							public void initElements(List<Object> children) {
								parentMapping.initChildren(parent, children,
									mapping);
							}
						};

					List<Object> children =
						new QueryList<Object>(storage.get(STORAGE_DEFINITION),
							childQuery, childCount, initializer);

					parentMapping.setChildren(parent, children, mapping);
				}
			}
		}
	}
}
