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
package de.esoco.storage.impl.jdbc;

import de.esoco.lib.logging.Log;
import de.esoco.lib.manage.Closeable;

import de.esoco.storage.QueryList;
import de.esoco.storage.QueryList.ElementInitializer;
import de.esoco.storage.QueryPredicate;
import de.esoco.storage.QueryResult;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageMapping;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.obrel.core.ObjectRelations;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import static de.esoco.storage.StorageRelationTypes.PERSISTENT;
import static de.esoco.storage.StorageRelationTypes.QUERY_DEPTH;
import static de.esoco.storage.StorageRelationTypes.STORAGE_DEFINITION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DISABLE_CHILD_COUNTS;


/********************************************************************
 * Implementation of the {@link QueryResult} interface that is backed by a JDBC
 * {@link ResultSet}.
 */
class JdbcQueryResult<T> extends RelatedObject implements QueryResult<T>
{
	//~ Instance fields --------------------------------------------------------

	private final JdbcStorage					  rStorage;
	private final StorageMapping<T, Relatable, ?> rMapping;
	private final ResultSet						  aResultSet;
	private final boolean						  bIsChildQuery;

	private int     nOffset		    = 0;
	private boolean bOffsetRelative = false;

	private boolean bHasNext = false;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Creates a new QueryResult from a JDBC prepared statement.
	 *
	 * @param  rStorage        The storage to perform the query on
	 * @param  rStorageMapping The storage mapping
	 * @param  rResultSet      rQueryStatement The JDBC statement to execute
	 * @param  nOffset         The initial offset to position the result at
	 * @param  bIsChildQuery   TRUE if this is a query for child objects
	 *
	 * @throws SQLException
	 */
	JdbcQueryResult(JdbcStorage						rStorage,
					StorageMapping<T, Relatable, ?> rStorageMapping,
					ResultSet						rResultSet,
					int								nOffset,
					boolean							bIsChildQuery)
		throws SQLException
	{
		this.rStorage	   = rStorage;
		this.rMapping	   = rStorageMapping;
		this.aResultSet    = rResultSet;
		this.nOffset	   = nOffset + 1; // +1 for 1-based JDBC indexing
		this.bIsChildQuery = bIsChildQuery;
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * @see Closeable#close()
	 */
	@Override
	public void close()
	{
		try
		{
			aResultSet.close();
		}
		catch (SQLException e)
		{
			Log.error("Closing ResultSet failed", e);
		}
	}

	/***************************************
	 * @see QueryResult#hasNext()
	 */
	@Override
	public boolean hasNext() throws StorageException
	{
		try
		{
			if (nOffset != 0)
			{
				bHasNext = aResultSet.absolute(nOffset);

				if (bOffsetRelative)
				{
					bHasNext = aResultSet.relative(nOffset);
				}
				else
				{
					bHasNext = aResultSet.absolute(nOffset);
				}

				nOffset = 0;
			}
			else
			{
				bHasNext = aResultSet.next();
			}
		}
		catch (SQLException e)
		{
			throw new StorageException(e);
		}

		return bHasNext;
	}

	/***************************************
	 * @see QueryResult#next()
	 */
	@Override
	public T next() throws StorageException
	{
		T aResult = null;

		if (bHasNext)
		{
			try
			{
				int nResultSize = aResultSet.getMetaData().getColumnCount();

				boolean bUseChildCounts =
					!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS);

				int nChildMappings = rMapping.getChildMappings().size();
				int nColumns	   =
					bUseChildCounts ? nResultSize - nChildMappings
									: nResultSize;

				List<Object> aValues	  = new ArrayList<Object>(nColumns);
				int[]		 aChildCounts = null;

				for (int i = 1; i <= nColumns; i++)
				{
					Object rValue = aResultSet.getObject(i);

					aValues.add(rValue);
				}

				if (bUseChildCounts)
				{
					aChildCounts = new int[nChildMappings];

					for (int i = 0; i < nChildMappings; i++)
					{
						aChildCounts[i] = aResultSet.getInt(++nColumns);
					}
				}

				aResult = rMapping.createObject(aValues, bIsChildQuery);

				Relatable rResultRelatable =
					ObjectRelations.getRelatable(aResult);

				// QUERY_DEPTH will be Integer.MAX_VALUE if not set
				int nQueryDepth = get(QUERY_DEPTH).intValue();

				// read children until query depth is zero, but only if
				// parent object is not marked as persistent already
				// which also means that is has been read completely
				// and is returned from a cache
				if (!rResultRelatable.hasFlag(PERSISTENT))
				{
					rResultRelatable.set(PERSISTENT);

					if (nQueryDepth > 0)
					{
						readChildren(rMapping,
									 aResult,
									 nQueryDepth - 1,
									 aChildCounts);
					}
				}

				Log.debugf("QueryResult: %s", aResult);

				return aResult;
			}
			catch (SQLException e)
			{
				throw new StorageException(e);
			}
		}

		return aResult;
	}

	/***************************************
	 * @see java.util.Iterator#remove()
	 */
	public void remove()
	{
		throw new UnsupportedOperationException("Remove not supported");
	}

	/***************************************
	 * @see QueryResult#setPosition(int, boolean)
	 */
	@Override
	public void setPosition(int nIndex, boolean bRelative)
	{
		// convert absolute 0-based index to 1-based JDBC offset
		nOffset = bRelative ? nIndex : nIndex >= 0 ? nIndex + 1 : nIndex;

		bOffsetRelative = bRelative;
	}

	/***************************************
	 * Reads all children of the given parent object from this result's storage
	 * and adds them to the parent object.
	 *
	 * @param  rParentMapping The storage mapping for the parent object
	 * @param  rParent        The parent object to read the children of
	 * @param  nDepth         The depth up to which children of the child
	 *                        objects shall be fetched
	 * @param  rChildCounts   An array containing the number of children for
	 *                        each child mapping
	 *
	 * @throws StorageException If reading the children fails
	 */
	private <C extends StorageMapping<?, Relatable, ?>> void readChildren(
		final StorageMapping<T, Relatable, C> rParentMapping,
		final T								  rParent,
		int									  nDepth,
		int[]								  rChildCounts)
		throws StorageException
	{
		Collection<C> rChildMappings = rParentMapping.getChildMappings();
		int			  nChildMappings = rChildMappings.size();

		if (nChildMappings > 0)
		{
			int    nCurrentMapping = 0;
			Object aParentId	   =
				rParentMapping.getAttributeValue(rParent,
												 rParentMapping
												 .getIdAttribute());

			for (final C rMapping : rChildMappings)
			{
				int nChildCount =
					(rChildCounts != null ? rChildCounts[nCurrentMapping++]
										  : -1);

				if (nChildCount != 0)
				{
					@SuppressWarnings("unchecked")
					StorageMapping<Object, Relatable, ?> rChildMapping =
						(StorageMapping<Object, Relatable, ?>) rMapping;

					QueryPredicate<Object> pChildQuery =
						JdbcQuery.createChildQueryPredicate(rParentMapping,
															rChildMapping,
															aParentId,
															nDepth);

					ElementInitializer<Object> aInitializer =
						new ElementInitializer<Object>()
						{
							@Override
							public void initElements(List<Object> rChildren)
							{
								rParentMapping.initChildren(rParent,
															rChildren,
															rMapping);
							}
						};

					List<Object> rChildren =
						new QueryList<Object>(rStorage.get(STORAGE_DEFINITION),
											  pChildQuery,
											  nChildCount,
											  aInitializer);

					rParentMapping.setChildren(rParent, rChildren, rMapping);
				}
			}
		}
	}
}
