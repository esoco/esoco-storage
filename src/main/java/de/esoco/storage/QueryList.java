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


/********************************************************************
 * A {@link List} implementation that receives it's content from a storage
 * query. The list elements will only be queried when the list is accessed for
 * the first time.
 *
 * @author eso
 */
public class QueryList<T> extends AbstractList<T>
{
	//~ Instance fields --------------------------------------------------------

	private final StorageDefinition rStorageDefinition;
	private final QueryPredicate<T> pQuery;

	private int					  nSize		   = -1;
	private ElementInitializer<T> rInitializer = null;

	private ArrayList<T> aElements = null;
	private boolean		 bQueried  = false;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Creates a new instance. This no-argument constructor exists for
	 * compliance with the collections API specification and will create an
	 * empty list without a query that can be modified freely.
	 */
	public QueryList()
	{
		rStorageDefinition = null;
		pQuery			   = null;
		aElements		   = new ArrayList<T>();
		bQueried		   = true;
	}

	/***************************************
	 * Creates a new instance.
	 *
	 * @param rDefinition  The definition of the storage to query the list
	 *                     elements from
	 * @param pChildQuery  The query to read the list elements from
	 * @param nSize        The size of the list after querying or -1 if unknown
	 * @param rInitializer The initializer for the queried list elements
	 */
	public QueryList(StorageDefinition	   rDefinition,
					 QueryPredicate<T>	   pChildQuery,
					 int				   nSize,
					 ElementInitializer<T> rInitializer)
	{
		this.rStorageDefinition = rDefinition;
		this.pQuery			    = pChildQuery;
		this.nSize			    = nSize;
		this.rInitializer	    = rInitializer;
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * @see AbstractList#add(int, Object)
	 */
	@Override
	public void add(int nIndex, T rElement)
	{
		query();

		aElements.add(nIndex, rElement);
	}

	/***************************************
	 * @see AbstractList#get(int)
	 */
	@Override
	public T get(int nIndex)
	{
		query();

		return aElements.get(nIndex);
	}

	/***************************************
	 * Returns the underlying storage query of this list.
	 *
	 * @return The query
	 */
	public final QueryPredicate<T> getQuery()
	{
		return pQuery;
	}

	/***************************************
	 * Checks whether this list has already been queried.
	 *
	 * @return TRUE if this list has been queried already
	 */
	public final boolean isQueried()
	{
		return bQueried;
	}

	/***************************************
	 * Queries the elements of this list if it hasn't been done already. The
	 * query is normally performed automatically and this method normally
	 * doesn't need to be invoked by application code.
	 *
	 * @throws StorageRuntimeException If performing the query fails
	 */
	public synchronized void query() throws StorageRuntimeException
	{
		if (!bQueried)
		{
			Storage rStorage = null;

			try
			{
				rStorage = StorageManager.getStorage(rStorageDefinition);

				Query<T> rQuery = rStorage.query(pQuery);

				try
				{
					QueryResult<T> aResult		  = rQuery.execute();
					ArrayList<T>   aResultObjects = new ArrayList<T>();

					while (aResult.hasNext())
					{
						aResultObjects.add(aResult.next());
					}

					if (rInitializer != null)
					{
						rInitializer.initElements(aResultObjects);
					}

					aElements = aResultObjects;
					nSize     = aElements.size();
					bQueried  = true;
				}
				finally
				{
					rQuery.close();
				}
			}
			catch (StorageException e)
			{
				throw new StorageRuntimeException(e);
			}
			finally
			{
				if (rStorage != null)
				{
					rStorage.release();
				}
			}
		}
	}

	/***************************************
	 * @see AbstractList#remove(int)
	 */
	@Override
	public T remove(int nIndex)
	{
		query();

		return aElements.remove(nIndex);
	}

	/***************************************
	 * @see AbstractList#set(int, Object)
	 */
	@Override
	public T set(int nIndex, T rElement)
	{
		query();

		return aElements.set(nIndex, rElement);
	}

	/***************************************
	 * @see AbstractList#size()
	 */
	@Override
	public int size()
	{
		if (nSize < 0)
		{
			query();
		}

		return aElements != null ? aElements.size() : nSize;
	}

	//~ Inner Interfaces -------------------------------------------------------

	/********************************************************************
	 * A simple interface that provides a method to initialize the list elements
	 * after they have been queried.
	 *
	 * @author eso
	 */
	public static interface ElementInitializer<T>
	{
		//~ Methods ------------------------------------------------------------

		/***************************************
		 * Will be invoked immediately after the list elements have been
		 * queried.
		 *
		 * @param rElements The list element
		 */
		public void initElements(List<T> rElements);
	}
}
