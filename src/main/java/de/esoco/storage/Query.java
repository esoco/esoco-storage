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

import de.esoco.lib.manage.Closeable;

import java.util.Set;

import org.obrel.core.Relatable;


/********************************************************************
 * A query object that represents a query of storage objects. Closing a query
 * will also close any currently active {@link QueryResult} of that query.
 * Therefore it is not necessary to close the result separately. Only if a
 * result is no longer needed but the query is kept for later re-execution the
 * result should be closed explicitly.
 *
 * @author eso
 */
public interface Query<T> extends Closeable, Relatable
{
	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Executes this query and returns a QueryResult for the objects that match
	 * the query criteria. A query may be executed multiple times.
	 *
	 * @return An iterator that will return the objects matching the query
	 *
	 * @throws StorageException If executing the query fails
	 */
	public QueryResult<T> execute() throws StorageException;

	/***************************************
	 * Returns a set containing the distinct values of a certain object
	 * attribute.
	 *
	 * @param  rAttribute The attribute to get the distinct values of
	 *
	 * @return A set containing the distinct attribute values
	 *
	 * @throws StorageException If querying the values fails
	 */
	public Set<Object> getDistinct(Relatable rAttribute)
		throws StorageException;

	/***************************************
	 * Returns the query predicate that this query is based on.
	 *
	 * @return The query predicate
	 */
	public QueryPredicate<T> getQueryPredicate();

	/***************************************
	 * Returns the storage this query has been created for.
	 *
	 * @return The storage
	 */
	public Storage getStorage();

	/***************************************
	 * Determines the zero-based position of an object with a certain ID in the
	 * result of this query. If the ID doesn't exist in this query -1 will be
	 * returned.
	 *
	 * <p>This is an optional operation which may not be supported by some
	 * implementations or underlying storages in which case -1 will be
	 * returned.</p>
	 *
	 * @param  rId The ID of the object to determine the position of
	 *
	 * @return The zero-based index of the given ID in the query or -1 if the ID
	 *         could not be found in the query result or if this operation is
	 *         not supported by the implementation
	 *
	 * @throws StorageException If querying the position fails
	 */
	public int positionOf(Object rId) throws StorageException;

	/***************************************
	 * Returns the size of this query, i.e. the number of objects that an
	 * execution will yield. Depending on the underlying implementation the
	 * result may not be exact and therefore differ from the actual number of
	 * objects returned by iterating through a query result (e.g. if changes are
	 * made to a database concurrently). Therefore application should use the
	 * returned value only as an estimation and base all final calculations on
	 * the iteration.
	 *
	 * <p>This is an optional operation which may not be supported by some
	 * implementations or underlying storages in which case -1 will be returned.
	 * </p>
	 *
	 * @return The estimated query size or -1 if it cannot be determined
	 *
	 * @throws StorageException If the size calculation fails
	 */
	public int size() throws StorageException;
}
