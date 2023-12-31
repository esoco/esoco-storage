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

import de.esoco.lib.manage.Closeable;

import org.obrel.core.Relatable;

/**
 * An interface that defines the methods for the iteration over the result of a
 * storage query. A result set must be closed after it has been used. If the end
 * of the result set is reached (i.e. FALSE is returned by {@link #hasNext()} a
 * query result instance will be closed automatically. Only if the reading from
 * the result is stopped before the end is reached the close() method must be
 * invoked explicitly. A query result is also closed automatically if the query
 * by which it had been created is closed.
 *
 * @author eso
 */
public interface QueryResult<T> extends Closeable, Relatable {

	/**
	 * Checks whether more objects are available in this query result. This
	 * method must be invoked before each call to the {@link #next()} method,
	 * else the next result will be undetermined.
	 *
	 * @return TRUE if this result contains more objects
	 * @throws StorageException If determining the result state fails
	 */
	public boolean hasNext() throws StorageException;

	/**
	 * Returns the next object from this query result. A call to this method is
	 * only possible if the {@link #hasNext()} method has been invoked before.
	 *
	 * @return The next object in this result
	 * @throws StorageException If retrieving the object data fails
	 */
	public T next() throws StorageException;

	/**
	 * Sets the current position of this result. If the new position is valid
	 * (i.e. this method returns TRUE) the next call to {@link #next()} will
	 * return the object at the given position. The index of the first
	 * object is
	 * 0. Negative index values will position the result from the end where -1
	 * indicates the last object. A relative position of 0 corresponds to the
	 * current position, i.e. no change. The result will typically only be
	 * positioned on the designated record when the method
	 * {@link #hasNext()} is
	 * invoked before querying the object at the given position with
	 * {@link #next()}.
	 *
	 * <p>This is an optional operation which may not be supported by some
	 * implementations in which case an {@link UnsupportedOperationException}
	 * will be thrown. Some implementations may only support positive index
	 * values.</p>
	 *
	 * @param index    The new position of this result set
	 * @param relative TRUE to set the position relative to the current
	 *                 position, FALSE to set the absolute position
	 * @throws UnsupportedOperationException If positioning is not supported by
	 *                                       the result implementation
	 */
	public void setPosition(int index, boolean relative);
}
