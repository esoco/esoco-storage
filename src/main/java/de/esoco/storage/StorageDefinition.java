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

import de.esoco.lib.manage.ElementDefinition;

import org.obrel.core.SerializableRelatedObject;

/**
 * An abstract base class that defines a storage that can be created by the
 * class {@link StorageManager}. Subclasses must implement the abstract method
 * {@link #createStorage()} to create a new storage instance that corresponds to
 * the definition. All subclasses must implement correct serialization to allow
 * that storage definition instances can be transferred over the net or stored
 * in application server sessions.
 *
 * <p>Subclasses must also provide implementations of {@link #equals(Object)}
 * and {@link #hashCode()} that are not based on object identity but on the
 * definition parameters. Therefore these methods have be overloaded as abstract
 * methods.</p>
 *
 * @author eso
 */
public abstract class StorageDefinition extends SerializableRelatedObject
	implements ElementDefinition<Storage> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new StorageDefinition instance.
	 */
	public StorageDefinition() {
	}

	/**
	 * Must be implemented by subclasses together with {@link #hashCode()} to
	 * perform an equality test that is not based on object identity.
	 *
	 * @see Object#equals(Object)
	 */
	@Override
	public abstract boolean equals(Object rObj);

	/**
	 * Must be implemented by subclasses together with {@link #equals(Object)}
	 * to calculate a hash code that is not based on object identity.
	 *
	 * @see Object#equals(Object)
	 */
	@Override
	public abstract int hashCode();

	/**
	 * Must be implemented by subclasses to create a new storage instance.
	 *
	 * @return The new storage instance
	 * @throws StorageException If creating the storage fails
	 */
	protected abstract Storage createStorage() throws StorageException;
}
