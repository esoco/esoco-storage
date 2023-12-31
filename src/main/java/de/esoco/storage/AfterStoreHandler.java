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

/**
 * An interface that can be implemented by objects that perform some operations
 * after they have been stored by a {@link Storage} implementation.
 *
 * @author eso
 */
public interface AfterStoreHandler {

	/**
	 * Will be invoked after the object has been stored in a {@link Storage}.
	 *
	 * @throws Exception Any exception may be thrown to signal a failure
	 */
	public void afterStore() throws Exception;
}
