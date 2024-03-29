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
package de.esoco.storage.impl.jdbc;

import de.esoco.storage.AbstractStorageTest;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import org.junit.jupiter.api.BeforeAll;

/**
 * JDBC specific storage test implementation.
 *
 * @author eso
 */
public class JdbcStorageTest extends AbstractStorageTest {

	/**
	 * Initializes the storage for this test.
	 *
	 * @throws StorageException If the storage initialization fails
	 */
	@BeforeAll
	public static void init() throws StorageException {
		try {
			Class.forName("org.h2.Driver");
			StorageManager.setDefaultStorage(JdbcStorageDefinition.create(
				"jdbc:h2:mem:testdb;user=sa;password="));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IllegalStateException(e);
		}
	}
}
