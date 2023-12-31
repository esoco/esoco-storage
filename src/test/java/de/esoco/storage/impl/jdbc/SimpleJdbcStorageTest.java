//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2016 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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

import de.esoco.storage.Query;
import de.esoco.storage.QueryResult;
import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.TestDetail;
import de.esoco.storage.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.SQLException;
import java.util.Date;

import static de.esoco.lib.expression.Predicates.equalTo;
import static de.esoco.lib.expression.Predicates.ifField;
import static de.esoco.storage.StoragePredicates.forType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple test of JDBC storage functionality.
 *
 * @author eso
 */
class SimpleJdbcStorageTest {

	private Storage storage;

	/**
	 * Initializes the storage for the tests.
	 */
	@BeforeEach
	void setUp() throws ClassNotFoundException, StorageException {
		Class.forName("org.h2.Driver");
//		Class.forName("org.postgresql.Driver");

		StorageManager.registerStorage(JdbcStorageDefinition.create(
			"jdbc:h2:mem:testdb;user=sa;password="), TestRecord.class);

		storage = StorageManager.getStorage(TestRecord.class);

		storage.initObjectStorage(TestRecord.class);
	}

	/**
	 * Performs a rollback and closes the storage.
	 *
	 * @throws StorageException On errors
	 */
	@AfterEach
	void tearDown() throws StorageException, SQLException {
		storage.rollback();
		storage.release();
	}

	/**
	 * Store test.
	 */
	@Test
	void testStore() throws Exception {
		TestRecord testRecord = new TestRecord(1, "Test1", 1, new Date(),
			new URL("http://example.com"));
		Query<TestRecord> query = storage.query(
			forType(TestRecord.class, ifField("name", equalTo("Test1"))));

		storage.store(testRecord);

		QueryResult<TestRecord> result = query.execute();

		assertTrue(result.hasNext());

		testRecord = result.next();
		testRecord.addDetail(new TestDetail("TD 1.1", 11));
		testRecord.addDetail(new TestDetail("TD 1.2", 12));
		storage.store(testRecord);

		result = query.execute();

		assertTrue(result.hasNext());

		testRecord = result.next();
		assertEquals(2, testRecord.getDetails().size());
	}
}
