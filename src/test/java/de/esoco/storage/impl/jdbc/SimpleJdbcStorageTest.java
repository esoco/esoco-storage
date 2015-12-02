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

import de.esoco.storage.Query;
import de.esoco.storage.QueryResult;
import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.TestDetail;
import de.esoco.storage.TestRecord;

import java.sql.SQLException;

import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static de.esoco.lib.expression.Predicates.equalTo;
import static de.esoco.lib.expression.Predicates.ifField;

import static de.esoco.storage.StoragePredicates.forType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/********************************************************************
 * Simple test of JDBC storage functionality.
 *
 * @author eso
 */
public class SimpleJdbcStorageTest
{
	//~ Instance fields --------------------------------------------------------

	private Storage rStorage;

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Initializes the storage for the tests.
	 *
	 * @throws ClassNotFoundException
	 * @throws StorageException
	 */
	@Before
	public void setUp() throws ClassNotFoundException, StorageException
	{
		Class.forName("org.h2.Driver");
//		Class.forName("org.postgresql.Driver");

		StorageManager.registerStorage(JdbcStorageDefinition.create("jdbc:h2:mem:testdb;user=sa;password="),
									   TestRecord.class);

		rStorage = StorageManager.getStorage(TestRecord.class);

		rStorage.initObjectStorage(TestRecord.class);
	}

	/***************************************
	 * Performs a rollback and closes the storage.
	 *
	 * @throws StorageException On errors
	 * @throws SQLException
	 */
	@After
	public void tearDown() throws StorageException, SQLException
	{
		rStorage.rollback();
		rStorage.release();
	}

	/***************************************
	 * Store test.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testStore() throws StorageException
	{
		TestRecord		  aTestRecord =
			new TestRecord(1, "Test1", 1, new Date());
		Query<TestRecord> aQuery	  =
			rStorage.query(forType(TestRecord.class,
								   ifField("name", equalTo("Test1"))));

		rStorage.store(aTestRecord);

		QueryResult<TestRecord> aResult = aQuery.execute();

		assertTrue(aResult.hasNext());

		aTestRecord = aResult.next();
		aTestRecord.addDetail(new TestDetail("TD 1.1", 11));
		aTestRecord.addDetail(new TestDetail("TD 1.2", 12));
		rStorage.store(aTestRecord);

		aResult = aQuery.execute();

		assertTrue(aResult.hasNext());

		aTestRecord = aResult.next();
		assertEquals(2, aTestRecord.getDetails().size());
	}
}
