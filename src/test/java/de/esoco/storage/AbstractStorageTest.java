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
package de.esoco.storage;

import de.esoco.lib.expression.Function;
import de.esoco.lib.expression.Functions;
import de.esoco.lib.expression.Predicate;
import de.esoco.lib.expression.StringFunctions;
import de.esoco.lib.expression.predicate.FunctionPredicate;
import de.esoco.storage.mapping.ClassMapping;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Date;
import java.util.Set;

import static de.esoco.lib.expression.Predicates.equalTo;
import static de.esoco.lib.expression.Predicates.greaterOrEqual;
import static de.esoco.lib.expression.Predicates.lessThan;
import static de.esoco.storage.StoragePredicates.forType;
import static de.esoco.storage.StoragePredicates.hasChild;
import static de.esoco.storage.StoragePredicates.ifField;
import static de.esoco.storage.StoragePredicates.like;
import static de.esoco.storage.StoragePredicates.similarTo;
import static de.esoco.storage.StoragePredicates.sortBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Abstract base class with tests of storage functions that are independent of
 * storage implementations. A subclass that tests a specific storage
 * implementations only needs to implement a method with the {@link BeforeAll}
 * annotation of JUnit that sets the storage definition for the implementation
 * with {@link StorageManager#setDefaultStorage(StorageDefinition)}.
 *
 * @author eso
 */
public abstract class AbstractStorageTest {

	static final String TEST_URL = "http://www.example.com/";

	private Storage storage = null;

	/**
	 * Test cleanup that performs a rollback and then releases the test
	 * storage.
	 *
	 * @throws StorageException If the rollback fails
	 */
	@AfterEach
	public void finish() throws StorageException {
		storage.rollback();
		storage.release();
	}

	/**
	 * Acquires and (if necessary) initializes the storage.
	 *
	 * @throws Exception On errors
	 */
	@BeforeEach
	public void initStorage() throws Exception {
		storage = StorageManager.getStorage(TestRecord.class);

		storage.initObjectStorage(TestRecord.class);

		Query<TestRecord> query =
			storage.query(forType(TestRecord.class, null));

		if (!query.execute().hasNext()) {
			storeTestRecords("jones", 1, 1);
			storeTestRecords("smith", 2, 2);
			storage.commit();
		}

		query.close();
	}

	/**
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testAutoDatatypeMapping() throws StorageException {
		QueryResult<TestRecord> qr = storage
			.query(forType(TestRecord.class, ifField("name",
				equalTo("jones"))))
			.execute();

		assertTrue(qr.hasNext());

		TestRecord tr = qr.next();

		URL u = tr.getUrl();

		assertTrue(u.toString().startsWith(TEST_URL));
	}

	/**
	 * Test method for the enabled {@link Storage#delete(Object)} method.
	 */
	@Test
	public void testDeleteEnabled() throws StorageException {
		performDelete();
	}

	/**
	 * Test of the {@link Storage#PROPERTY_DELETE_DISABLED} property.
	 */
	@Test
	public void testDeleteGloballyDisabled() throws StorageException {
		System.setProperty(Storage.PROPERTY_DELETE_DISABLED, "true");

		try {
			performDelete();
			fail("Should throw an StorageException");
		} catch (StorageException e) {
			// this is expected
		}

		System.setProperty(Storage.PROPERTY_DELETE_DISABLED, "false");
	}

	/**
	 * Test of {@link Storage#hasObjectStorage(Class)}
	 */
	@Test
	public void testObjectStorage() throws StorageException {
		assertTrue(storage.hasObjectStorage(TestRecord.class));

		storage.removeObjectStorage(TestDetail.class);
		storage.removeObjectStorage(TestRecord.class);
		assertFalse(storage.hasObjectStorage(TestRecord.class));
		storage.initObjectStorage(TestRecord.class);
		assertTrue(storage.hasObjectStorage(TestRecord.class));
	}

	/**
	 * Test absolute result positioning with query offset and limit.
	 */
	@Test
	public void testPaging() throws StorageException {
//		Query<TestRecord> q =
//			storage.query(forType(TestRecord.class,
//								   sortBy("name").and(sortBy("value"))));
//
//		assertEquals(3, q.size());
//		q.set(StorageRelationTypes.QUERY_OFFSET, 1);
//		q.set(StorageRelationTypes.QUERY_LIMIT, 1);
//
//		QueryResult<TestRecord> result = q.execute();
//
//		assertTrue(result.hasNext());
//
//		TestRecord r = result.next();
//
//		assertEquals("smith", r.getName());
//		assertEquals(1, r.getValue());
//		assertFalse(result.hasNext());
//
//		q.close();
	}

	/**
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQuery() throws StorageException {
		Predicate<Object> withNameJones = ifField("name", equalTo("jones"));
		Predicate<Object> withNameSmith = ifField("name", equalTo("smith"));

		Query<TestRecord> jones =
			storage.query(forType(TestRecord.class, withNameJones));

		assertEquals(1, getResultSize(jones.execute()));

		Query<TestRecord> smith =
			storage.query(forType(TestRecord.class, withNameSmith));

		assertEquals(2, getResultSize(smith.execute()));

		jones.close();
		smith.close();

		Predicate<Object> isSmithOrJones = withNameSmith.or(withNameJones);
		Predicate<Object> isSmithAndJones = withNameSmith.and(withNameJones);

		Query<TestRecord> querySmithOrJones =
			storage.query(forType(TestRecord.class, isSmithOrJones));
		Query<TestRecord> querySmithAndJones =
			storage.query(forType(TestRecord.class, isSmithAndJones));

		assertEquals(3, getResultSize(querySmithOrJones.execute()));
		assertEquals(0, getResultSize(querySmithAndJones.execute()));

		querySmithOrJones.close();
		querySmithAndJones.close();
	}

	/**
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryAlmostLike() throws StorageException {
		Predicate<Object> withNameLikeJones =
			ifField("name", similarTo("jones"));

		Query<TestRecord> jones =
			storage.query(forType(TestRecord.class, withNameLikeJones));

		assertEquals(1, getResultSize(jones.execute()));

		jones.close();
	}

	/**
	 * Test detail queries.
	 */
	@Test
	public void testQueryDetail() throws StorageException {
		Query<TestRecord> byDetail = storage.query(forType(TestRecord.class,
			ifField("details", hasChild(TestDetail.class,
				ifField("name", equalTo("smith-1"))))));

		assertEquals(2, getResultSize(byDetail.execute()));
		byDetail.close();

		byDetail = storage.query(forType(TestRecord.class, ifField("details",
			hasChild(TestDetail.class,
				ifField("name", greaterOrEqual("smith-2")).and(
					ifField("name", lessThan("smith-3")))))));

		assertEquals(2, getResultSize(byDetail.execute()));
		byDetail.close();
	}

	/**
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryFunction() throws StorageException {
		Predicate<Object> nameIsJones = equalTo("jones");

		Function<Object, String> lowerCaseName =
			StringFunctions.toLowerCase().from(Functions.readField("name"));

		FunctionPredicate<Object, String> functionPredicate =
			new FunctionPredicate<Object, String>(lowerCaseName, nameIsJones);

		Query<TestRecord> lowerJones =
			storage.query(forType(TestRecord.class, functionPredicate));

		assertEquals(1, getResultSize(lowerJones.execute()));

		lowerJones.close();
	}

	/**
	 * Test method for {@link Query#size()}.
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryGetDistinct() throws StorageException {
		@SuppressWarnings("unchecked")
		ClassMapping<TestRecord> mapping =
			(ClassMapping<TestRecord>) StorageManager.getMapping(
				TestRecord.class);

		Query<TestRecord> q = storage.query(forType(TestRecord.class, null));

		Set<Object> names = q.getDistinct(mapping.getFieldDescriptor("name"));

		assertEquals(2, names.size());
		assertTrue(names.contains("jones"));
		assertTrue(names.contains("smith"));

		q.close();
	}

	/**
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryLike() throws StorageException {
		Predicate<Object> withNameLikeJones = ifField("name", like("%ones"));

		Query<TestRecord> jones =
			storage.query(forType(TestRecord.class, withNameLikeJones));

		assertEquals(1, getResultSize(jones.execute()));

		jones.close();
	}

	/**
	 * Test random query result access through
	 * {@link QueryResult#setPosition(int, boolean)}.
	 */
	@Test
	public void testQueryPositionOf() throws StorageException {
		Query<TestRecord> q =
			storage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		// row_number() function currently not working in H2 database
//		assertEquals(0, q.positionOf(1));
//		assertEquals(1, q.positionOf(2));
	}

	/**
	 * Test absolute result positioning with
	 * {@link QueryResult#setPosition(int, boolean)}.
	 */
	@Test
	public void testQueryResultAbsolutePosition() throws StorageException {
		Query<TestRecord> q =
			storage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		QueryResult<TestRecord> result = q.execute();

		// first iterate to end
		while (result.hasNext()) {
			result.next();
		}

		result.setPosition(0, false);
		assertTrue(result.hasNext());
		assertEquals("jones", result.next().getName());

		result.setPosition(2, false);
		assertTrue(result.hasNext());
		assertEquals("smith", result.next().getName());

		q.close();
	}

	/**
	 * Test relative result positioning with
	 * {@link QueryResult#setPosition(int, boolean)}.
	 */
	@Test
	public void testQueryResultRelativePosition() throws StorageException {
		Query<TestRecord> q =
			storage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		QueryResult<TestRecord> result = q.execute();

		// first iterate to end
		while (result.hasNext()) {
			result.next();
		}

		result.setPosition(-3, true);
		assertTrue(result.hasNext());
		assertEquals("jones", result.next().getName());

		result.setPosition(2, true);
		assertTrue(result.hasNext());
		assertEquals("smith", result.next().getName());

		q.close();
	}

	/**
	 * Test method for {@link Query#size()}.
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQuerySize() throws StorageException {
		Query<TestRecord> q = storage.query(
			forType(TestRecord.class, ifField("name", equalTo("jones"))));

		assertEquals(1, q.size());
		q.close();

		q = storage.query(
			forType(TestRecord.class, ifField("name", equalTo("smith"))));
		assertEquals(2, q.size());
		q.close();

		q = storage.query(
			forType(TestRecord.class, ifField("name", equalTo("nothing"))));
		assertEquals(0, q.size());
		q.close();
	}

	/**
	 * Checks that a repeated call to {@link Storage#initObjectStorage(Class)}
	 * won't fail.
	 */
	@Test
	public void testRepeatInitObjectStorage() throws StorageException {
		// TestRecord has been create during init already
		storage.initObjectStorage(TestRecord.class);
	}

	/**
	 * Test the ascending sorting of queries.
	 */
	@Test
	public void testSortAscending() throws StorageException {
		sortedQueryTest(true);
	}

	/**
	 * Test the descending sorting of queries.
	 */
	@Test
	public void testSortDescending() throws StorageException {
		sortedQueryTest(false);
	}

	/**
	 * Returns the size of a query result. The query result argument will be
	 * closed before returning.
	 *
	 * @param result The query result to evaluate
	 * @return The number of objects returned by the query result
	 * @throws StorageException If processing the query result fails
	 */
	int getResultSize(QueryResult<?> result) throws StorageException {
		int size = 0;

		while (result.hasNext()) {
			result.next();
			size++;
		}

		result.close();

		return size;
	}

	/**
	 * Test method for {@link Storage#delete(Object)}.
	 */
	void performDelete() throws StorageException {
		Predicate<Object> withNameJones = ifField("name", equalTo("jones"));

		Query<TestRecord> queryJones =
			storage.query(forType(TestRecord.class, withNameJones));

		QueryResult<TestRecord> jonesResult = queryJones.execute();

		assertTrue(jonesResult.hasNext());

		TestRecord jones = jonesResult.next();

		for (TestDetail jonesDetail : jones.getDetails()) {
			storage.delete(jonesDetail);
		}

		storage.delete(jones);
		assertFalse(queryJones.execute().hasNext());
	}

	/**
	 * Internal method to perform and test a sorted query.
	 *
	 * @param ascending TRUE for ascending, false for descending ordering
	 * @throws StorageException If the query fails
	 */
	void sortedQueryTest(boolean ascending) throws StorageException {
		Query<TestRecord> smithSorted = storage.query(forType(TestRecord.class,
			ifField("name", equalTo("smith")).and(sortBy("value", ascending))));

		QueryResult<TestRecord> result = smithSorted.execute();
		int value = ascending ? 1 : 2;

		while (result.hasNext()) {
			TestRecord next = result.next();

			assertTrue(value >= 1 && value <= 2);
			assertEquals(next.getValue(), value);
			value += ascending ? 1 : -1;
		}

		result.close();
		smithSorted.execute();
	}

	/**
	 * Stores a set of test records.
	 *
	 * @param name    The record name
	 * @param idStart The ID of the first record
	 * @param count   The number of records to store
	 * @throws Exception On errors
	 */
	void storeTestRecords(String name, int idStart, int count)
		throws Exception {
		for (int i = 1; i <= count; i++) {
			TestRecord record =
				new TestRecord(idStart + i - 1, name, i, new Date(),
					new URL(TEST_URL + i));

			for (int j = 1; j <= 5; j++) {
				record.addDetail(new TestDetail(name + "-" + j, i * 10 + j));
			}

			storage.store(record);
		}
	}
}
