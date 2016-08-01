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
package de.esoco.storage;

import de.esoco.lib.expression.Function;
import de.esoco.lib.expression.Functions;
import de.esoco.lib.expression.Predicate;
import de.esoco.lib.expression.StringFunctions;
import de.esoco.lib.expression.predicate.FunctionPredicate;

import de.esoco.storage.mapping.ClassMapping;

import java.util.Date;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static de.esoco.lib.expression.Predicates.equalTo;
import static de.esoco.lib.expression.Predicates.greaterOrEqual;
import static de.esoco.lib.expression.Predicates.lessThan;

import static de.esoco.storage.StoragePredicates.almostLike;
import static de.esoco.storage.StoragePredicates.forType;
import static de.esoco.storage.StoragePredicates.hasChild;
import static de.esoco.storage.StoragePredicates.ifField;
import static de.esoco.storage.StoragePredicates.like;
import static de.esoco.storage.StoragePredicates.sortBy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/********************************************************************
 * Abstract base class with tests of storage functions that are independent of
 * storage implementations. A subclass that tests a specific storage
 * implementations only needs to implement a method with the {@link BeforeClass}
 * annotation of JUnit that sets the storage definition for the implementation
 * with {@link StorageManager#setDefaultStorage(StorageDefinition)}.
 *
 * @author eso
 */
public abstract class AbstractStorageTest
{
	//~ Instance fields --------------------------------------------------------

	private Storage rStorage = null;

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Test cleanup that performs a rollback and then releases the test storage.
	 *
	 * @throws StorageException If the rollback fails
	 */
	@After
	public void finish() throws StorageException
	{
		rStorage.rollback();
		rStorage.release();
	}

	/***************************************
	 * Acquires and (if necessary) initializes the storage.
	 *
	 * @throws StorageException On errors
	 */
	@Before
	public void initStorage() throws StorageException
	{
		rStorage = StorageManager.getStorage(TestRecord.class);

		rStorage.initObjectStorage(TestRecord.class);

		Query<TestRecord> rQuery =
			rStorage.query(forType(TestRecord.class, null));

		if (!rQuery.execute().hasNext())
		{
			storeTestRecords("jones", 1, 1);
			storeTestRecords("smith", 2, 2);
			rStorage.commit();
		}

		rQuery.close();
	}

	/***************************************
	 * Test method for the enabled {@link Storage#delete(Object)} method.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testDeleteEnabled() throws StorageException
	{
		performDelete();
	}

	/***************************************
	 * Test of the {@link Storage#PROPERTY_DELETE_DISABLED} property.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testDeleteGloballyDisabled() throws StorageException
	{
		System.setProperty(Storage.PROPERTY_DELETE_DISABLED, "true");

		try
		{
			performDelete();
			assertTrue("Should throw an StorageException", false);
		}
		catch (StorageException e)
		{
			// this is expected
		}

		System.setProperty(Storage.PROPERTY_DELETE_DISABLED, "false");
	}

	/***************************************
	 * Test of {@link Storage#hasObjectStorage(Class)}
	 *
	 * @throws StorageException
	 */
	@Test
	public void testObjectStorage() throws StorageException
	{
		assertTrue(rStorage.hasObjectStorage(TestRecord.class));

		rStorage.removeObjectStorage(TestRecord.class);
		assertFalse(rStorage.hasObjectStorage(TestRecord.class));
		rStorage.initObjectStorage(TestRecord.class);
		assertTrue(rStorage.hasObjectStorage(TestRecord.class));
	}

	/***************************************
	 * Test absolute result positioning with {@link QueryResult#setPosition(int,
	 * boolean)}.
	 *
	 * @throws StorageException
	 */
	@SuppressWarnings("boxing")
	@Test
	public void testPaging() throws StorageException
	{
		Query<TestRecord> q =
			rStorage.query(forType(TestRecord.class,
								   sortBy("name").and(sortBy("value"))));

		assertEquals(3, q.size());
		q.set(StorageRelationTypes.QUERY_OFFSET, 1);
		q.set(StorageRelationTypes.QUERY_LIMIT, 1);

		QueryResult<TestRecord> aResult = q.execute();

		assertTrue(aResult.hasNext());

		TestRecord r = aResult.next();

		assertEquals("smith", r.getName());
		assertEquals(1, r.getValue());
		assertFalse(aResult.hasNext());

		q.close();
	}

	/***************************************
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQuery() throws StorageException
	{
		Predicate<Object> pWithNameJones = ifField("name", equalTo("jones"));
		Predicate<Object> pWithNameSmith = ifField("name", equalTo("smith"));

		Query<TestRecord> qJones =
			rStorage.query(forType(TestRecord.class, pWithNameJones));

		assertEquals(1, getResultSize(qJones.execute()));

		Query<TestRecord> qSmith =
			rStorage.query(forType(TestRecord.class, pWithNameSmith));

		assertEquals(2, getResultSize(qSmith.execute()));

		qJones.close();
		qSmith.close();

		Predicate<Object> pSmithOrJones  = pWithNameSmith.or(pWithNameJones);
		Predicate<Object> pSmithAndJones = pWithNameSmith.and(pWithNameJones);

		Query<TestRecord> qSmithOrJones  =
			rStorage.query(forType(TestRecord.class, pSmithOrJones));
		Query<TestRecord> qSmithAndJones =
			rStorage.query(forType(TestRecord.class, pSmithAndJones));

		assertEquals(3, getResultSize(qSmithOrJones.execute()));
		assertEquals(0, getResultSize(qSmithAndJones.execute()));

		qSmithOrJones.close();
		qSmithAndJones.close();
	}

	/***************************************
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryAlmostLike() throws StorageException
	{
		Predicate<Object> pWithNameLikeJones =
			ifField("name", almostLike("jones"));

		Query<TestRecord> qJones =
			rStorage.query(forType(TestRecord.class, pWithNameLikeJones));

		assertEquals(1, getResultSize(qJones.execute()));

		qJones.close();
	}

	/***************************************
	 * Test detail queries.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testQueryDetail() throws StorageException
	{
		Query<TestRecord> qByDetail =
			rStorage.query(forType(TestRecord.class,
								   ifField("details",
										   hasChild(TestDetail.class,
													ifField("name",
															equalTo("smith-1"))))));

		assertEquals(2, getResultSize(qByDetail.execute()));
		qByDetail.close();

		qByDetail =
			rStorage.query(forType(TestRecord.class,
								   ifField("details",
										   hasChild(TestDetail.class,
													ifField("name",
															greaterOrEqual("smith-2"))
													.and(ifField("name",
																 lessThan("smith-3")))))));

		assertEquals(2, getResultSize(qByDetail.execute()));
		qByDetail.close();
	}

	/***************************************
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryFunction() throws StorageException
	{
		Predicate<Object> pNameIsJones = equalTo("jones");

		Function<Object, String> fLowerCaseName =
			StringFunctions.toLowerCase()
						   .from(Functions.<Object, String>readField("name"));

		FunctionPredicate<Object, String> pFunctionPredicate =
			new FunctionPredicate<Object, String>(fLowerCaseName, pNameIsJones);

		Query<TestRecord> qLowerJones =
			rStorage.query(forType(TestRecord.class, pFunctionPredicate));

		assertEquals(1, getResultSize(qLowerJones.execute()));

		qLowerJones.close();
	}

	/***************************************
	 * Test method for {@link Query#size()}.
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryGetDistinct() throws StorageException
	{
		@SuppressWarnings("unchecked")
		ClassMapping<TestRecord> rMapping =
			(ClassMapping<TestRecord>) StorageManager.getMapping(TestRecord.class);

		Query<TestRecord> q = rStorage.query(forType(TestRecord.class, null));

		Set<Object> names = q.getDistinct(rMapping.getFieldDescriptor("name"));

		assertEquals(2, names.size());
		assertTrue(names.contains("jones"));
		assertTrue(names.contains("smith"));

		q.close();
	}

	/***************************************
	 * Test method for {@link Storage#query(QueryPredicate)}
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQueryLike() throws StorageException
	{
		Predicate<Object> pWithNameLikeJones = ifField("name", like("%ones"));

		Query<TestRecord> qJones =
			rStorage.query(forType(TestRecord.class, pWithNameLikeJones));

		assertEquals(1, getResultSize(qJones.execute()));

		qJones.close();
	}

	/***************************************
	 * Test random query result access through {@link
	 * QueryResult#setPosition(int)}.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testQueryPositionOf() throws StorageException
	{
		Query<TestRecord> q =
			rStorage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		// row_number() function currently not working in H2 database
//		assertEquals(0, q.positionOf(1));
//		assertEquals(1, q.positionOf(2));
	}

	/***************************************
	 * Test absolute result positioning with {@link QueryResult#setPosition(int,
	 * boolean)}.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testQueryResultAbsolutePosition() throws StorageException
	{
		Query<TestRecord> q =
			rStorage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		QueryResult<TestRecord> aResult = q.execute();

		// first iterate to end
		while (aResult.hasNext())
		{
			aResult.next();
		}

		aResult.setPosition(0, false);
		assertEquals("jones", aResult.next().getName());

		aResult.setPosition(2, false);
		assertEquals("smith", aResult.next().getName());

		q.close();
	}

	/***************************************
	 * Test relative result positioning with {@link QueryResult#setPosition(int,
	 * boolean)}.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testQueryResultRelativePosition() throws StorageException
	{
		Query<TestRecord> q =
			rStorage.query(forType(TestRecord.class, sortBy("name", true)));

		assertEquals(3, q.size());

		QueryResult<TestRecord> aResult = q.execute();

		// first iterate to end
		while (aResult.hasNext())
		{
			aResult.next();
		}

		aResult.setPosition(-3, true);
		assertEquals("jones", aResult.next().getName());

		aResult.setPosition(2, true);
		assertEquals("smith", aResult.next().getName());

		q.close();
	}

	/***************************************
	 * Test method for {@link Query#size()}.
	 *
	 * @throws StorageException On errors
	 */
	@Test
	public void testQuerySize() throws StorageException
	{
		Query<TestRecord> q =
			rStorage.query(forType(TestRecord.class,
								   ifField("name", equalTo("jones"))));

		assertEquals(1, q.size());
		q.close();

		q = rStorage.query(forType(TestRecord.class,
								   ifField("name", equalTo("smith"))));
		assertEquals(2, q.size());
		q.close();

		q = rStorage.query(forType(TestRecord.class,
								   ifField("name", equalTo("nothing"))));
		assertEquals(0, q.size());
		q.close();
	}

	/***************************************
	 * Checks that a repeated call to {@link Storage#initObjectStorage(Class)}
	 * won't fail.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testRepeatInitObjectStorage() throws StorageException
	{
		// TestRecord has been create during init already
		rStorage.initObjectStorage(TestRecord.class);
	}

	/***************************************
	 * Test the ascending sorting of queries.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testSortAscending() throws StorageException
	{
		sortedQueryTest(true);
	}

	/***************************************
	 * Test the descending sorting of queries.
	 *
	 * @throws StorageException
	 */
	@Test
	public void testSortDescending() throws StorageException
	{
		sortedQueryTest(false);
	}

	/***************************************
	 * Returns the size of a query result. The query result argument will be
	 * closed before returning.
	 *
	 * @param  rResult The query result to evaluate
	 *
	 * @return The number of objects returned by the query result
	 *
	 * @throws StorageException If processing the query result fails
	 */
	int getResultSize(QueryResult<?> rResult) throws StorageException
	{
		int nSize = 0;

		while (rResult.hasNext())
		{
			rResult.next();
			nSize++;
		}

		rResult.close();

		return nSize;
	}

	/***************************************
	 * Test method for {@link Storage#delete(Object)}.
	 *
	 * @throws StorageException
	 */
	void performDelete() throws StorageException
	{
		Predicate<Object> pWithNameJones = ifField("name", equalTo("jones"));

		Query<TestRecord> qJones =
			rStorage.query(forType(TestRecord.class, pWithNameJones));

		QueryResult<TestRecord> rJonesResult = qJones.execute();

		assertTrue(rJonesResult.hasNext());

		TestRecord rJones = rJonesResult.next();

		for (TestDetail rJonesDetail : rJones.getDetails())
		{
			rStorage.delete(rJonesDetail);
		}

		rStorage.delete(rJones);
		assertFalse(qJones.execute().hasNext());
	}

	/***************************************
	 * Internal method to perform and test a sorted query.
	 *
	 * @param  bAscending TRUE for ascending, false for descending ordering
	 *
	 * @throws StorageException If the query fails
	 */
	void sortedQueryTest(boolean bAscending) throws StorageException
	{
		Query<TestRecord> qSmithSorted =
			rStorage.query(forType(TestRecord.class,
								   ifField("name", equalTo("smith")).and(sortBy("value",
																				bAscending))));

		QueryResult<TestRecord> rResult = qSmithSorted.execute();
		int					    nValue  = bAscending ? 1 : 2;

		while (rResult.hasNext())
		{
			TestRecord rNext = rResult.next();

			assertTrue(nValue >= 1 && nValue <= 2);
			assertTrue(rNext.getValue() == nValue);
			nValue += bAscending ? 1 : -1;
		}

		rResult.close();
		qSmithSorted.execute();
	}

	/***************************************
	 * Stores a set of test records.
	 *
	 * @param  sName    The record name
	 * @param  nIdStart The ID of the first record
	 * @param  nCount   The number of records to store
	 *
	 * @throws StorageException On errors
	 */
	void storeTestRecords(String sName, int nIdStart, int nCount)
		throws StorageException
	{
		for (int i = 1; i <= nCount; i++)
		{
			TestRecord aRecord =
				new TestRecord(nIdStart + i - 1, sName, i, new Date());

			for (int j = 1; j <= 5; j++)
			{
				aRecord.addDetail(new TestDetail(sName + "-" + j, i * 10 + j));
			}

			rStorage.store(aRecord);
		}
	}
}
