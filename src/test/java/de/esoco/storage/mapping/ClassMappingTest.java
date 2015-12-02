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
package de.esoco.storage.mapping;

import de.esoco.storage.StorageException;
import de.esoco.storage.TestDetail;
import de.esoco.storage.TestRecord;

import java.util.Date;

import static org.junit.Assert.assertEquals;


/********************************************************************
 * Test class storage mappings.
 *
 * @author eso
 */
public class ClassMappingTest
{
	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Test instance creation.
	 *
	 * @throws StorageException
	 */
	public void testConstructor() throws StorageException
	{
		ClassMapping<TestRecord> aMapping =
			new ClassMapping<>(TestRecord.class);

		TestRecord aRecord = new TestRecord(1, "foo", 42, new Date());

		aRecord.addDetail(new TestDetail("DETAIL1", 1));
		aRecord.addDetail(new TestDetail("DETAIL2", 2));

		assertEquals(4, aMapping.getAttributes().size());
		assertEquals(1, aMapping.getChildMappings().size());
	}
}
