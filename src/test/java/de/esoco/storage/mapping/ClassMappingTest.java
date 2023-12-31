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
package de.esoco.storage.mapping;

import de.esoco.storage.TestRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class storage mappings.
 *
 * @author eso
 */
public class ClassMappingTest {

	/**
	 * Test instance creation.
	 */
	@Test
	public void testConstructor() throws Exception {
		ClassMapping<TestRecord> aMapping =
			new ClassMapping<>(TestRecord.class);

		assertEquals(5, aMapping.getAttributes().size());
		assertEquals(1, aMapping.getChildMappings().size());
	}
}
