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

/********************************************************************
 * Detail data for test records.
 *
 * @author eso
 */
public class TestDetail
{
	//~ Instance fields --------------------------------------------------------

	TestRecord     parent;
	private int    id    = -1;
	private String name;
	private int    value;

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Default constructor.
	 */
	public TestDetail()
	{
	}

	/***************************************
	 * Creates a new instance.
	 *
	 * @param sName
	 * @param nValue
	 */
	public TestDetail(String sName, int nValue)
	{
		this.name  = sName;
		this.value = nValue;
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Returns the id.
	 *
	 * @return The id
	 */
	public final int getId()
	{
		return id;
	}

	/***************************************
	 * Returns the name.
	 *
	 * @return The name
	 */
	public final String getName()
	{
		return name;
	}

	/***************************************
	 * Returns the parent.
	 *
	 * @return The parent
	 */
	public final TestRecord getParent()
	{
		return parent;
	}

	/***************************************
	 * Returns the value.
	 *
	 * @return The value
	 */
	public final int getValue()
	{
		return value;
	}

	/***************************************
	 * toString method
	 *
	 * @return a string
	 */
	@Override
	public String toString()
	{
		return "TestDetail[" + name + ", " + value + "]";
	}
}
