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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/********************************************************************
 * Test data record.
 *
 * @author eso
 */
public class TestRecord
{
	//~ Instance fields --------------------------------------------------------

	private int				 id;
	private String			 name;
	private int				 value;
	private Date			 date;
	private List<TestDetail> details = new ArrayList<TestDetail>();

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Default constructor.
	 */
	public TestRecord()
	{
	}

	/***************************************
	 * Creates a new instance.
	 *
	 * @param nId
	 * @param sName
	 * @param nValue
	 * @param rDate
	 */
	public TestRecord(int nId, String sName, int nValue, Date rDate)
	{
		this.id    = nId;
		this.date  = rDate;
		this.name  = sName;
		this.value = nValue;
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Adds a detail record.
	 *
	 * @param rDetail The new detail record
	 */
	public void addDetail(TestDetail rDetail)
	{
		details.add(rDetail);
		rDetail.parent = this;
	}

	/***************************************
	 * Returns the date.
	 *
	 * @return The date
	 */
	public final Date getDate()
	{
		return date;
	}

	/***************************************
	 * Returns the details.
	 *
	 * @return The details
	 */
	public final List<TestDetail> getDetails()
	{
		return details;
	}

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
		StringBuilder sb = new StringBuilder("TestRecord[");

		sb.append(id + ", " + name + ", " + value + ", " + date);

		if (details.size() > 0)
		{
			sb.append(details);
		}

		sb.append(']');

		return sb.toString();
	}
}
