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

import de.esoco.lib.expression.Predicate;


/********************************************************************
 * An interface for the formatting of expressions in SQL statements.
 *
 * @author eso
 */
public interface SqlExpressionFormat
{
	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * Formats an expression predicate with the given values.
	 *
	 * @param  rStorage    The JDBC storage to format the expressionfor
	 * @param  pExpression The expression predicate
	 * @param  sColumn     The column to format the expression for
	 * @param  sValue      The compare value(s) or value placeholders
	 * @param  bNegate     TRUE if the expression should be negated
	 *
	 * @return The resulting expression string
	 */
	public String format(JdbcStorage  rStorage,
						 Predicate<?> pExpression,
						 String		  sColumn,
						 String		  sValue,
						 boolean	  bNegate);
}
