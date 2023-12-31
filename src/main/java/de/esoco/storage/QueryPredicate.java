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

import de.esoco.lib.expression.Predicate;
import org.obrel.core.RelatedObject;

/**
 * A predicate that encapsulates the data that is necessary to perform a query
 * on objects in a storage. Although this class is mainly intended to define
 * storage queries it's evaluate() method can also be applied to objects of the
 * queried type if necessary.
 *
 * @author eso
 */
public class QueryPredicate<T> extends RelatedObject implements Predicate<T> {

	private final Class<T> queryType;

	private final Predicate<? super T> criteria;

	/**
	 * Creates a new instance for a certain class and query criteria.
	 *
	 * @param queryType The class of the objects to be queried
	 * @param criteria  The query criteria predicate
	 */
	public QueryPredicate(Class<T> queryType, Predicate<? super T> criteria) {
		this.queryType = queryType;
		this.criteria = criteria;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		QueryPredicate<?> other = (QueryPredicate<?>) obj;

		return queryType == other.queryType && criteria.equals(other.criteria);
	}

	/**
	 * Evaluates the argument value with the query criteria of this instance.
	 *
	 * @param value The value to evaluate
	 * @return The result of the evaluation
	 */
	@Override
	public Boolean evaluate(T value) {
		return criteria.evaluate(value);
	}

	/**
	 * Returns the query criteria predicate for this storage object predicate.
	 *
	 * @return The query criteria predicate
	 */
	public final Predicate<? super T> getCriteria() {
		return criteria;
	}

	/**
	 * Returns the datatype that will be queried with this predicate.
	 *
	 * @return The query type
	 */
	public final Class<T> getQueryType() {
		return queryType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return 31 * (queryType.hashCode() + 31 * criteria.hashCode());
	}
}
