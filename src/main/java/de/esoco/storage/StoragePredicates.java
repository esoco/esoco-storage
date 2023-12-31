//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2018 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
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

import de.esoco.lib.expression.BinaryPredicate;
import de.esoco.lib.expression.ElementAccessFunction;
import de.esoco.lib.expression.Function;
import de.esoco.lib.expression.Predicate;
import de.esoco.lib.expression.Predicates;
import de.esoco.lib.expression.function.GetElement;
import de.esoco.lib.expression.function.GetElement.ReadField;
import de.esoco.lib.expression.predicate.Comparison;
import de.esoco.lib.expression.predicate.ElementPredicate;
import de.esoco.lib.property.SortDirection;
import de.esoco.lib.reflect.ReflectUtil;

import de.esoco.storage.impl.jdbc.JdbcStorage;
import de.esoco.storage.impl.jdbc.SqlExpressionFormat;

import java.util.regex.Pattern;

import org.obrel.core.Relatable;
import org.obrel.core.RelationType;

import static de.esoco.storage.StorageRelationTypes.STORAGE_FUNCTION;

import static org.obrel.type.MetaTypes.SORT_DIRECTION;

/**
 * Contains static factory methods that generate predicates which are specific
 * for storage queries.
 *
 * @author eso
 */
public class StoragePredicates {

	/**
	 * Private, only static use.
	 */
	private StoragePredicates() {
	}

	/**
	 * Creates a wildcard filter predicate for a SQL like expression. The
	 * filter
	 * string may contain wildcard characters like '*' or '?' (or their SQL
	 * equivalents). If the filter string doesn't contain a '*' wildcard one
	 * will be appended to the end (to match all values that start with the
	 * filter value). Any wildcards in the given filter value will be converted
	 * to the corresponding SQL wildcards by means of
	 * {@link StorageManager#convertToSqlConstraint(String)}.
	 *
	 * @param filter The original filter value
	 * @return The converted filter value
	 */
	@SuppressWarnings("unchecked")
	public static <T> Predicate<T> createWildcardFilter(String filter) {
		if (filter.indexOf('*') == -1) {
			filter = "%" + filter + "%";
		}

		filter = StorageManager.convertToSqlConstraint(filter);

		return (Predicate<T>) like(filter);
	}

	/**
	 * Returns a new query predicate for a certain type of storage object.
	 *
	 * @param type    The class of the storage object to query for
	 * @param critera A predicate containing the query criteria
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> QueryPredicate<T> forType(Class<T> type,
		Predicate<? super T> critera) {
		return new QueryPredicate<T>(type, critera);
	}

	/**
	 * Returns a new query predicate for queries on child elements in
	 * master-detail relationships. This method is a shortcut to create a new
	 * instance of {@link QueryPredicate} for master-detail queries. It is only
	 * intended to be used for (parsed) storage queries and not for direct
	 * evaluation.
	 *
	 * @param childType    The child type to query for
	 * @param childCritera The criteria to apply to the child elements
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> Predicate<T> hasChild(Class<T> childType,
		Predicate<? super T> childCritera) {
		return new QueryPredicate<T>(childType, childCritera);
	}

	/**
	 * Creates a new {@link ElementPredicate} for a certain attribute of
	 * storage
	 * objects. This provides generic access to attributes defined in arbitrary
	 * storage mapping but with less type safety than more specific element
	 * predicates. If possible applications should prefer such predicates if
	 * possible.
	 *
	 * @param mapping       The storage mapping the attribute is defined in
	 * @param attribute     The attribute to apply the predicate to
	 * @param valueCriteria The predicate to apply to attribute values
	 * @return A new element predicate with the given parameters
	 */
	public static <T, A extends Relatable> ElementPredicate<T, Object> ifAttribute(
		StorageMapping<T, A, ?> mapping, A attribute,
		Predicate<Object> valueCriteria) {
		return new ElementPredicate<T, Object>(
			new GetAttribute<T, A>(mapping, attribute), valueCriteria);
	}

	/**
	 * This method is a just renamed variant of the standard predicate method
	 * {@link Predicates#ifField(String, Predicate)}. It exists only for
	 * semantic reasons to provide better readability in the context of storage
	 * queries, e.g. query(forType(X, withField(value, equalTo(1)))).
	 *
	 * @see Predicates#ifField(String, Predicate)
	 */
	public static <T, V> ElementPredicate<T, V> ifField(String field,
		Predicate<V> valueCriteria) {
		return Predicates.ifField(field, valueCriteria);
	}

	/**
	 * Returns a predicate that checks whether input strings match the pattern
	 * of a certain SQL LIKE expression.
	 *
	 * @param sqlPattern The SQL LIKE pattern to compare input values with
	 * @return A new instance of {@link Like}
	 */
	public static BinaryPredicate<Object, String> like(String sqlPattern) {
		return new Like(sqlPattern, false);
	}

	/**
	 * Returns a new query predicate for sub-queries on referenced objects in
	 * relationships between storage objects. This method is a shortcut to
	 * create a new instance of {@link QueryPredicate} for such queries with
	 * the
	 * appropriate semantics. It is mainly intended to be used for (parsed)
	 * storage queries and not for direct evaluation.
	 *
	 * @param referencedType The referenced type to query for
	 * @param critera        The criteria to apply to the referenced type
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> QueryPredicate<T> refersTo(Class<T> referencedType,
		Predicate<? super T> critera) {
		return new QueryPredicate<T>(referencedType, critera);
	}

	/**
	 * Returns a new query predicate for sub-queries on referenced objects in
	 * relationships between storage objects. This method is a shortcut to
	 * create a new instance of {@link QueryPredicate} for such queries with
	 * the
	 * appropriate semantics. It is intended to be used for (parsed) storage
	 * queries and not for direct evaluation.
	 *
	 * @param referencedType The referenced type to query for
	 * @param referencedAttr A function that defines the referenced attribute
	 * @param critera        The criteria to apply to the referenced type
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T, V> Predicate<V> refersTo(Class<T> referencedType,
		Function<? super T, V> referencedAttr, Predicate<? super T> critera) {
		@SuppressWarnings("unchecked")
		QueryPredicate<V> refersTo =
			(QueryPredicate<V>) refersTo(referencedType, critera);

		refersTo.set(STORAGE_FUNCTION, referencedAttr);

		return refersTo;
	}

	/**
	 * Returns a predicate that matches if the target attribute is similar to a
	 * certain value ("fuzzy" search). The similarity is determined by applying
	 * a SQL function. This function is defined from the storage parameters
	 * when
	 * creating a new storage instance.
	 *
	 * @param value The compare value
	 * @return A new instance of {@link Like}
	 */
	public static BinaryPredicate<Object, String> similarTo(String value) {
		return new Like(value, true);
	}

	/**
	 * Creates a new predicate that defines an ascending sort order for a
	 * field.
	 *
	 * @see #sortBy(String, boolean)
	 */
	public static <T> SortPredicate<T> sortBy(String field) {
		return sortBy(field, true);
	}

	/**
	 * Creates a new predicate that defines an ascending sort order for a
	 * certain relation.
	 *
	 * @see #sortBy(RelationType, boolean)
	 */
	public static <T extends Relatable> SortPredicate<T> sortBy(
		RelationType<?> type) {
		return sortBy(type, true);
	}

	/**
	 * Creates a new predicate that defines a sort order for a certain field.
	 * The returned predicate only has a declarative purpose and will therefore
	 * always evaluate to TRUE.
	 *
	 * @param field     The name of the field to sort by
	 * @param ascending TRUE for ascending sort order, FALSE for descending
	 * @return A new sort predicate
	 */
	public static <T> SortPredicate<T> sortBy(String field,
		boolean ascending) {
		return sortBy(field,
			ascending ? SortDirection.ASCENDING : SortDirection.DESCENDING);
	}

	/**
	 * Creates a new predicate that defines a sort order for a certain field.
	 * The returned predicate only has a declarative purpose and will therefore
	 * always evaluate to TRUE.
	 *
	 * @param field     The name of the field to sort by
	 * @param direction The sort direction
	 * @return A new sort predicate
	 */
	public static <T> SortPredicate<T> sortBy(String field,
		SortDirection direction) {
		return new SortPredicate<>(field, direction);
	}

	/**
	 * Creates a new predicate that defines a sort order for a certain property
	 * that is defined by a relation type. The returned predicate only has a
	 * declarative purpose and will therefore always evaluate to TRUE.
	 *
	 * @param type      The type of the property to sort by
	 * @param ascending TRUE for ascending sort order, FALSE for descending
	 * @return A new sort predicate
	 */
	public static <T extends Relatable> SortPredicate<T> sortBy(
		RelationType<?> type, boolean ascending) {
		return sortBy(type,
			ascending ? SortDirection.ASCENDING : SortDirection.DESCENDING);
	}

	/**
	 * Creates a new predicate that defines a sort order for a certain property
	 * that is defined by a relation type. The returned predicate only has a
	 * declarative purpose and will therefore always evaluate to TRUE.
	 *
	 * @param type      The type of the property to sort by
	 * @param direction The sort direction
	 * @return A new sort predicate
	 */
	public static <T extends Relatable> SortPredicate<T> sortBy(
		RelationType<?> type, SortDirection direction) {
		return new SortPredicate<>(type, direction);
	}

	/**
	 * An element accessor that uses reflection to query the value of a certain
	 * field in target objects. The reflective access is done through the
	 * method
	 * {@link ReflectUtil#getFieldValue(String, Object)} which will try to make
	 * the field accessible if necessary. If that fails an exception will be
	 * thrown by {@link #getElementValue(Object, Relatable)}.
	 *
	 * <p><b>Attention:</b> if the function's output type (O) is not of type
	 * Object the field value will be cast to that type at runtime. If it does
	 * not match that type a ClassCastException will be thrown.</p>
	 */
	public static class GetAttribute<I, A extends Relatable>
		extends GetElement<I, A, Object> {

		private final StorageMapping<I, A, ?> storageMapping;

		/**
		 * Creates a new instance that accesses a particular field.
		 *
		 * @param mapping   The storage mapping
		 * @param attribute The name of the field to access
		 */
		public GetAttribute(StorageMapping<I, A, ?> mapping, A attribute) {
			super(attribute, "GetAttribute");
			storageMapping = mapping;
		}

		/**
		 * @see GetElement#getElementValue(Object, Object)
		 */
		@Override
		protected Object getElementValue(I object, A attribute) {
			try {
				return storageMapping.getAttributeValue(object, attribute);
			} catch (StorageException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}

	/**
	 * A comparison that is equivalent to the SQL LIKE expression which
	 * compares
	 * string input values to a SQL-specific string pattern. Although this
	 * predicate is mainly intended to be parsed in storage patterns it can
	 * also
	 * be used directly. In that case the SQL pattern will be translated into a
	 * regular expression by replacing all occurrences of "%" with ".*" and of
	 * "_" with ".". Normally this should yield the same result as the SQL
	 * pattern but that may depend on the actual storage implementation or the
	 * underlying database, respectively. Therefore application code should not
	 * rely on the identity of database queries and direct application of this
	 * predicate.
	 */
	public static class Like extends Comparison<Object, String>
		implements SqlExpressionFormat {

		private boolean fuzzySearch;

		/**
		 * Creates a new instance for a certain pattern.
		 *
		 * @param sqlPattern  The SQL LIKE pattern to compare input values with
		 * @param fuzzySearch TRUE for a fuzzy search that also finds similar
		 *                    terms
		 */
		public Like(String sqlPattern, boolean fuzzySearch) {
			super(sqlPattern, "LIKE");

			this.fuzzySearch = fuzzySearch;
		}

		/**
		 * Converts a SQL LIKE pattern into a regular expression by replacing
		 * all occurrences of "%" with ".*" and of "_" with ".".
		 *
		 * @param sqlPattern The SQL LIKE pattern to convert
		 * @return The resulting regular expression
		 */
		public static String convertLikeToRegEx(String sqlPattern) {
			sqlPattern = sqlPattern.replaceAll("%", ".*");
			sqlPattern = sqlPattern.replaceAll("_", ".");

			return sqlPattern;
		}

		/**
		 * Converts the SQL pattern of this instance into a regular expression
		 * and matches the target value against it.
		 *
		 * @param value   text The text string to evaluate
		 * @param pattern The LIKE pattern to compare with
		 * @return TRUE if the text string matches the pattern
		 */
		@Override
		@SuppressWarnings("boxing")
		public Boolean evaluate(Object value, String pattern) {
			return Pattern.matches(convertLikeToRegEx(pattern),
				value.toString());
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String format(JdbcStorage storage, Predicate<?> expression,
			String column, String value, boolean negate) {
			StringBuilder result = new StringBuilder();
			String fuzzySearchFunction = storage.getFuzzySearchFunction();

			if (fuzzySearch && fuzzySearchFunction != null) {
				result.append(fuzzySearchFunction);
				result.append('(').append(column).append(')').append(' ');
				result.append(negate ? "<>" : "=");
				result.append(' ').append(fuzzySearchFunction);
				result.append('(').append(value).append(')');
			} else {
				result.append(column).append(' ');
				result.append(negate ? "NOT LIKE" : "LIKE");
				result.append(' ').append(value);
			}

			return result.toString();
		}
	}

	/**
	 * An element predicate that is used to define the ordering of storage
	 * queries.
	 *
	 * @author eso
	 */
	public static class SortPredicate<T> extends ElementPredicate<T, Object> {

		/**
		 * Creates a new instance for a certain field in the target class.
		 *
		 * @param field     The name of the field
		 * @param direction The sort direction
		 */
		public SortPredicate(String field, SortDirection direction) {
			this(new ReadField<T, Object>(field), direction);
		}

		/**
		 * Creates a new instance.
		 *
		 * @param sortElement The function to access the element that defines
		 *                    the sort order
		 * @param direction   The sort direction
		 */
		@SuppressWarnings({ "unchecked", "boxing" })
		public SortPredicate(ElementAccessFunction<?, ? super T, ?> sortElement,
			SortDirection direction) {
			super((ElementAccessFunction<?, ? super T, Object>) sortElement,
				Predicates.alwaysTrue());

			set(SORT_DIRECTION, direction);
		}
	}
}
