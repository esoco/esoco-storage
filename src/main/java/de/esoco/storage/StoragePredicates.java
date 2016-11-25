//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// This file is a part of the 'esoco-storage' project.
// Copyright 2016 Elmar Sonnenschein, esoco GmbH, Flensburg, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		 http://www.apache.org/licenses/LICENSE-2.0
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
import de.esoco.lib.reflect.ReflectUtil;

import de.esoco.storage.impl.jdbc.JdbcStorage;
import de.esoco.storage.impl.jdbc.SqlExpressionFormat;

import java.util.regex.Pattern;

import org.obrel.core.Relatable;
import org.obrel.core.RelationType;

import static de.esoco.storage.StorageRelationTypes.STORAGE_FUNCTION;

import static org.obrel.type.MetaTypes.SORT_ASCENDING;


/********************************************************************
 * Contains static factory methods that generate predicates which are specific
 * for storage queries.
 *
 * @author eso
 */
public class StoragePredicates
{
	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Private, only static use.
	 */
	private StoragePredicates()
	{
	}

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Returns a predicate that matches if the target attribute is like a
	 * certain value after applying a SQL function. This is typically used to
	 * use fuzzy search functions in queries. Because these functions are mostly
	 * database-dependent the function name must be given as a string. Common
	 * examples for fuzzy matching functions are "soundex" or "metaphone".
	 *
	 * @param  sValue The compare value
	 *
	 * @return A new instance of {@link Like}
	 */
	public static BinaryPredicate<Object, String> almostLike(String sValue)
	{
		return new Like(sValue, true);
	}

	/***************************************
	 * Returns a new query predicate for a certain type of storage object.
	 *
	 * @param  rType    The class of the storage object to query for
	 * @param  pCritera A predicate containing the query criteria
	 *
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> QueryPredicate<T> forType(
		Class<T>			 rType,
		Predicate<? super T> pCritera)
	{
		return new QueryPredicate<T>(rType, pCritera);
	}

	/***************************************
	 * Returns a new query predicate for queries on child elements in
	 * master-detail relationships. This method is a shortcut to create a new
	 * instance of {@link QueryPredicate} for master-detail queries. It is only
	 * intended to be used for (parsed) storage queries and not for direct
	 * evaluation.
	 *
	 * @param  rChildType    The child type to query for
	 * @param  pChildCritera The criteria to apply to the child elements
	 *
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> Predicate<T> hasChild(
		Class<T>			 rChildType,
		Predicate<? super T> pChildCritera)
	{
		return new QueryPredicate<T>(rChildType, pChildCritera);
	}

	/***************************************
	 * Creates a new {@link ElementPredicate} for a certain attribute of storage
	 * objects. This provides generic access to attributes defined in arbitrary
	 * storage mapping but with less type safety than more specific element
	 * predicates. If possible applications should prefer such predicates if
	 * possible.
	 *
	 * @param  rMapping       The storage mapping the attribute is defined in
	 * @param  rAttribute     The attribute to apply the predicate to
	 * @param  pValueCriteria The predicate to apply to attribute values
	 *
	 * @return A new element predicate with the given parameters
	 */
	public static <T, A extends Relatable> ElementPredicate<T, Object> ifAttribute(
		StorageMapping<T, A, ?> rMapping,
		A						rAttribute,
		Predicate<Object>		pValueCriteria)
	{
		return new ElementPredicate<T, Object>(new GetAttribute<T, A>(rMapping,
																	  rAttribute),
											   pValueCriteria);
	}

	/***************************************
	 * This method is a just renamed variant of the standard predicate method
	 * {@link Predicates#ifField(String, Predicate)}. It exists only for
	 * semantic reasons to provide better readability in the context of storage
	 * queries, e.g. query(forType(X, withField(value, equalTo(1)))).
	 *
	 * @see Predicates#ifField(String, Predicate)
	 */
	public static <T, V> ElementPredicate<T, V> ifField(
		String		 sField,
		Predicate<V> pValueCriteria)
	{
		return Predicates.ifField(sField, pValueCriteria);
	}

	/***************************************
	 * Returns a predicate that checks whether input strings match the pattern
	 * of a certain SQL LIKE expression.
	 *
	 * @param  sSqlPattern The SQL LIKE pattern to compare input values with
	 *
	 * @return A new instance of {@link Like}
	 */
	public static BinaryPredicate<Object, String> like(String sSqlPattern)
	{
		return new Like(sSqlPattern, false);
	}

	/***************************************
	 * Returns a new query predicate for sub-queries on referenced objects in
	 * relationships between storage objects. This method is a shortcut to
	 * create a new instance of {@link QueryPredicate} for such queries with the
	 * appropriate semantics. It is mainly intended to be used for (parsed)
	 * storage queries and not for direct evaluation.
	 *
	 * @param  rReferencedType The referenced type to query for
	 * @param  pCritera        The criteria to apply to the referenced type
	 *
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> QueryPredicate<T> refersTo(
		Class<T>			 rReferencedType,
		Predicate<? super T> pCritera)
	{
		return new QueryPredicate<T>(rReferencedType, pCritera);
	}

	/***************************************
	 * Returns a new query predicate for sub-queries on referenced objects in
	 * relationships between storage objects. This method is a shortcut to
	 * create a new instance of {@link QueryPredicate} for such queries with the
	 * appropriate semantics. It is mainly intended to be used for (parsed)
	 * storage queries and not for direct evaluation.
	 *
	 * @param  rReferencedType The referenced type to query for
	 * @param  fReferencedAttr A function that defines the referenced attribute
	 * @param  pCritera        The criteria to apply to the referenced type
	 *
	 * @return A new instance of {@link QueryPredicate}
	 */
	public static <T> Predicate<T> refersTo(
		Class<T>			   rReferencedType,
		Function<? super T, ?> fReferencedAttr,
		Predicate<? super T>   pCritera)
	{
		QueryPredicate<T> pRefersTo = refersTo(rReferencedType, pCritera);

		pRefersTo.set(STORAGE_FUNCTION, fReferencedAttr);

		return pRefersTo;
	}

	/***************************************
	 * Creates a new predicate that defines an ascending sort order for a field.
	 *
	 * @see #sortBy(String, boolean)
	 */
	public static <T> SortPredicate<T> sortBy(String sField)
	{
		return sortBy(sField, true);
	}

	/***************************************
	 * Creates a new predicate that defines an ascending sort order for a
	 * certain relation.
	 *
	 * @see #sortBy(RelationType, boolean)
	 */
	public static <T extends Relatable> SortPredicate<T> sortBy(
		RelationType<?> rProperty)
	{
		return sortBy(rProperty, true);
	}

	/***************************************
	 * Creates a new predicate that defines a sort order for a certain field.
	 * The returned predicate only has a declarative purpose and will therefore
	 * always evaluate to TRUE.
	 *
	 * @param  sField     The name of the field to sort by
	 * @param  bAscending TRUE for ascending sort order, FALSE for descending
	 *
	 * @return A new sort predicate
	 */
	public static <T> SortPredicate<T> sortBy(String  sField,
											  boolean bAscending)
	{
		return new SortPredicate<>(sField, bAscending);
	}

	/***************************************
	 * Creates a new predicate that defines a sort order for a certain property
	 * that is defined by a relation type. The returned predicate only has a
	 * declarative purpose and will therefore always evaluate to TRUE.
	 *
	 * @param  rProperty  The type of the property to sort by
	 * @param  bAscending TRUE for ascending sort order, FALSE for descending
	 *
	 * @return A new sort predicate
	 */
	public static <T extends Relatable> SortPredicate<T> sortBy(
		RelationType<?> rProperty,
		boolean			bAscending)
	{
		return new SortPredicate<>(rProperty, bAscending);
	}

	//~ Inner Classes ----------------------------------------------------------

	/********************************************************************
	 * An element accessor that uses reflection to query the value of a certain
	 * field in target objects. The reflective access is done through the method
	 * {@link ReflectUtil#getFieldValue(String, Object)} which will try to make
	 * the field accessible if necessary. If that fails an exception will be
	 * thrown by {@link #getElementValue(Object, Relatable)}.
	 *
	 * <p><b>Attention:</b> if the function's output type (O) is not of type
	 * Object the field value will be cast to that type at runtime. If it does
	 * not match that type a ClassCastException will be thrown.</p>
	 */
	public static class GetAttribute<I, A extends Relatable>
		extends GetElement<I, A, Object>
	{
		//~ Instance fields ----------------------------------------------------

		private final StorageMapping<I, A, ?> rStorageMapping;

		//~ Constructors -------------------------------------------------------

		/***************************************
		 * Creates a new instance that accesses a particular field.
		 *
		 * @param rMapping   The storage mapping
		 * @param rAttribute The name of the field to access
		 */
		public GetAttribute(StorageMapping<I, A, ?> rMapping, A rAttribute)
		{
			super(rAttribute, "GetAttribute");
			rStorageMapping = rMapping;
		}

		//~ Methods ------------------------------------------------------------

		/***************************************
		 * @see GetElement#getElementValue(Object, Object)
		 */
		@Override
		protected Object getElementValue(I rObject, A rAttribute)
		{
			try
			{
				return rStorageMapping.getAttributeValue(rObject, rAttribute);
			}
			catch (StorageException e)
			{
				throw new IllegalArgumentException(e);
			}
		}
	}

	/********************************************************************
	 * A comparison that is equivalent to the SQL LIKE expression which compares
	 * string input values to a SQL-specific string pattern. Although this
	 * predicate is mainly intended to be parsed in storage patterns it can also
	 * be used directly. In that case the SQL pattern will be translated into a
	 * regular expression by replacing all occurrences of "%" with ".*" and of
	 * "_" with ".". Normally this should yield the same result as the SQL
	 * pattern but that may depend on the actual storage implementation or the
	 * underlying database, respectively. Therefore application code should not
	 * rely on the identity of database queries and direct application of this
	 * predicate.
	 */
	public static class Like extends Comparison<Object, String>
		implements SqlExpressionFormat
	{
		//~ Instance fields ----------------------------------------------------

		private boolean bFuzzySearch;

		//~ Constructors -------------------------------------------------------

		/***************************************
		 * Creates a new instance for a certain pattern.
		 *
		 * @param sSqlPattern  The SQL LIKE pattern to compare input values with
		 * @param bFuzzySearch bImmutable TRUE for an immutable instance
		 */
		public Like(String sSqlPattern, boolean bFuzzySearch)
		{
			super(sSqlPattern, "LIKE", true);

			this.bFuzzySearch = bFuzzySearch;
		}

		//~ Static methods -----------------------------------------------------

		/***************************************
		 * Converts a SQL LIKE pattern into a regular expression by replacing
		 * all occurrences of "%" with ".*" and of "_" with ".".
		 *
		 * @param  sSqlPattern The SQL LIKE pattern to convert
		 *
		 * @return The resulting regular expression
		 */
		public static String convertLikeToRegEx(String sSqlPattern)
		{
			sSqlPattern = sSqlPattern.replaceAll("%", ".*");
			sSqlPattern = sSqlPattern.replaceAll("_", ".");

			return sSqlPattern;
		}

		//~ Methods ------------------------------------------------------------

		/***************************************
		 * Converts the SQL pattern of this instance into a regular expression
		 * and matches the target value against it.
		 *
		 * @param  rValue   sText The text string to evaluate
		 * @param  sPattern The LIKE pattern to compare with
		 *
		 * @return TRUE if the text string matches the pattern
		 */
		@Override
		@SuppressWarnings("boxing")
		public Boolean evaluate(Object rValue, String sPattern)
		{
			return Pattern.matches(convertLikeToRegEx(sPattern),
								   rValue.toString());
		}

		/***************************************
		 * {@inheritDoc}
		 */
		@Override
		public String format(JdbcStorage  rStorage,
							 Predicate<?> pExpression,
							 String		  sColumn,
							 String		  sValue,
							 boolean	  bNegate)
		{
			StringBuilder aResult			   = new StringBuilder();
			String		  sFuzzySearchFunction =
				rStorage.getFuzzySearchFunction();

			if (bFuzzySearch && sFuzzySearchFunction != null)
			{
				aResult.append(sFuzzySearchFunction);
				aResult.append('(').append(sColumn).append(')').append(' ');
				aResult.append(bNegate ? "<>" : "=");
				aResult.append(' ').append(sFuzzySearchFunction);
				aResult.append('(').append(sValue).append(')');
			}
			else
			{
				aResult.append(sColumn).append(' ');
				aResult.append(bNegate ? "NOT LIKE" : "LIKE");
				aResult.append(' ').append(sValue);
			}

			return aResult.toString();
		}
	}

	/********************************************************************
	 * An element predicate that is used to define the ordering of storage
	 * queries.
	 *
	 * @author eso
	 */
	public static class SortPredicate<T> extends ElementPredicate<T, Object>
	{
		//~ Constructors -------------------------------------------------------

		/***************************************
		 * Creates a new instance for a certain field in the target class.
		 *
		 * @param sField     The name of the field
		 * @param bAscending TRUE for an ascending sort, FALSE for descending
		 */
		public SortPredicate(String sField, boolean bAscending)
		{
			this(new ReadField<T, Object>(sField), bAscending);
		}

		/***************************************
		 * Creates a new instance.
		 *
		 * @param fSortElement The function to access the element that defines
		 *                     the sort order
		 * @param bAscending   TRUE for an ascending sort, FALSE for descending
		 */
		@SuppressWarnings({ "unchecked", "boxing" })
		public SortPredicate(
			ElementAccessFunction<?, ? super T, ?> fSortElement,
			boolean								   bAscending)
		{
			super((ElementAccessFunction<?, ? super T, Object>) fSortElement,
				  Predicates.alwaysTrue());

			set(SORT_ASCENDING, bAscending);
		}
	}
}
