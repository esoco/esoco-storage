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
package de.esoco.storage.impl.jdbc;

import de.esoco.lib.expression.ElementAccess;
import de.esoco.lib.expression.Function;
import de.esoco.lib.expression.Predicate;
import de.esoco.lib.expression.Predicates;
import de.esoco.lib.expression.StringFunctions;
import de.esoco.lib.expression.function.Cast;
import de.esoco.lib.expression.function.FunctionChain;
import de.esoco.lib.expression.function.GetElement.ReadField;
import de.esoco.lib.expression.function.GetSubstring;
import de.esoco.lib.expression.predicate.Comparison;
import de.esoco.lib.expression.predicate.Comparison.ElementOf;
import de.esoco.lib.expression.predicate.Comparison.EqualTo;
import de.esoco.lib.expression.predicate.Comparison.GreaterOrEqual;
import de.esoco.lib.expression.predicate.Comparison.GreaterThan;
import de.esoco.lib.expression.predicate.Comparison.LessOrEqual;
import de.esoco.lib.expression.predicate.Comparison.LessThan;
import de.esoco.lib.expression.predicate.ElementPredicate;
import de.esoco.lib.expression.predicate.FunctionPredicate;
import de.esoco.lib.expression.predicate.PredicateJoin;
import de.esoco.lib.logging.Log;
import de.esoco.lib.manage.Closeable;

import de.esoco.storage.Query;
import de.esoco.storage.QueryPredicate;
import de.esoco.storage.QueryResult;
import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.StorageMapping;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import static de.esoco.lib.expression.Predicates.equalTo;
import static de.esoco.lib.expression.Predicates.isNull;

import static de.esoco.storage.StoragePredicates.ifAttribute;
import static de.esoco.storage.StorageRelationTypes.IS_CHILD_QUERY;
import static de.esoco.storage.StorageRelationTypes.QUERY_DEPTH;
import static de.esoco.storage.StorageRelationTypes.QUERY_LIMIT;
import static de.esoco.storage.StorageRelationTypes.QUERY_OFFSET;
import static de.esoco.storage.StorageRelationTypes.STORAGE_FUNCTION;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.JDBC_CHILD_QUERY;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_DISABLE_CHILD_COUNTS;
import static de.esoco.storage.impl.jdbc.JdbcRelationTypes.SQL_QUERY_PAGING_EXPRESSION;
import static de.esoco.storage.impl.jdbc.JdbcStorage.formatStatement;

import static org.obrel.type.MetaTypes.SORT_ASCENDING;


/********************************************************************
 * A JDBC implementation of the query interface.
 *
 * @author eso
 */
public class JdbcQuery<T> extends RelatedObject implements Query<T>, Closeable
{
	//~ Static fields/initializers ---------------------------------------------

	private static final String SELECT_TEMPLATE = "SELECT %s FROM %s";
	private static final String SQL_NEGATION    = " NOT ";

	//~ Instance fields --------------------------------------------------------

	private JdbcStorage rStorage;

	private final QueryPredicate<T>				  pQuery;
	private final StorageMapping<T, Relatable, ?> rMapping;
	private final String						  sQueryCriteria;
	private final String						  sOrderCriteria;

	private JdbcQueryResult<T> aCurrentResult;
	private PreparedStatement  aQueryStatement;

	private List<Object> aCompareAttributes = new ArrayList<Object>();
	private List<Object> aCompareValues     = new ArrayList<Object>();

	private List<ElementPredicate<?, ?>> aSortPredicates =
		new ArrayList<ElementPredicate<?, ?>>();

	//~ Constructors -----------------------------------------------------------

	/***************************************
	 * Creates a new instance that retrieves objects from a JDBC connection
	 * which match a certain query predicate.
	 *
	 * @param  rStorage The storage to perform the query on
	 * @param  pQuery   The query predicate
	 *
	 * @throws StorageException On database or criteria parsing errors
	 */
	@SuppressWarnings("unchecked")
	JdbcQuery(JdbcStorage rStorage, QueryPredicate<T> pQuery)
		throws StorageException
	{
		this.rStorage = rStorage;
		this.pQuery   = pQuery;

		Class<T>			 rType     = pQuery.getQueryType();
		Predicate<? super T> pCriteria = pQuery.getCriteria();

		rMapping =
			(StorageMapping<T, Relatable, ?>) StorageManager.getMapping(rType);

		// parseQueryCriteria will also fill aSortPredicates
		sQueryCriteria = parseQueryCriteria(rMapping, pCriteria);
		sOrderCriteria = createOrderCriteria(aSortPredicates);

		if (pQuery.hasRelation(QUERY_DEPTH))
		{
			set(QUERY_DEPTH, pQuery.get(QUERY_DEPTH));
		}
		else if (pCriteria != null &&
				 (pCriteria instanceof Relatable) &&
				 ((Relatable) pCriteria).hasRelation(QUERY_DEPTH))
		{
			set(QUERY_DEPTH, ((Relatable) pCriteria).get(QUERY_DEPTH));
		}
		else if (rStorage.hasRelation(QUERY_DEPTH))
		{
			set(QUERY_DEPTH, rStorage.get(QUERY_DEPTH));
		}
	}

	//~ Static methods ---------------------------------------------------------

	/***************************************
	 * Creates a query predicate for the children in a hierarchical oder a
	 * master-detail query.
	 *
	 * @param  rParentMapping The storage mapping of the parent object
	 * @param  rChildMapping  The storage mapping of the child objects to query
	 * @param  rParentId      The ID of the parent object
	 * @param  nQueryDepth    The maximum query depth for recursive queries
	 *
	 * @return The child query predicate
	 */
	@SuppressWarnings("boxing")
	static <T> QueryPredicate<T> createChildQueryPredicate(
		StorageMapping<?, ?, ?>			rParentMapping,
		StorageMapping<T, Relatable, ?> rChildMapping,
		Object							rParentId,
		int								nQueryDepth)
	{
		Relatable rParentAttr =
			rChildMapping.getParentAttribute(rParentMapping);

		if (rParentAttr == null)
		{
			throw new IllegalStateException(String.format("No parent attribute for %s in %s",
														  rParentMapping,
														  rChildMapping));
		}

		Predicate<T> pCriteria =
			ifAttribute(rChildMapping, rParentAttr, equalTo(rParentId));

		// if the query is for the roots of a master-detail relationship and
		// the details also have a self-hierarchy query only the root objects
		// where the self-typed parent is NULL
		if (rParentMapping != rChildMapping)
		{
			Relatable rAttr = rChildMapping.getParentAttribute(rChildMapping);

			if (rAttr != null)
			{
				pCriteria =
					pCriteria.and(ifAttribute(rChildMapping, rAttr, isNull()));
			}
		}

		QueryPredicate<T> pQuery =
			new QueryPredicate<T>(rChildMapping.getMappedType(), pCriteria);

		pQuery.set(QUERY_DEPTH, nQueryDepth);
		pQuery.set(JDBC_CHILD_QUERY);

		return pQuery;
	}

	//~ Methods ----------------------------------------------------------------

	/***************************************
	 * @see Closeable#close()
	 */
	@Override
	public void close()
	{
		try
		{
			if (aCurrentResult != null)
			{
				aCurrentResult.close();
				aCurrentResult = null;
			}
		}
		catch (Exception e)
		{
			Log.warn("Closing ResultSet failed", e);
		}

		try
		{
			if (aQueryStatement != null)
			{
				aQueryStatement.close();
				aQueryStatement = null;
			}
		}
		catch (SQLException e)
		{
			Log.error("Closing Statement failed", e);
		}
	}

	/***************************************
	 * @see Query#execute()
	 */
	@Override
	@SuppressWarnings("boxing")
	public QueryResult<T> execute() throws StorageException
	{
		try
		{
			if (aQueryStatement != null)
			{
				aQueryStatement.close();
			}

			int    nOffset = get(QUERY_OFFSET);
			String sPaging = "";

			if (sOrderCriteria.length() > 0 && hasRelation(QUERY_LIMIT))
			{
				sPaging = rStorage.get(SQL_QUERY_PAGING_EXPRESSION);

				if (sPaging != null)
				{
					sPaging =
						String.format(" " + sPaging, get(QUERY_LIMIT), nOffset);
					nOffset = 0;
				}
				else
				{
					sPaging = "";
				}
			}

			aQueryStatement = prepareQueryStatement(sPaging);

			Log.debugf("QueryParams: %s", aCompareValues);
			setQueryParameters(aQueryStatement);

			ResultSet rResultSet  = aQueryStatement.executeQuery();
			boolean   bChildQuery =
				pQuery.hasFlag(JDBC_CHILD_QUERY) ||
				pQuery.hasFlag(IS_CHILD_QUERY);

			aCurrentResult =
				new JdbcQueryResult<T>(rStorage,
									   rMapping,
									   rResultSet,
									   nOffset,
									   bChildQuery);

			if (hasRelation(QUERY_DEPTH))
			{
				aCurrentResult.set(QUERY_DEPTH, get(QUERY_DEPTH));
			}

			return aCurrentResult;
		}
		catch (SQLException e)
		{
			String sMessage = "Query execution failed: " + pQuery;

			Log.error(sMessage, e);
			throw new StorageException(sMessage, e);
		}
	}

	/***************************************
	 * @see Query#getDistinct(Relatable)
	 */
	@Override
	public Set<Object> getDistinct(Relatable rAttribute) throws StorageException
	{
		String sSql =
			formatStatement(SELECT_TEMPLATE,
							"DISTINCT " + rStorage.getSqlName(rAttribute, true),
							rStorage.getSqlName(rMapping, true)) +
			sQueryCriteria;

		Set<Object> aResult = new HashSet<>();

		try (PreparedStatement aStatement =
			 rStorage.getConnection().prepareStatement(sSql))
		{
			setQueryParameters(aStatement);

			ResultSet rResult = aStatement.executeQuery();

			while (rResult.next())
			{
				Object rValue = rResult.getObject(1);

				rValue = rMapping.checkAttributeValue(rAttribute, rValue);
				aResult.add(rValue);
			}
		}
		catch (SQLException e)
		{
			throw new StorageException(e);
		}

		return aResult;
	}

	/***************************************
	 * @see Query#getQueryPredicate()
	 */
	@Override
	public QueryPredicate<T> getQueryPredicate()
	{
		return pQuery;
	}

	/***************************************
	 * @see Query#getStorage()
	 */
	@Override
	public Storage getStorage()
	{
		return rStorage;
	}

	/***************************************
	 * @see Query#positionOf(Object)
	 */
	@Override
	public int positionOf(Object rId)
	{
		String sIdAttr   = rStorage.getSqlName(rMapping.getIdAttribute(), true);
		String sCriteria = "";
		String sOrder    = "";

		if (sQueryCriteria != null && sQueryCriteria.length() > 0)
		{
			sCriteria = sQueryCriteria;
		}

		if (sOrderCriteria != null && sOrderCriteria.length() > 0)
		{
			sOrder = sOrderCriteria;
		}

		String sSql =
			formatStatement("SELECT row FROM (SELECT row_number() " +
							"OVER(%s) as row, %s FROM %s %s) AS rownums WHERE %s = ?",
							sOrder,
							sIdAttr,
							rStorage.getSqlName(rMapping, true),
							sCriteria,
							sIdAttr);

		try
		{
			return queryInteger(sSql, rId) - 1;
		}
		catch (StorageException e)
		{
			Log.debug("Database doesn't support row_number() function", e);

			return -1;
		}
	}

	/***************************************
	 * @see Query#size()
	 */
	@Override
	public int size() throws StorageException
	{
		String sSql =
			formatStatement(SELECT_TEMPLATE,
							"COUNT(*)",
							rStorage.getSqlName(rMapping, true)) +
			sQueryCriteria;

		return queryInteger(sSql);
	}

	/***************************************
	 * Creates the SQL ORDER BY clause from a list of predicates.
	 *
	 * @param  rSortPredicates The element predicates to create the order clause
	 *                         from
	 *
	 * @return The complete ORDER BY clause or an empty string if no sort
	 *         criteria are defined
	 */
	String createOrderCriteria(List<ElementPredicate<?, ?>> rSortPredicates)
	{
		String sResult = "";

		if (rSortPredicates.size() > 0)
		{
			StringBuilder aCriteria = new StringBuilder(" ORDER BY ");

			for (ElementPredicate<?, ?> rPredicate : rSortPredicates)
			{
				Object rAttr = rPredicate.getElementDescriptor();

				aCriteria.append(rStorage.getSqlName(rAttr, true));

				if (!rPredicate.hasFlag(SORT_ASCENDING))
				{
					aCriteria.append(" DESC");
				}

				aCriteria.append(',');
			}

			// remove the trailing comma
			aCriteria.setLength(aCriteria.length() - 1);
			sResult = aCriteria.toString();
		}

		return sResult;
	}

	/***************************************
	 * Returns a comma separated list of column names for a certain storage
	 * mapping.
	 *
	 * @param  rMapping The storage mapping
	 *
	 * @return The column list for the given mapping
	 */
	String getColumnList(StorageMapping<?, ?, ?> rMapping)
	{
		Collection<?> rAttributes = rMapping.getAttributes();
		StringBuilder aColumns    = new StringBuilder(rAttributes.size() * 10);

		for (Object rAttr : rAttributes)
		{
			aColumns.append(rStorage.getSqlName(rAttr, true)).append(',');
		}

		if (!rMapping.hasFlag(SQL_DISABLE_CHILD_COUNTS))
		{
			for (StorageMapping<?, ?, ?> rChildMapping :
				 rMapping.getChildMappings())
			{
				aColumns.append(rStorage.getChildCountColumn(rChildMapping));
				aColumns.append(',');
			}
		}

		aColumns.setLength(aColumns.length() - 1);

		return aColumns.toString();
	}

	/***************************************
	 * Appends the strings that represent the SQL expression for an attribute
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param rMapping   The mapping of the object to parse the query for
	 * @param sAttribute The attribute descriptor
	 * @param pValue     The predicate for the attribute value
	 * @param rResult    The string builder to append the created string to
	 */
	void parseAttributePredicate(StorageMapping<?, ?, ?> rMapping,
								 String					 sAttribute,
								 Predicate<?>			 pValue,
								 StringBuilder			 rResult)
	{
		if (pValue instanceof QueryPredicate<?>)
		{
			parseDetailQuery(rMapping,
							 sAttribute,
							 (QueryPredicate<?>) pValue,
							 rResult);
		}
		else
		{
			parseCriteria(rMapping, sAttribute, pValue, rResult);
		}
	}

	/***************************************
	 * Parses a comparison predicate into the corresponding SQL expression and
	 * appends it to a string builder.
	 *
	 * @param rComparison The comparison predicate to parse
	 * @param sAttribute  The attribute to apply the comparison to
	 * @param rResult     The string builder to append the created string to
	 * @param bNegate     TRUE if the expression shall be negated
	 */
	void parseComparison(Comparison<?, ?> rComparison,
						 String			  sAttribute,
						 StringBuilder    rResult,
						 boolean		  bNegate)
	{
		Object rCompareValue = rComparison.getRightValue();
		String sPlaceholders = getComparisonPlaceholders(rCompareValue);

		aCompareValues.add(rCompareValue);

		if (rComparison instanceof SqlExpressionFormat)
		{
			String sExpression =
				((SqlExpressionFormat) rComparison).format(rStorage,
														   rComparison,
														   sAttribute,
														   sPlaceholders,
														   bNegate);

			rResult.append(sExpression);
		}
		else
		{
			rResult.append(sAttribute).append(' ');

			if (rComparison instanceof EqualTo<?>)
			{
				if (rCompareValue != null)
				{
					rResult.append(bNegate ? "<>" : "=");
				}
				else
				{
					rResult.append(bNegate ? "IS NOT NULL" : "IS NULL");
					sPlaceholders = null;
				}
			}
			else if (rComparison instanceof ElementOf<?>)
			{
				rResult.append(bNegate ? "NOT IN" : "IN");
			}
			else if (rComparison instanceof LessThan<?> ||
					 rComparison instanceof LessOrEqual<?>)
			{
				if (bNegate)
				{
					rResult.append('>');

					if (rComparison instanceof LessThan<?>)
					{
						rResult.append('=');
					}
				}
				else
				{
					rResult.append('<');

					if (rComparison instanceof LessOrEqual<?>)
					{
						rResult.append('=');
					}
				}
			}
			else if (rComparison instanceof GreaterThan<?> ||
					 rComparison instanceof GreaterOrEqual<?>)
			{
				if (bNegate)
				{
					rResult.append('<');

					if (rComparison instanceof GreaterThan<?>)
					{
						rResult.append('=');
					}
				}
				else
				{
					rResult.append('>');

					if (rComparison instanceof GreaterOrEqual<?>)
					{
						rResult.append('=');
					}
				}
			}
			else
			{
				throw new IllegalArgumentException("Unsupported comparison: " +
												   rComparison);
			}

			if (sPlaceholders != null)
			{
				rResult.append(' ').append(sPlaceholders);
			}
		}
	}

	/***************************************
	 * Recursively parses a query criteria predicate for the given storage
	 * mapping and appends the corresponding SQL expressions to the given string
	 * builder.
	 *
	 * @param  rMapping   The mapping of the object to parse the query for
	 * @param  sAttribute The attribute to parse the criteria for or NULL if an
	 *                    attribute is not net known
	 * @param  pCriteria  The query predicate to parse
	 * @param  rResult    The string builder to append the created string to
	 *
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseCriteria(StorageMapping<?, ?, ?> rMapping,
						  String				  sAttribute,
						  Predicate<?>			  pCriteria,
						  StringBuilder			  rResult)
	{
		boolean bNegate = false;
		boolean bValid  = true;

		if (pCriteria instanceof Predicates.Not<?>)
		{
			pCriteria = ((Predicates.Not<?>) pCriteria).getPredicate();
			bNegate   = true;

			rResult.append(SQL_NEGATION);
		}

		if (pCriteria instanceof PredicateJoin<?>)
		{
			parseJoin(rMapping, (PredicateJoin<?>) pCriteria, rResult);
		}
		else if (pCriteria instanceof ElementPredicate<?, ?>)
		{
			ElementPredicate<?, ?> pElement =
				(ElementPredicate<?, ?>) pCriteria;

			bValid = parseElementPredicate(rMapping, pElement, rResult);
		}
		else if (pCriteria instanceof FunctionPredicate<?, ?>)
		{
			FunctionPredicate<?, ?> pFunction =
				(FunctionPredicate<?, ?>) pCriteria;

			bValid = parseFunctionPredicate(rMapping, pFunction, rResult);
		}
		else if (pCriteria instanceof Comparison<?, ?>)
		{
			if (bNegate)
			{
				// negations will be handled by parseComparison
				rResult.setLength(rResult.length() - SQL_NEGATION.length());
			}

			parseComparison((Comparison<?, ?>) pCriteria,
							sAttribute,
							rResult,
							bNegate);
		}
		else
		{
			throw new IllegalArgumentException("Unsupported query predicate: " +
											   pCriteria);
		}

		return bValid;
	}

	/***************************************
	 * Parses a sub-query on a certain attribute or child of the storage mapping
	 * that is evaluated by this query. The descriptor of the queried attribute
	 * is of type object instead of relatable because certain element predicates
	 * like {@link ReadField} only contain a field name instead of a relatable
	 * descriptor object.
	 *
	 * @param rMainMapping The storage mapping of the current parent element
	 * @param sAttribute   The attribute that is the target of the sub-query
	 * @param pDetail      A {@link QueryPredicate} defining the sub-query
	 * @param rResult      The string builder to append the sub-query string to
	 */
	void parseDetailQuery(StorageMapping<?, ?, ?> rMainMapping,
						  String				  sAttribute,
						  QueryPredicate<?>		  pDetail,
						  StringBuilder			  rResult)
	{
		StorageMapping<?, ?, ?> rDetailMapping =
			StorageManager.getMapping(pDetail.getQueryType());

		Relatable rParentAttr = rDetailMapping.getParentAttribute(rMainMapping);
		String    sChildTable = rStorage.getSqlName(rDetailMapping, true);
		String    sMainAttr;
		String    sDetailAttr;

		if (rParentAttr != null)
		{
			// for a parent-child relation, the query must be
			// SELECT ... FROM <parent> WHERE <parent-ID>
			//	 IN (SELECT <parent-ID> FROM <child> WHERE <criteria>)
			sMainAttr   =
				rStorage.getSqlName(rMainMapping.getIdAttribute(), true);
			sDetailAttr = rStorage.getSqlName(rParentAttr, true);
		}
		else
		{
			// for an object reference relation, the query must be
			// SELECT ... FROM <main> WHERE <main-attr>
			//	 IN (SELECT id FROM <detail> WHERE <criteria>)
			Function<?, ?> fDetailAttr = pDetail.get(STORAGE_FUNCTION);

			sMainAttr = sAttribute;

			if (fDetailAttr != null)
			{
				sDetailAttr = parseFunction(fDetailAttr);

				// remove compare value that has been added by parseFunction()
				aCompareAttributes.remove(aCompareAttributes.size() - 1);
			}
			else
			{
				sDetailAttr =
					rStorage.getSqlName(rDetailMapping.getIdAttribute(), true);
			}
		}

		rResult.append(sMainAttr);
		rResult.append(" IN (");
		rResult.append(formatStatement(SELECT_TEMPLATE,
									   sDetailAttr,
									   sChildTable));
		rResult.append(" WHERE ");
		parseCriteria(rDetailMapping, null, pDetail.getCriteria(), rResult);
		rResult.append(')');
	}

	/***************************************
	 * Appends the strings that represent the SQL expression for an element
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param  rMapping The mapping of the object to parse the query for
	 * @param  pElement The element predicate to apply
	 * @param  rResult  The string builder to append the created string to
	 *
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseElementPredicate(StorageMapping<?, ?, ?> rMapping,
								  ElementPredicate<?, ?>  pElement,
								  StringBuilder			  rResult)
	{
		if (pElement.hasRelation(SORT_ASCENDING))
		{
			aSortPredicates.add(pElement);
		}

		Predicate<?> pValue = pElement.getPredicate();

		boolean bHasCriteria = (pValue != Predicates.alwaysTrue());

		// value predicate alwaysTRUE means that element predicate exists only
		// to set the sort order, therefore we can ignore it
		if (bHasCriteria)
		{
			String sColumn =
				getColumnName(pElement.getElementDescriptor(),
							  bHasCriteria &&
							  !(pValue instanceof QueryPredicate));

			parseAttributePredicate(rMapping, sColumn, pValue, rResult);
		}

		return bHasCriteria;
	}

	/***************************************
	 * Parses a function and returns a string with the corresponding SQL
	 * function call. The result contains a string format placeholder (%s) that
	 * will be replaced with the name of the attribute to which the function is
	 * to be applied.
	 *
	 * @param  fFunction The function to parse
	 *
	 * @return The corresponding SQL function call
	 *
	 * @throws IllegalArgumentException If the function is not supported
	 */
	@SuppressWarnings("boxing")
	String parseFunction(Function<?, ?> fFunction)
	{
		String sSqlFunction = null;

		if (fFunction == StringFunctions.toLowerCase())
		{
			sSqlFunction = "LOWER(%s)";
		}
		else if (fFunction == StringFunctions.toUpperCase())
		{
			sSqlFunction = "UPPER(%s)";
		}
		else if (fFunction instanceof Cast)
		{
			Class<?> rCastType = ((Cast<?, ?>) fFunction).getRightValue();

			sSqlFunction =
				String.format("CAST(%s as %s)",
							  "%s",
							  rStorage.mapSqlDatatype(rCastType));
		}
		else if (fFunction instanceof GetSubstring)
		{
			GetSubstring fSubstring = (GetSubstring) fFunction;

			int nBegin = fSubstring.getBeginIndex() + 1;
			int nEnd   = fSubstring.getEndIndex() + 1;

			if (nEnd == 0)
			{
				sSqlFunction = String.format("SUBSTRING(%s,%d)", "%s", nBegin);
			}
			else
			{
				sSqlFunction =
					String.format("SUBSTRING(%s,%d,%d)", "%s", nBegin, nEnd);
			}
		}
		else if (fFunction instanceof FunctionChain)
		{
			FunctionChain<?, ?, ?> fChain = (FunctionChain<?, ?, ?>) fFunction;

			sSqlFunction =
				String.format(parseFunction(fChain.getOuter()),
							  parseFunction(fChain.getInner()));
		}
		else if (fFunction instanceof ElementAccess)
		{
			sSqlFunction =
				getColumnName(((ElementAccess<?>) fFunction)
							  .getElementDescriptor(),
							  true);
		}
		else
		{
			sSqlFunction = getColumnName(fFunction, true);
		}

		return sSqlFunction;
	}

	/***************************************
	 * Appends the strings that represent the SQL expression for an element
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param  rMapping  The mapping of the object to parse the query for
	 * @param  pFunction pElement The element predicate to apply
	 * @param  rResult   The string builder to append the created string to
	 *
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseFunctionPredicate(StorageMapping<?, ?, ?> rMapping,
								   FunctionPredicate<?, ?> pFunction,
								   StringBuilder		   rResult)
	{
		Function<?, ?> fFunction = pFunction.getFunction();

		if (fFunction instanceof FunctionChain<?, ?, ?>)
		{
			FunctionChain<?, ?, ?> fChain = (FunctionChain<?, ?, ?>) fFunction;

			parseAttributePredicate(rMapping,
									parseFunction(fChain),
									pFunction.getPredicate(),
									rResult);
		}
		else
		{
			throw new IllegalArgumentException("Unparseable function predicate: " +
											   pFunction);
		}

		return true;
	}

	/***************************************
	 * Appends the strings that represent a predicate join to a string builder.
	 *
	 * @param  rMapping The mapping of the object to parse the query for
	 * @param  pJoin    The predicate join to apply
	 * @param  rResult  The string builder to append the created string to
	 *
	 * @return TRUE if the resulting join predicate is valid to be used in
	 *         further joins, FALSE if not
	 */
	boolean parseJoin(StorageMapping<?, ?, ?> rMapping,
					  PredicateJoin<?>		  pJoin,
					  StringBuilder			  rResult)
	{
		StringBuilder aLeft  = new StringBuilder();
		StringBuilder aRight = new StringBuilder();

		boolean bLeftValid  =
			parseCriteria(rMapping, null, pJoin.getLeft(), aLeft);
		boolean bRightValid =
			parseCriteria(rMapping, null, pJoin.getRight(), aRight);

		bLeftValid  = bLeftValid && aLeft.length() > 0;
		bRightValid = bRightValid && aRight.length() > 0;

		boolean bBothValid = bLeftValid && bRightValid;

		if (bBothValid)
		{
			rResult.append('(');
		}

		rResult.append(aLeft);

		if (bBothValid)
		{
			if (pJoin instanceof Predicates.And<?>)
			{
				rResult.append(" AND ");
			}
			else if (pJoin instanceof Predicates.Or<?>)
			{
				rResult.append(" OR ");
			}
			else
			{
				throw new IllegalArgumentException("Unsupported predicate join: " +
												   pJoin);
			}
		}

		rResult.append(aRight);

		if (bBothValid)
		{
			rResult.append(')');
		}

		return bLeftValid || bRightValid;
	}

	/***************************************
	 * Parses a query criteria predicate for a certain storage mapping and
	 * returns a string containing the corresponding SQL WHERE clause.
	 *
	 * @param  rMapping  The storage mapping
	 * @param  pCriteria The predicate containing the query criteria
	 *
	 * @return The resulting WHERE clause or an empty string if no criteria are
	 *         defined
	 */
	String parseQueryCriteria(
		StorageMapping<?, ?, ?> rMapping,
		Predicate<?>			pCriteria)
	{
		StringBuilder aResult = new StringBuilder();

		aSortPredicates.clear();

		if (pCriteria != null)
		{
			parseCriteria(rMapping, null, pCriteria, aResult);

			if (aResult.length() > 0)
			{
				aResult.insert(0, " WHERE ");
			}
		}

		return aResult.toString();
	}

	/***************************************
	 * Prepares a new JDBC statement for this query. The criteria string
	 * argument may contain both a search criteria and an ordering part.
	 * Therefore it must also contain the corresponding keywords "WHERE" and an
	 * "ORDER BY" for each of these parts.
	 *
	 * @param  sAdditionalExpressions An optional string with additional
	 *                                expressions to append to the SQL statement
	 *                                (empty for none)
	 *
	 * @return The prepared statement for this query
	 *
	 * @throws StorageException If preparing the statement fails
	 */
	PreparedStatement prepareQueryStatement(String sAdditionalExpressions)
		throws StorageException
	{
		try
		{
			String sSql =
				formatStatement(SELECT_TEMPLATE,
								getColumnList(rMapping),
								rStorage.getSqlName(rMapping, true));

			sSql += sQueryCriteria + sOrderCriteria + sAdditionalExpressions;

			Log.debug("Query: " + sSql);

			Connection		  rConnection = rStorage.getConnection();
			PreparedStatement aStatement;

			if (rConnection.getMetaData()
				.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE))
			{
				aStatement =
					rConnection.prepareStatement(sSql,
												 ResultSet.TYPE_SCROLL_INSENSITIVE,
												 ResultSet.CONCUR_READ_ONLY);
			}
			else
			{
				aStatement = rConnection.prepareStatement(sSql);
			}

			return aStatement;
		}
		catch (SQLException e)
		{
			throw new StorageException(e);
		}
	}

	/***************************************
	 * Sets the compare value parameters on a query statement.
	 *
	 * @param  rStatement The statement to set the parameters on
	 *
	 * @return The index of the next statement parameter to set
	 *
	 * @throws SQLException     If setting a parameter fails
	 * @throws StorageException If mapping a value fails
	 */
	int setQueryParameters(PreparedStatement rStatement) throws SQLException,
																StorageException
	{
		assert aCompareAttributes.size() == aCompareValues.size();

		int nCount = aCompareValues.size();
		int nIndex = 1;

		for (int i = 0; i < nCount; i++)
		{
			Object rAttribute = aCompareAttributes.get(i);
			Object rValue     = aCompareValues.get(i);

			if (rValue instanceof Collection<?>)
			{
				for (Object rElement : (Collection<?>) rValue)
				{
					rStatement.setObject(nIndex++,
										 rStorage.mapValue(rMapping,
														   rAttribute,
														   rElement));
				}
			}
			else if (rValue != null) // NULL will be mapped directly to IS NULL
			{
				rStatement.setObject(nIndex++,
									 rStorage.mapValue(rMapping,
													   rAttribute,
													   rValue));
			}
		}

		return nIndex;
	}

	/***************************************
	 * Returns the column name for a certain element descriptor and stores the
	 * descriptor in the compare attributes if necessary.
	 *
	 * @param  rElementDescriptor The element descriptor to map
	 * @param  bIsCompareAttr     TRUE if the descriptor should be stored for
	 *                            attribute comparisons
	 *
	 * @return The column name
	 */
	private String getColumnName(
		Object  rElementDescriptor,
		boolean bIsCompareAttr)
	{
		String sAttribute = rStorage.getSqlName(rElementDescriptor, true);

		if (bIsCompareAttr)
		{
			aCompareAttributes.add(rElementDescriptor);
		}

		return sAttribute;
	}

	/***************************************
	 * Returns the placeholders for the compare values in a prepared statement
	 * ('?') and adds the corresponding compare values to the internal list.
	 *
	 * @param  rCompareValue rComparison The comparison to create the
	 *                       placeholders for
	 *
	 * @return The resulting string of comma-separated placeholders
	 */
	private String getComparisonPlaceholders(Object rCompareValue)
	{
		StringBuilder aResult = new StringBuilder();

		if (rCompareValue instanceof Collection<?>)
		{
			Collection<?> rValues =
				new ArrayList<Object>((Collection<?>) rCompareValue);

			int nCount = rValues.size();

			aResult.append('(');

			if (nCount > 0)
			{
				while (nCount-- > 1)
				{
					aResult.append("?,");
				}

				aResult.append("?");
			}

			aResult.append(")");
		}
		else
		{
			aResult.append('?');
		}

		return aResult.toString();
	}

	/***************************************
	 * Executes a query statement that returns an integer value.
	 *
	 * @param  sSql        The SQL statement of the query which must yield an
	 *                     int value
	 * @param  rParameters Optional additional statement parameter values
	 *
	 * @return The integer result of the query or -1 if not supported by the
	 *         implementation
	 *
	 * @throws StorageException If executing the query fails
	 */
	private int queryInteger(String sSql, Object... rParameters)
		throws StorageException
	{
		int nCount;

		try (PreparedStatement aStatement =
			 rStorage.getConnection().prepareStatement(sSql))
		{
			int nIndex = setQueryParameters(aStatement);

			if (rParameters != null && rParameters.length > 0)
			{
				for (Object rParam : rParameters)
				{
					aStatement.setObject(nIndex++, rParam);
				}
			}

			ResultSet rResult = aStatement.executeQuery();

			rResult.next();
			nCount = rResult.getInt(1);
		}
		catch (SQLException e)
		{
			throw new StorageException(e);
		}

		return nCount;
	}
}
