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
import de.esoco.lib.property.SortDirection;
import de.esoco.storage.Query;
import de.esoco.storage.QueryPredicate;
import de.esoco.storage.QueryResult;
import de.esoco.storage.Storage;
import de.esoco.storage.StorageException;
import de.esoco.storage.StorageManager;
import de.esoco.storage.StorageMapping;
import org.obrel.core.Relatable;
import org.obrel.core.RelatedObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import static de.esoco.storage.impl.jdbc.JdbcStorage.formatStatement;
import static org.obrel.type.MetaTypes.SORT_DIRECTION;

/**
 * A JDBC implementation of the query interface.
 *
 * @author eso
 */
public class JdbcQuery<T> extends RelatedObject implements Query<T>, Closeable {

	private static final String SELECT_TEMPLATE = "SELECT %s FROM %s";

	private static final String SQL_NEGATION = " NOT ";

	private final QueryPredicate<T> query;

	private final StorageMapping<T, Relatable, ?> mapping;

	private final String queryCriteria;

	private final String orderCriteria;

	private final JdbcStorage storage;

	private final List<Object> compareAttributes = new ArrayList<Object>();

	private final List<Object> compareValues = new ArrayList<Object>();

	private final List<ElementPredicate<?, ?>> sortPredicates =
		new ArrayList<ElementPredicate<?, ?>>();

	private JdbcQueryResult<T> currentResult;

	private PreparedStatement queryStatement;

	/**
	 * Creates a new instance that retrieves objects from a JDBC connection
	 * which match a certain query predicate.
	 *
	 * @param storage The storage to perform the query on
	 * @param query   The query predicate
	 * @throws StorageException On database or criteria parsing errors
	 */
	@SuppressWarnings("unchecked")
	JdbcQuery(JdbcStorage storage, QueryPredicate<T> query)
		throws StorageException {
		this.storage = storage;
		this.query = query;

		Class<T> type = query.getQueryType();
		Predicate<? super T> criteria = query.getCriteria();

		mapping =
			(StorageMapping<T, Relatable, ?>) StorageManager.getMapping(type);

		Predicate<T> defaultCriteria = mapping.getDefaultCriteria(type);

		if (defaultCriteria != null) {
			criteria = Predicates.and(criteria, defaultCriteria);
		}

		// parseQueryCriteria will also fill sortPredicates
		queryCriteria = parseQueryCriteria(mapping, criteria);
		orderCriteria = createOrderCriteria(sortPredicates);

		if (query.hasRelation(QUERY_OFFSET)) {
			set(QUERY_OFFSET, query.get(QUERY_OFFSET));
		}

		if (query.hasRelation(QUERY_LIMIT)) {
			set(QUERY_LIMIT, query.get(QUERY_LIMIT));
		}

		if (query.hasRelation(QUERY_DEPTH)) {
			set(QUERY_DEPTH, query.get(QUERY_DEPTH));
		} else if (criteria != null && (criteria instanceof Relatable) &&
			((Relatable) criteria).hasRelation(QUERY_DEPTH)) {
			set(QUERY_DEPTH, ((Relatable) criteria).get(QUERY_DEPTH));
		} else if (storage.hasRelation(QUERY_DEPTH)) {
			set(QUERY_DEPTH, storage.get(QUERY_DEPTH));
		}
	}

	/**
	 * Creates a query predicate for the children in a hierarchical oder a
	 * master-detail query.
	 *
	 * @param parentMapping The storage mapping of the parent object
	 * @param childMapping  The storage mapping of the child objects to query
	 * @param parentId      The ID of the parent object
	 * @param queryDepth    The maximum query depth for recursive queries
	 * @return The child query predicate
	 */
	static <T> QueryPredicate<T> createChildQueryPredicate(
		StorageMapping<?, ?, ?> parentMapping,
		StorageMapping<T, Relatable, ?> childMapping, Object parentId,
		int queryDepth) {
		Relatable parentAttr = childMapping.getParentAttribute(parentMapping);

		if (parentAttr == null) {
			throw new IllegalStateException(
				String.format("No parent attribute for %s in %s",
					parentMapping,
					childMapping));
		}

		Predicate<T> criteria =
			ifAttribute(childMapping, parentAttr, equalTo(parentId));

		// if the query is for the roots of a master-detail relationship and
		// the details also have a self-hierarchy query only the root objects
		// where the self-typed parent is NULL
		if (parentMapping != childMapping) {
			Relatable attr = childMapping.getParentAttribute(childMapping);

			if (attr != null) {
				criteria =
					criteria.and(ifAttribute(childMapping, attr, isNull()));
			}
		}

		QueryPredicate<T> query =
			new QueryPredicate<T>(childMapping.getMappedType(), criteria);

		query.set(QUERY_DEPTH, queryDepth);
		query.set(JDBC_CHILD_QUERY);

		return query;
	}

	/**
	 * @see Closeable#close()
	 */
	@Override
	public void close() {
		try {
			if (currentResult != null) {
				currentResult.close();
				currentResult = null;
			}
		} catch (Exception e) {
			Log.warn("Closing ResultSet failed", e);
		}

		try {
			if (queryStatement != null) {
				queryStatement.close();
				queryStatement = null;
			}
		} catch (SQLException e) {
			Log.error("Closing Statement failed", e);
		}
	}

	/**
	 * @see Query#execute()
	 */
	@Override
	@SuppressWarnings("boxing")
	public QueryResult<T> execute() throws StorageException {
		long start = System.currentTimeMillis();

		try {
			if (queryStatement != null) {
				queryStatement.close();
			}

			queryStatement = prepareQueryStatement();

			Log.debugf("QueryParams: %s", compareValues);
			setQueryParameters(queryStatement);

			ResultSet resultSet = queryStatement.executeQuery();
			boolean childQuery = query.hasFlag(JDBC_CHILD_QUERY) ||
				query.hasFlag(IS_CHILD_QUERY);

			checkLogLongQuery(start);

			currentResult =
				new JdbcQueryResult<T>(storage, mapping, resultSet, 0,
					childQuery);

			if (hasRelation(QUERY_DEPTH)) {
				currentResult.set(QUERY_DEPTH, get(QUERY_DEPTH));
			}

			return currentResult;
		} catch (SQLException e) {
			String message = "Query execution failed: " + query;

			Log.error(message, e);
			throw new StorageException(message, e);
		}
	}

	/**
	 * @see Query#getDistinct(Relatable)
	 */
	@Override
	public Set<Object> getDistinct(Relatable attribute)
		throws StorageException {
		String sql = formatStatement(SELECT_TEMPLATE,
			"DISTINCT " + storage.getSqlName(attribute, true),
			storage.getSqlName(mapping, true)) + queryCriteria;

		Set<Object> result = new HashSet<>();

		try (PreparedStatement statement = storage
			.getConnection()
			.prepareStatement(sql)) {
			setQueryParameters(statement);

			ResultSet resultSet = statement.executeQuery();

			while (resultSet.next()) {
				Object value = resultSet.getObject(1);

				value = mapping.checkAttributeValue(attribute, value);
				result.add(value);
			}
		} catch (SQLException e) {
			throw new StorageException(e);
		}

		return result;
	}

	/**
	 * @see Query#getQueryPredicate()
	 */
	@Override
	public QueryPredicate<T> getQueryPredicate() {
		return query;
	}

	/**
	 * @see Query#getStorage()
	 */
	@Override
	public Storage getStorage() {
		return storage;
	}

	/**
	 * @see Query#positionOf(Object)
	 */
	@Override
	public int positionOf(Object id) {
		String idAttr = storage.getSqlName(mapping.getIdAttribute(), true);
		String criteria = "";
		String order = "";

		if (queryCriteria != null && queryCriteria.length() > 0) {
			criteria = queryCriteria;
		}

		if (orderCriteria != null && orderCriteria.length() > 0) {
			order = orderCriteria;
		}

		String sql = formatStatement("SELECT row FROM (SELECT row_number() " +
				"OVER(%s) as row, %s FROM %s %s) AS rownums WHERE %s = ?",
			order,
			idAttr, storage.getSqlName(mapping, true), criteria, idAttr);

		try {
			return queryInteger(sql, id) - 1;
		} catch (StorageException e) {
			Log.debug("Database doesn't support row_number() function", e);

			return -1;
		}
	}

	/**
	 * @see Query#size()
	 */
	@Override
	public int size() throws StorageException {
		String sql = formatStatement(SELECT_TEMPLATE, "COUNT(*)",
			storage.getSqlName(mapping, true)) + queryCriteria;

		return queryInteger(sql);
	}

	/**
	 * Creates the SQL ORDER BY clause from a list of predicates.
	 *
	 * @param sortPredicates The element predicates to create the order clause
	 *                       from
	 * @return The complete ORDER BY clause or an empty string if no sort
	 * criteria are defined
	 */
	String createOrderCriteria(List<ElementPredicate<?, ?>> sortPredicates) {
		String result = "";

		if (sortPredicates.size() > 0) {
			StringBuilder criteria = new StringBuilder(" ORDER BY ");

			for (ElementPredicate<?, ?> predicate : sortPredicates) {
				Object attr = predicate.getElementDescriptor();

				criteria.append(storage.getSqlName(attr, true));

				if (predicate.get(SORT_DIRECTION) == SortDirection.DESCENDING) {
					criteria.append(" DESC");
				}

				criteria.append(',');
			}

			// remove the trailing comma
			criteria.setLength(criteria.length() - 1);
			result = criteria.toString();
		}

		return result;
	}

	/**
	 * Returns a comma separated list of column names for a certain storage
	 * mapping.
	 *
	 * @param mapping The storage mapping
	 * @return The column list for the given mapping
	 */
	String getColumnList(StorageMapping<?, ?, ?> mapping) {
		Collection<?> attributes = mapping.getAttributes();
		StringBuilder columns = new StringBuilder(attributes.size() * 10);

		for (Object attr : attributes) {
			columns.append(storage.getSqlName(attr, true)).append(',');
		}

		if (!mapping.hasFlag(SQL_DISABLE_CHILD_COUNTS)) {
			for (StorageMapping<?, ?, ?> childMapping :
				mapping.getChildMappings()) {
				columns.append(storage.getChildCountColumn(childMapping));
				columns.append(',');
			}
		}

		columns.setLength(columns.length() - 1);

		return columns.toString();
	}

	/**
	 * Appends the strings that represent the SQL expression for an attribute
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param mapping   The mapping of the object to parse the query for
	 * @param attribute The attribute descriptor
	 * @param value     The predicate for the attribute value
	 * @param result    The string builder to append the created string to
	 */
	void parseAttributePredicate(StorageMapping<?, ?, ?> mapping,
		String attribute, Predicate<?> value, StringBuilder result) {
		if (value instanceof QueryPredicate<?>) {
			parseDetailQuery(mapping, attribute, (QueryPredicate<?>) value,
				result);
		} else {
			parseCriteria(mapping, attribute, value, result);
		}
	}

	/**
	 * Parses a comparison predicate into the corresponding SQL expression and
	 * appends it to a string builder.
	 *
	 * @param comparison The comparison predicate to parse
	 * @param attribute  The attribute to apply the comparison to
	 * @param result     The string builder to append the created string to
	 * @param negate     TRUE if the expression shall be negated
	 */
	void parseComparison(Comparison<?, ?> comparison, String attribute,
		StringBuilder result, boolean negate) {
		Object compareValue = comparison.getRightValue();
		String placeholders = getComparisonPlaceholders(compareValue);

		compareValues.add(compareValue);

		if (comparison instanceof SqlExpressionFormat) {
			String expression =
				((SqlExpressionFormat) comparison).format(storage, comparison,
					attribute, placeholders, negate);

			result.append(expression);
		} else {
			result.append(attribute).append(' ');

			if (comparison instanceof EqualTo<?>) {
				if (compareValue != null) {
					result.append(negate ? "<>" : "=");
				} else {
					result.append(negate ? "IS NOT NULL" : "IS NULL");
					placeholders = null;
				}
			} else if (comparison instanceof ElementOf<?>) {
				result.append(negate ? "NOT IN" : "IN");
			} else if (comparison instanceof LessThan<?> ||
				comparison instanceof LessOrEqual<?>) {
				if (negate) {
					result.append('>');

					if (comparison instanceof LessThan<?>) {
						result.append('=');
					}
				} else {
					result.append('<');

					if (comparison instanceof LessOrEqual<?>) {
						result.append('=');
					}
				}
			} else if (comparison instanceof GreaterThan<?> ||
				comparison instanceof GreaterOrEqual<?>) {
				if (negate) {
					result.append('<');

					if (comparison instanceof GreaterThan<?>) {
						result.append('=');
					}
				} else {
					result.append('>');

					if (comparison instanceof GreaterOrEqual<?>) {
						result.append('=');
					}
				}
			} else {
				throw new IllegalArgumentException(
					"Unsupported comparison: " + comparison);
			}

			if (placeholders != null) {
				result.append(' ').append(placeholders);
			}
		}
	}

	/**
	 * Recursively parses a query criteria predicate for the given storage
	 * mapping and appends the corresponding SQL expressions to the given
	 * string
	 * builder.
	 *
	 * @param mapping   The mapping of the object to parse the query for
	 * @param attribute The attribute to parse the criteria for or NULL if an
	 *                  attribute is not net known
	 * @param criteria  The query predicate to parse
	 * @param result    The string builder to append the created string to
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseCriteria(StorageMapping<?, ?, ?> mapping, String attribute,
		Predicate<?> criteria, StringBuilder result) {
		boolean negate = false;
		boolean valid = true;

		if (criteria instanceof Predicates.Not<?>) {
			criteria = ((Predicates.Not<?>) criteria).getInvertedPredicate();
			negate = true;

			result.append(SQL_NEGATION);
		}

		if (criteria instanceof PredicateJoin<?>) {
			parseJoin(mapping, (PredicateJoin<?>) criteria, result);
		} else if (criteria instanceof ElementPredicate<?, ?>) {
			ElementPredicate<?, ?> element = (ElementPredicate<?, ?>) criteria;

			valid = parseElementPredicate(mapping, element, result);
		} else if (criteria instanceof FunctionPredicate<?, ?>) {
			FunctionPredicate<?, ?> function =
				(FunctionPredicate<?, ?>) criteria;

			valid = parseFunctionPredicate(mapping, function, result);
		} else if (criteria instanceof Comparison<?, ?>) {
			if (negate) {
				// negations will be handled by parseComparison
				result.setLength(result.length() - SQL_NEGATION.length());
			}

			parseComparison((Comparison<?, ?>) criteria, attribute, result,
				negate);
		} else {
			throw new IllegalArgumentException(
				"Unsupported query predicate: " + criteria);
		}

		return valid;
	}

	/**
	 * Parses a sub-query on a certain attribute or child of the storage
	 * mapping
	 * that is evaluated by this query. The descriptor of the queried attribute
	 * is of type object instead of relatable because certain element
	 * predicates
	 * like {@link ReadField} only contain a field name instead of a relatable
	 * descriptor object.
	 *
	 * @param mainMapping The storage mapping of the current parent element
	 * @param attribute   The attribute that is the target of the sub-query
	 * @param detail      A {@link QueryPredicate} defining the sub-query
	 * @param result      The string builder to append the sub-query string to
	 */
	void parseDetailQuery(StorageMapping<?, ?, ?> mainMapping,
		String attribute,
		QueryPredicate<?> detail, StringBuilder result) {
		StorageMapping<?, ?, ?> detailMapping =
			StorageManager.getMapping(detail.getQueryType());

		Relatable parentAttr = detailMapping.getParentAttribute(mainMapping);
		String childTable = storage.getSqlName(detailMapping, true);
		String mainAttr;
		String detailAttr;

		if (parentAttr != null) {
			// for a parent-child relation, the query must be
			// SELECT ... FROM <parent> WHERE <parent-ID>
			//	 IN (SELECT <parent-ID> FROM <child> WHERE <criteria>)
			mainAttr = storage.getSqlName(mainMapping.getIdAttribute(), true);
			detailAttr = storage.getSqlName(parentAttr, true);
		} else {
			// for an object reference relation, the query must be
			// SELECT ... FROM <main> WHERE <main-attr>
			//	 IN (SELECT id FROM <detail> WHERE <criteria>)
			Function<?, ?> getDetailAttr = detail.get(STORAGE_FUNCTION);

			mainAttr = attribute;

			if (getDetailAttr != null) {
				detailAttr = parseFunction(getDetailAttr);

				// remove compare value that has been added by parseFunction()
				compareAttributes.remove(compareAttributes.size() - 1);
			} else {
				detailAttr =
					storage.getSqlName(detailMapping.getIdAttribute(), true);
			}
		}

		result.append(mainAttr);
		result.append(" IN (");
		result.append(formatStatement(SELECT_TEMPLATE, detailAttr,
			childTable));
		result.append(" WHERE ");
		parseCriteria(detailMapping, null, detail.getCriteria(), result);
		result.append(')');
	}

	/**
	 * Appends the strings that represent the SQL expression for an element
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param mapping The mapping of the object to parse the query for
	 * @param element The element predicate to apply
	 * @param result  The string builder to append the created string to
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseElementPredicate(StorageMapping<?, ?, ?> mapping,
		ElementPredicate<?, ?> element, StringBuilder result) {
		if (element.get(SORT_DIRECTION) != null) {
			sortPredicates.add(element);
		}

		Predicate<?> value = element.getPredicate();

		boolean hasCriteria = (value != Predicates.alwaysTrue());

		// value predicate alwaysTRUE means that element predicate exists only
		// to set the sort order, therefore we can ignore it
		if (hasCriteria) {
			String column = getColumnName(element.getElementDescriptor(),
				hasCriteria && !(value instanceof QueryPredicate));

			parseAttributePredicate(mapping, column, value, result);
		}

		return hasCriteria;
	}

	/**
	 * Parses a function and returns a string with the corresponding SQL
	 * function call. The result contains a string format placeholder (%s) that
	 * will be replaced with the name of the attribute to which the function is
	 * to be applied.
	 *
	 * @param function The function to parse
	 * @return The corresponding SQL function call
	 * @throws IllegalArgumentException If the function is not supported
	 */
	@SuppressWarnings("boxing")
	String parseFunction(Function<?, ?> function) {
		String sqlFunction = null;

		if (function == StringFunctions.toLowerCase()) {
			sqlFunction = "LOWER(%s)";
		} else if (function == StringFunctions.toUpperCase()) {
			sqlFunction = "UPPER(%s)";
		} else if (function instanceof Cast) {
			Class<?> castType = ((Cast<?, ?>) function).getRightValue();

			sqlFunction = String.format("CAST(%s as %s)", "%s",
				storage.mapSqlDatatype(castType));
		} else if (function instanceof GetSubstring) {
			GetSubstring substring = (GetSubstring) function;

			int begin = substring.getBeginIndex() + 1;
			int end = substring.getEndIndex() + 1;

			if (end == 0) {
				sqlFunction = String.format("SUBSTRING(%s,%d)", "%s", begin);
			} else {
				sqlFunction =
					String.format("SUBSTRING(%s,%d,%d)", "%s", begin, end);
			}
		} else if (function instanceof FunctionChain) {
			FunctionChain<?, ?, ?> chain = (FunctionChain<?, ?, ?>) function;

			sqlFunction = String.format(parseFunction(chain.getOuter()),
				parseFunction(chain.getInner()));
		} else if (function instanceof ElementAccess) {
			sqlFunction = getColumnName(
				((ElementAccess<?>) function).getElementDescriptor(), true);
		} else {
			sqlFunction = getColumnName(function, true);
		}

		return sqlFunction;
	}

	/**
	 * Appends the strings that represent the SQL expression for an element
	 * predicate to a string builder. The return value indicates whether the
	 * element predicate is valid to be included in any surrounding join
	 * predicate or if it only defines a sort criterion.
	 *
	 * @param mapping           The mapping of the object to parse the query
	 *                          for
	 * @param functionPredicate The function predicate to apply
	 * @param result            The string builder to append the created string
	 *                          to
	 * @return TRUE if the predicate is valid to be used in joins, FALSE if not
	 */
	boolean parseFunctionPredicate(StorageMapping<?, ?, ?> mapping,
		FunctionPredicate<?, ?> functionPredicate, StringBuilder result) {
		Function<?, ?> function = functionPredicate.getFunction();

		if (function instanceof FunctionChain<?, ?, ?>) {
			FunctionChain<?, ?, ?> chain = (FunctionChain<?, ?, ?>) function;

			parseAttributePredicate(mapping, parseFunction(chain),
				functionPredicate.getPredicate(), result);
		} else {
			throw new IllegalArgumentException(
				"Unparseable function predicate: " + function);
		}

		return true;
	}

	/**
	 * Appends the strings that represent a predicate join to a string builder.
	 *
	 * @param mapping The mapping of the object to parse the query for
	 * @param join    The predicate join to apply
	 * @param result  The string builder to append the created string to
	 * @return TRUE if the resulting join predicate is valid to be used in
	 * further joins, FALSE if not
	 */
	boolean parseJoin(StorageMapping<?, ?, ?> mapping, PredicateJoin<?> join,
		StringBuilder result) {
		StringBuilder left = new StringBuilder();
		StringBuilder right = new StringBuilder();

		boolean leftValid = parseCriteria(mapping, null, join.getLeft(), left);
		boolean rightValid =
			parseCriteria(mapping, null, join.getRight(), right);

		leftValid = leftValid && left.length() > 0;
		rightValid = rightValid && right.length() > 0;

		boolean bothValid = leftValid && rightValid;

		if (bothValid) {
			result.append('(');
		}

		result.append(left);

		if (bothValid) {
			if (join instanceof Predicates.And<?>) {
				result.append(" AND ");
			} else if (join instanceof Predicates.Or<?>) {
				result.append(" OR ");
			} else {
				throw new IllegalArgumentException(
					"Unsupported predicate join: " + join);
			}
		}

		result.append(right);

		if (bothValid) {
			result.append(')');
		}

		return leftValid || rightValid;
	}

	/**
	 * Parses a query criteria predicate for a certain storage mapping and
	 * returns a string containing the corresponding SQL WHERE clause.
	 *
	 * @param mapping  The storage mapping
	 * @param criteria The predicate containing the query criteria
	 * @return The resulting WHERE clause or an empty string if no criteria are
	 * defined
	 */
	String parseQueryCriteria(StorageMapping<?, ?, ?> mapping,
		Predicate<?> criteria) {
		StringBuilder result = new StringBuilder();

		sortPredicates.clear();

		if (criteria != null) {
			parseCriteria(mapping, null, criteria, result);

			if (result.length() > 0) {
				result.insert(0, " WHERE ");
			}
		}

		return result.toString();
	}

	/**
	 * Prepares a new JDBC statement for this query. The criteria string
	 * argument may contain both a search criteria and an ordering part.
	 * Therefore it must also contain the corresponding keywords "WHERE" and an
	 * "ORDER BY" for each of these parts.
	 *
	 * @return The prepared statement for this query
	 * @throws StorageException If preparing the statement fails
	 */
	@SuppressWarnings("boxing")
	PreparedStatement prepareQueryStatement() throws StorageException {
		try {
			int offset = get(QUERY_OFFSET);
			int limit = get(QUERY_LIMIT);

			StringBuilder sql = new StringBuilder(
				formatStatement(SELECT_TEMPLATE, getColumnList(mapping),
					storage.getSqlName(mapping, true)));

			sql.append(queryCriteria);
			sql.append(orderCriteria);

			if (offset > 0) {
				sql.append(" OFFSET ").append(offset);
			}

			if (limit > 0) {
				sql.append(" LIMIT ").append(limit);
			}

			Log.debug("Query: " + sql);

			Connection connection = storage.getConnection();
			PreparedStatement statement;

			if (connection
				.getMetaData()
				.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
				statement = connection.prepareStatement(sql.toString(),
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
			} else {
				statement = connection.prepareStatement(sql.toString());
			}

			return statement;
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	/**
	 * Sets the compare value parameters on a query statement.
	 *
	 * @param statement The statement to set the parameters on
	 * @return The index of the next statement parameter to set
	 * @throws SQLException     If setting a parameter fails
	 * @throws StorageException If mapping a value fails
	 */
	int setQueryParameters(PreparedStatement statement)
		throws SQLException, StorageException {
		assert compareAttributes.size() == compareValues.size();

		int count = compareValues.size();
		int index = 1;

		for (int i = 0; i < count; i++) {
			Object attribute = compareAttributes.get(i);
			Object value = compareValues.get(i);

			if (value instanceof Collection<?>) {
				for (Object element : (Collection<?>) value) {
					statement.setObject(index++,
						storage.mapValue(mapping, attribute, element));
				}
			} else if (value != null) // NULL will be mapped directly to IS
			// NULL
			{
				statement.setObject(index++,
					storage.mapValue(mapping, attribute, value));
			}
		}

		return index;
	}

	/**
	 * Checks whether certain query duration thresholds have been exceeded and
	 * logs them if necessary.
	 *
	 * @param startTime The start time in milliseconds
	 */
	@SuppressWarnings("boxing")
	private void checkLogLongQuery(long startTime) {
		long duration = System.currentTimeMillis() - startTime;

		if (duration > 1000) {
			if (duration > 3000) {
				Log.warnf("Very high query time: %d.%03d for %s\n",
					duration / 1000, duration % 1000, queryStatement);
			} else {
				Log.infof("High query time: %d.%03d for %s\n", duration / 1000,
					duration % 1000, queryStatement);
			}
		}
	}

	/**
	 * Returns the column name for a certain element descriptor and stores the
	 * descriptor in the compare attributes if necessary.
	 *
	 * @param elementDescriptor The element descriptor to map
	 * @param isCompareAttr     TRUE if the descriptor should be stored for
	 *                          attribute comparisons
	 * @return The column name
	 */
	private String getColumnName(Object elementDescriptor,
		boolean isCompareAttr) {
		String attribute = storage.getSqlName(elementDescriptor, true);

		if (isCompareAttr) {
			compareAttributes.add(elementDescriptor);
		}

		return attribute;
	}

	/**
	 * Returns the placeholders for the compare values in a prepared statement
	 * ('?') and adds the corresponding compare values to the internal list.
	 *
	 * @param compareValue rComparison The comparison to create the
	 *                        placeholders
	 *                     for
	 * @return The resulting string of comma-separated placeholders
	 */
	private String getComparisonPlaceholders(Object compareValue) {
		StringBuilder result = new StringBuilder();

		if (compareValue instanceof Collection<?>) {
			Collection<?> values =
				new ArrayList<Object>((Collection<?>) compareValue);

			int count = values.size();

			result.append('(');

			if (count > 0) {
				while (count-- > 1) {
					result.append("?,");
				}

				result.append("?");
			}

			result.append(")");
		} else {
			result.append('?');
		}

		return result.toString();
	}

	/**
	 * Executes a query statement that returns an integer value.
	 *
	 * @param sql        The SQL statement of the query which must yield an int
	 *                   value
	 * @param parameters Optional additional statement parameter values
	 * @return The integer result of the query or -1 if not supported by the
	 * implementation
	 * @throws StorageException If executing the query fails
	 */
	private int queryInteger(String sql, Object... parameters)
		throws StorageException {
		int count;

		try (PreparedStatement statement = storage
			.getConnection()
			.prepareStatement(sql)) {
			int index = setQueryParameters(statement);

			if (parameters != null) {
				for (Object param : parameters) {
					statement.setObject(index++, param);
				}
			}

			ResultSet result = statement.executeQuery();

			result.next();
			count = result.getInt(1);
		} catch (SQLException e) {
			throw new StorageException(e);
		}

		return count;
	}
}
