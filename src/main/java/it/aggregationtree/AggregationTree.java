package it.aggregationtree;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import it.aggregationtree.exceptions.TooManyLabelsInQueryException;

public class AggregationTree<V, R> {

	private Composite<V> root;
	private final LinkedHashSet<String> labelsOrder;
	private final Function<List<V>, R> aggregationFunction;

	// constructors

	public AggregationTree(Function<List<V>, R> aggregationFunction, LinkedHashSet<String> labelsOrder) {
		this.labelsOrder = labelsOrder;
		this.root = new Composite<V>("All");
		this.aggregationFunction = aggregationFunction;
	}

	public AggregationTree(List<Row<V>> data, Function<List<V>, R> aggregationFunction,
			LinkedHashSet<String> labelsOrder) {
		this.labelsOrder = labelsOrder;
		this.root = new Composite<V>("All");
		this.load(data);
		this.aggregationFunction = aggregationFunction;

	}

	public AggregationTree(Function<List<V>, R> aggregationFunction, String... labelsOrder) {
		this.labelsOrder = new LinkedHashSet<String>();
		for (int i = 0; i < labelsOrder.length; i++) {
			this.labelsOrder.add(labelsOrder[i]);
		}
		this.root = new Composite<V>("All");
		this.aggregationFunction = aggregationFunction;
	}

	public AggregationTree(List<Row<V>> data, Function<List<V>, R> aggregationFunction, String... labelsOrder) {
		this.labelsOrder = new LinkedHashSet<String>();
		for (int i = 0; i < labelsOrder.length; i++) {
			this.labelsOrder.add(labelsOrder[i]);
		}
		this.root = new Composite<V>("All");
		this.aggregationFunction = aggregationFunction;
		this.load(data);
	}

	// public methods

	/**
	 * Loads a dataset of rows into the aggregation tree.
	 * 
	 * @param data: the list of rows to be loaded into the tree.
	 */
	public void load(List<Row<V>> data) {
		for (Row<V> r : data) {
			this.insert(r);
		}
	}

	/**
	 * Insert a row into the aggregation tree. If caching of previously queried
	 * aggregations is enabled, this method invalidates the cache after having
	 * inserted the new row.
	 * 
	 * @param row: the row to be inserted.
	 */
	public void insert(Row<V> row) {
		if (row.getValue() != null) {
			if (this.verifyInsert(row.getLabels().keySet())) {
				Composite<V> currentNode = root;
				for (String label : this.labelsOrder) {
					String labelValue = row.getLabels().get(label);
					Boolean found = false;
					Iterator<Node<V>> childIter = currentNode.getChilds().iterator();
					while (childIter.hasNext() && !found) {
						Composite<V> child = (Composite<V>) childIter.next();
						if (child.getLabel().equals(labelValue)) {
							currentNode = child;
							found = true;
						}
					}

					if (!found) {
						Composite<V> child = new Composite<V>(labelValue);
						if (this.root.getCaching()) {
							child.setCaching(true);
						}
						currentNode.addChild(child);
						currentNode = child;
					}
				}
				currentNode.addChild(new Leaf<V>(row.getValue()));

				// after a new element is inserted the cache is invalidated
				this.invalidateCache();
			} else {
				throw new IllegalArgumentException(
						"The inserted row misses a value for an aggregation dimension: " + row);
			}
		} else {
			throw new IllegalArgumentException("The inserted row does not have a value: " + row);
		}
	}

	/**
	 * Retrieves the aggregated value for a specified aggregation level.
	 * 
	 * @param labelValues: A list of labels defining the aggregation level. The list
	 *        of labels must be a prefix of the aggregation order specified when
	 *        creating the AggregationTree For example, if the specified aggregation
	 *        order is "nation"->"eyes"->"hair", then correct queries are
	 *        get("germany"), get("germany","blue"), get("germany", "blue",
	 *        "black"), while not correct queries are get("blue","germany"),
	 *        get("germany", "blue", "black", "male"). When called without
	 *        specifying any label to define the aggregation level, it retrives the
	 *        aggregated value for the entire stored dataset.
	 * 
	 * @return the aggregated value for the specified aggregation level.
	 */
	public R get(String... labelValues) {
		if (labelValues.length > this.labelsOrder.size()) {
			throw new IllegalArgumentException("The provided query specifies too many labels.");
		} else {
			LinkedHashMap<String, String> query = new LinkedHashMap<String, String>();
			Iterator<String> orderIter = this.labelsOrder.iterator();
			for (int i = 0; i < labelValues.length; i++) {
				query.put(orderIter.next(), labelValues[i]);
			}
			return this.get(query);
		}

	}

	/**
	 * Retrieves the aggregated value for a specified aggregation level.
	 * 
	 * @param query: A map defining a value for each aggregating label. The keys in
	 *        the map must be a subset of the set labels specified in the
	 *        aggregation order.
	 * 
	 * @return the aggregated value for the specified aggregation level.
	 */
	public R get(LinkedHashMap<String, String> query) {
		try {
			if (this.verifyGet(query.keySet())) {
				Composite<V> currentNode = root;
				for (String label : query.keySet()) {
					Boolean found = false;
					Iterator<Node<V>> childIter = currentNode.getChilds().iterator();
					while (childIter.hasNext() && !found) {
						Composite<V> child = (Composite<V>) childIter.next();
						if (child.getLabel().equals(query.get(label))) {
							currentNode = child;
							found = true;
						}
					}
					if (!found) {
						throw new IllegalArgumentException(
								"The provided query specifies a missing label value: " + query.get(label));
					}
				}

				List<V> toAggregate = currentNode.getValue();
				return this.aggregationFunction.apply(toAggregate);

			} else {
				throw new IllegalArgumentException("The provided query is not a prefix of the ordering.");
			}
		} catch (TooManyLabelsInQueryException e) {
			throw new IllegalArgumentException("The provided query specifies too many labels.");
		}
	}

	/**
	 * Builds a tree-like representation of the AggreationTree to be printed for
	 * visualization purpose.
	 * 
	 * @return the string containing the tree-like representation of the
	 *         AggregationTree.
	 */
	public String printTree() {
		return this.root.printPretty("", true);
	}

	/**
	 * Enables and disables caching of previously queried results.
	 * 
	 * @param caching: true if caching has to be enabled, false otherwise.
	 * 
	 */
	public void setCaching(Boolean caching) {
		this.root.setCaching(caching);
	}

	// private methods

	private void invalidateCache() {
		if (this.root.getCaching()) {
			this.root.setCaching(false);
			this.root.setCaching(true);
		}
	}

	// a row being inserted must contain all the labels in the ordering
	private Boolean verifyInsert(Set<String> labels) {
		Boolean verified = true;
		Iterator<String> orderIter = this.labelsOrder.iterator();
		while (orderIter.hasNext() && verified) {
			if (!labels.contains(orderIter.next())) {
				verified = false;
			}
		}
		return verified;
	}

	// a query must be a prefix of the ordering
	private Boolean verifyGet(Set<String> query) throws TooManyLabelsInQueryException {
		if (query.size() > this.labelsOrder.size()) {
			throw new TooManyLabelsInQueryException();
		} else {
			Boolean verified = true;
			Iterator<String> orderingIter = this.labelsOrder.iterator();
			Iterator<String> queryIter = query.iterator();
			while (queryIter.hasNext() && verified) {
				if (!queryIter.next().equals(orderingIter.next())) {
					verified = false;
				}
			}
			return verified;
		}

	}
}
