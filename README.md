
# Aggregation Tree

This project allows to create an Aggregation Tree out of a dataset, which essentially implements a pivot functionality. Given :

1. a dataset in which each entry is composed by a set of dimensions (or label) and of a value **V** to be aggregated
2. an aggregation function **G**
3. an aggregation order (i.e., an ordered set of dimensions/labels defining an heirarchy of aggregation levels)

the Aggregation Tree allows to retrieve the aggregated value of **V** according to **G** for any possible aggregation level.

# Requirements

The project relies on Java 8, which is the only requirement to use the provided API.

# Installation

After having cloned the repository, you can run the tests and install the artifact using Maven:

	mvn clean install

# Usage

You can add the following Maven dependency to use the Aggregation Tree within your project:

    <dependency>
      <groupId>it.aggregationtree</groupId>
      <artifactId>AggregationTree</artifactId>
      <version>1.0-SNAPSHOT</version>
    </dependency>

A single API is provided through the `AggregationTree` class. 
First, it need to be initialized with the aggregation function and the aggregation order to be considered. 
The type of the values being aggregated as well as the type of the resulting aggregated values have to be provided. 
The aggregation function can be specified as a lambda expression, which needs to be compatible with the specified input/output data types. 
For example, in the following we create an `AggregationTree` whose aggregation function is a simple summation of integers, thus producing an integer as output:

	AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>
	((l) -> {
		return l.parallelStream().reduce(0, (a, b) -> a + b);
	}, "nation", "eyes", "hair");

We can now load data into the `AggregationTree`. 
The `AggregationTree `provides the `insert(Row row`) and the` load(List<Row> data)` methods that can be used for this purpose.
The `Row` class is the data type being used internally by the` AggregationTree`. 
It allows to define a row as a set of `labels` with corresponding values (labels must be of type `String`) plus a generic `value` that will be used for computing aggregations. 
The library provides a convenient class `InputLoader` that can be used to load a dataset from a JSON file. 
For example, in the following we load an example JSON file using `InputLoader` and we pass the result to the `AggregationTree`:

	InputLoader<Integer> loader = new InputLoader<Integer>();
	List<Row<Integer>> data = loader.loadFromJson(
		new File(<PATH-TO-JSON-FILE>));
	pivot.load(data);

An example valid JSON file is the following:

	[
	    {
	        "value": 168,
	        "labels": {
	            "nation": "germany",
	            "eyes": "green",
	            "hair": "brown"
	        }
	    },
	    {
	        "value": 468,
	        "labels": {
	            "nation": "germany",
	            "eyes": "dark",
	            "hair": "black"
	        }
	    },
	    {
	        "value": 148,
	        "labels": {
	            "nation": "italy",
	            "eyes": "dark",
	            "hair": "black"
	        }
	    },
	    {
	        "value": 288,
	        "labels": {
	            "nation": "france",
	            "eyes": "green",
	            "hair": "blonde"
	        }
	    },
	    {
	        "value": 906,
	        "labels": {
	            "nation": "germany",
	            "eyes": "green",
	            "hair": "red"
	        }
	    },
	    {
	        "value": 498,
	        "labels": {
	            "nation": "france",
	            "eyes": "blue",
	            "hair": "black"
	        }
	    }
	]

At this point the `AggregationTree` is loaded and we can start querying it using the `get` method implementations:

	pivot.get("germany");
	pivot.get("germany", "blue");
	pivot.get("germany", "blue", "brown");

