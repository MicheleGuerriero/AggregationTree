package it.aggregationtree;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import it.aggregationtree.AggregationTree;
import it.aggregationtree.Row;
import it.aggregationtree.utils.InputLoader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class AggregationTreeTest {

	private static final String TEST_SAMPLE_DATASET_SRC = "src/test/resources/sample-input.json";
	private static final String TEST_INVALID_JSON_SRC = "src/test/resources/invalid-json.json";
	private static final Integer NEW_INSERTED_VALUE = 100;
	
	private static final String QUERY_MISSES_LABEL_MESSAGE = "The provided query specifies a missing label value: ";
	private static final String INSERT_MISSES_LABEL_VALUE_MESSAGE = "The inserted row misses a value for an aggregation dimension: ";
	private static final String INSERT_MISSES_VALUE_MESSAGE = "The inserted row does not have a value: ";
	private static final String TOO_MANY_LABELS_MESSAGE = "The provided query specifies too many labels.";
	private static final String PARSING_ERROR_MESSAGE = "Error parsing the input json file.";
	@Test
	public void testSampleDataset() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		// level 0 (entire dataset) aggregation
		assertEquals(pivot.get(), Integer.valueOf(8516));

		// level 1 aggregation
		assertEquals(pivot.get("germany"), Integer.valueOf(3323));
		assertEquals(pivot.get("france"), Integer.valueOf(2149));
		assertEquals(pivot.get("spain"), Integer.valueOf(2896));
		assertEquals(pivot.get("italy"), Integer.valueOf(148));

		// level 2 aggregation
		assertEquals(pivot.get("france", "blue"), Integer.valueOf(1004));
		assertEquals(pivot.get("france", "green"), Integer.valueOf(1145));

		assertEquals(pivot.get("germany", "blue"), Integer.valueOf(389));
		assertEquals(pivot.get("germany", "brown"), Integer.valueOf(753));
		assertEquals(pivot.get("germany", "dark"), Integer.valueOf(571));
		assertEquals(pivot.get("germany", "green"), Integer.valueOf(1610));

		assertEquals(pivot.get("italy", "dark"), Integer.valueOf(148));

		assertEquals(pivot.get("spain", "blue"), Integer.valueOf(852));
		assertEquals(pivot.get("spain", "brown"), Integer.valueOf(778));
		assertEquals(pivot.get("spain", "dark"), Integer.valueOf(907));
		assertEquals(pivot.get("spain", "green"), Integer.valueOf(359));

		// level 3 aggregation
		assertEquals(pivot.get("france", "blue", "black"), Integer.valueOf(1004));
		assertEquals(pivot.get("france", "green", "black"), Integer.valueOf(857));
		assertEquals(pivot.get("france", "green", "blonde"), Integer.valueOf(288));

		assertEquals(pivot.get("germany", "blue", "brown"), Integer.valueOf(389));
		assertEquals(pivot.get("germany", "brown", "red"), Integer.valueOf(753));
		assertEquals(pivot.get("germany", "dark", "black"), Integer.valueOf(468));
		assertEquals(pivot.get("germany", "dark", "brown"), Integer.valueOf(103));
		assertEquals(pivot.get("germany", "green", "brown"), Integer.valueOf(168));
		assertEquals(pivot.get("germany", "green", "red"), Integer.valueOf(1442));

		assertEquals(pivot.get("italy", "dark", "black"), Integer.valueOf(148));

		assertEquals(pivot.get("spain", "blue", "black"), Integer.valueOf(852));
		assertEquals(pivot.get("spain", "brown", "red"), Integer.valueOf(778));
		assertEquals(pivot.get("spain", "dark", "black"), Integer.valueOf(907));
		assertEquals(pivot.get("spain", "green", "brown"), Integer.valueOf(359));

	}

	@Test
	public void testEmptyDataset() {
		List<Row<Integer>> data = new ArrayList<Row<Integer>>();
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");
		try {
			pivot.get("france");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(QUERY_MISSES_LABEL_MESSAGE + "france"));
		}
	}

	@Test
	public void testMissingLabelValueInQuery() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		// missing at the first levele
		try {
			pivot.get("missingNation");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(QUERY_MISSES_LABEL_MESSAGE + "missingNation"));
		}

		// missing at the second level
		try {
			pivot.get("france", "missingEyesColor");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(QUERY_MISSES_LABEL_MESSAGE + "missingEyesColor"));
		}

		try {
			pivot.get("france", "blue", "missingHairColor");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(QUERY_MISSES_LABEL_MESSAGE + "missingHairColor"));
		}

	}

	@Test
	public void testMissingLabelInInsert() {
		List<Row<Integer>> data = new ArrayList<Row<Integer>>();
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");
		Row<Integer> toInsert = new Row<Integer>();
		toInsert.addLabel("nation", "france");
		toInsert.addLabel("eyes", "blue");
		// missing value "hair" label
		toInsert.setValue(1004);
		try {
			pivot.insert(toInsert);
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(INSERT_MISSES_LABEL_VALUE_MESSAGE + toInsert));
		}

		toInsert.addLabel("hair", "black");
		pivot.insert(toInsert);
		assertEquals(pivot.get("france", "blue", "black"), Integer.valueOf(1004));

	}

	@Test
	public void testMissingValueInInsert() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		Row<Integer> toInsert = new Row<Integer>();
		toInsert.addLabel("nation", "germany");
		toInsert.addLabel("eyes", "blue");
		toInsert.addLabel("hair", "brown");

		try {
			pivot.insert(toInsert);
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(INSERT_MISSES_VALUE_MESSAGE + toInsert));
		}
	}

	@Test
	public void testInsert() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		assertEquals(pivot.get(), Integer.valueOf(8516));
		assertEquals(pivot.get("germany"), Integer.valueOf(3323));
		assertEquals(pivot.get("germany", "blue"), Integer.valueOf(389));
		assertEquals(pivot.get("germany", "blue", "brown"), Integer.valueOf(389));

		Row<Integer> toInsert = new Row<Integer>();
		toInsert.addLabel("nation", "germany");
		toInsert.addLabel("eyes", "blue");
		toInsert.addLabel("hair", "brown");

		toInsert.setValue(NEW_INSERTED_VALUE);

		pivot.insert(toInsert);

		assertEquals(pivot.get(), Integer.valueOf(8516 + NEW_INSERTED_VALUE));
		assertEquals(pivot.get("germany"), Integer.valueOf(3323 + NEW_INSERTED_VALUE));
		assertEquals(pivot.get("germany", "blue"), Integer.valueOf(389 + NEW_INSERTED_VALUE));
		assertEquals(pivot.get("germany", "blue", "brown"), Integer.valueOf(389 + NEW_INSERTED_VALUE));
	}

	@Test
	public void testTooManyLabelsInQuery() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		assertEquals(pivot.get("france", "blue", "black"), Integer.valueOf(1004));

		try {
			pivot.get("france", "blue", "black", "tooMuch");
		} catch (IllegalArgumentException e) {
			assertThat(e.getMessage(), is(TOO_MANY_LABELS_MESSAGE));
		}
	}

	@Test
	public void testNotJsonInput() {
		try {
			InputLoader<Integer> loader = new InputLoader<Integer>();
			@SuppressWarnings("unused")
			List<Row<Integer>> data = loader.loadFromJson(new File(TEST_INVALID_JSON_SRC));
		} catch (RuntimeException e) {
			assertThat(e.getMessage(), is(PARSING_ERROR_MESSAGE));
		}
	}

	@Test
	public void testOrderingIsSubsetOfLabels() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation");

		// level 0 (entire dataset) aggregation
		assertEquals(pivot.get(), Integer.valueOf(8516));

		// level 1 aggregation
		assertEquals(pivot.get("germany"), Integer.valueOf(3323));
		assertEquals(pivot.get("france"), Integer.valueOf(2149));
		assertEquals(pivot.get("spain"), Integer.valueOf(2896));
		assertEquals(pivot.get("italy"), Integer.valueOf(148));

	}

	@Test
	public void testMeanOnSampleInput() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Double> pivot = new AggregationTree<Integer, Double>(data, (l) -> {
			Double sum = 0.0;
			for (Integer v : l) {
				sum = sum + v;
			}
			return (double) (sum / l.size());
		}, "nation", "eyes", "hair");

		// level 0 (entire dataset) aggregation
		assertEquals(pivot.get(), Double.valueOf(532.25));

		// level 1 aggregation
		assertEquals(pivot.get("germany"), Double.valueOf(474.7142857142857));
		assertEquals(pivot.get("france"), Double.valueOf(537.25));
		assertEquals(pivot.get("italy"), Double.valueOf(148));
		assertEquals(pivot.get("spain"), Double.valueOf(724));

		// level 2 aggregation
		assertEquals(pivot.get("france", "blue"), Double.valueOf(502));
		assertEquals(pivot.get("france", "green"), Double.valueOf(572.5));

		assertEquals(pivot.get("germany", "blue"), Double.valueOf(389));
		assertEquals(pivot.get("germany", "brown"), Double.valueOf(753));
		assertEquals(pivot.get("germany", "dark"), Double.valueOf(285.5));
		assertEquals(pivot.get("germany", "green"), Double.valueOf(536.6666666666666));

		assertEquals(pivot.get("italy", "dark"), Double.valueOf(148));

		assertEquals(pivot.get("spain", "blue"), Double.valueOf(852));
		assertEquals(pivot.get("spain", "brown"), Double.valueOf(778));
		assertEquals(pivot.get("spain", "dark"), Double.valueOf(907));
		assertEquals(pivot.get("spain", "green"), Double.valueOf(359));

		// level 3 aggregation

		assertEquals(pivot.get("france", "blue", "black"), Double.valueOf(502));
		assertEquals(pivot.get("france", "green", "black"), Double.valueOf(857));
		assertEquals(pivot.get("france", "green", "blonde"), Double.valueOf(288));

		assertEquals(pivot.get("germany", "blue", "brown"), Double.valueOf(389));
		assertEquals(pivot.get("germany", "brown", "red"), Double.valueOf(753));
		assertEquals(pivot.get("germany", "dark", "black"), Double.valueOf(468));
		assertEquals(pivot.get("germany", "dark", "brown"), Double.valueOf(103));
		assertEquals(pivot.get("germany", "green", "brown"), Double.valueOf(168));
		assertEquals(pivot.get("germany", "green", "red"), Double.valueOf(721));

		assertEquals(pivot.get("italy", "dark", "black"), Double.valueOf(148));

		assertEquals(pivot.get("spain", "blue", "black"), Double.valueOf(852));
		assertEquals(pivot.get("spain", "brown", "red"), Double.valueOf(778));
		assertEquals(pivot.get("spain", "dark", "black"), Double.valueOf(907));
		assertEquals(pivot.get("spain", "green", "brown"), Double.valueOf(359));
	}

	@Test
	public void testCaching() {
		InputLoader<Integer> loader = new InputLoader<Integer>();
		List<Row<Integer>> data = loader.loadFromJson(new File(TEST_SAMPLE_DATASET_SRC));
		AggregationTree<Integer, Integer> pivot = new AggregationTree<Integer, Integer>(data, (l) -> {
			return l.parallelStream().reduce(0, (a, b) -> a + b);
		}, "nation", "eyes", "hair");

		pivot.setCaching(true);
		Long start = System.nanoTime();
		pivot.get("france", "blue", "black");
		Long end = System.nanoTime();

		Long initial = end - start;

		start = System.nanoTime();
		pivot.get("france", "blue", "black");
		end = System.nanoTime();

		Long afterCached = end - start;
		assertThat(initial, greaterThan(afterCached));
	}
}
