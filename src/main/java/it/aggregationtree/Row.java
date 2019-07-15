package it.aggregationtree;

import java.util.HashMap;
import java.util.Map;

public class Row<V> {

	// properties

	private V value;

	private Map<String, String> labels;

	// constructor

	public Row() {
		this.labels = new HashMap<String, String>();
	}

	// getters and setters

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public Map<String, String> getLabels() {
		return labels;
	}

	public void setLabels(Map<String, String> labels) {
		this.labels = labels;
	}

	public String getLabel(String label) {
		return this.labels.get(label);
	}

	// others utility methods

	public void addLabel(String label, String value) {
		this.labels.put(label, value);
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();

		for (String k : this.labels.keySet()) {
			sb.append(this.labels.get(k) + ",");
		}
		sb.append(this.value);

		return sb.toString();
	}
}
