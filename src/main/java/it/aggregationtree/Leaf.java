package it.aggregationtree;

import java.util.ArrayList;
import java.util.List;

public class Leaf<V> implements Node<V> {

	private List<V> value;

	public Leaf(V value) {
		this.value = new ArrayList<V>();
		this.value.add(value);
	}

	public List<V> getValue() {
		return this.value;
	}

	@Override
	public String toString() {
		return this.value.toString();
	}

	public String printPretty(String indent, Boolean last) {
		StringBuilder sb = new StringBuilder();
		sb.append(indent);

		if (last) {
			sb.append("\\-");
			indent += "  ";
		} else {
			sb.append("|-");
			indent += "| ";
		}

		sb.append(this.toString());
		sb.append("\n");

		return sb.toString();

	}

}