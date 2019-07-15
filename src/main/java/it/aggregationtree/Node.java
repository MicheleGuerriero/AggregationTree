package it.aggregationtree;

import java.util.List;

public interface Node<V> {
	public List<V> getValue();

	public String printPretty(String indent, Boolean last);
}
