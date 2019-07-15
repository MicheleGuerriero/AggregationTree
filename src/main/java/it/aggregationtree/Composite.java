package it.aggregationtree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Composite<V> implements Node<V> {

	private Set<Node<V>> childs;
	private final String label;
	private List<V> cachedValue;
	private Boolean caching;

	public Composite(Set<Node<V>> childs, String label) {
		this.label = label;
		this.childs = childs;
		// by default caching is diabled
		this.caching = false;
	}

	public Composite(String label) {
		this.label = label;
		this.childs = new HashSet<Node<V>>();
		// by default caching is set to false
		this.caching = false;
	}

	public Set<Node<V>> getChilds() {
		return childs;
	}

	public void setChilds(Set<Node<V>> childs) {
		this.childs = childs;
	}

	public String getLabel() {
		return this.label;
	}

	public void addChild(Node<V> child) {
		this.childs.add(child);
	}

	public List<V> getValue() {
		if (this.caching) {
			if (this.cachedValue == null) {
				this.cachedValue = this.childs.parallelStream().map(n -> n.getValue()).reduce(new ArrayList<V>(), (a, b) -> {
					ArrayList<V> c = new ArrayList<V>();
					c.addAll(a);
					c.addAll(b);
					return c;
				});
			}
			return this.cachedValue;
		} else {
			return this.childs.parallelStream().map(n -> n.getValue()).reduce(new ArrayList<V>(), (a, b) -> {
				ArrayList<V> c = new ArrayList<V>();
				c.addAll(a);
				c.addAll(b);
				return c;
			});
		}
	}

	@Override
	public String toString() {
		return this.label;
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

		int count = 0;
		for (Node<V> c : this.childs) {
			sb.append(c.printPretty(indent, count == this.childs.size() - 1));
			count = count + 1;
		}

		return sb.toString();
	}

	public Boolean getCaching() {
		return caching;
	}

	public void setCaching(Boolean caching) {
		if (this.caching && !caching) {
			// invalidate cache
			this.cachedValue = null;
		}
		for (Node<V> c : this.childs) {
			if (c instanceof Composite) {
				((Composite<V>) c).setCaching(caching);
			}
		}
		this.caching = caching;
	}

}