package com.github.lsiu.vaadin.mongo;

public class MongoCacheValue<T> {
	
	private long timestamp;
	
	private T value;

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.timestamp = System.currentTimeMillis();
		this.value = value;
	}
	
	public long getValueSetTimestamp() {
		return timestamp;
	}

}
