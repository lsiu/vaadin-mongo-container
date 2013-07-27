package com.github.lsiu.vaadin.mongo;

import com.mongodb.DBObject;
import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractProperty;

@SuppressWarnings("serial")
public class MongoProperty<T> extends AbstractProperty<T> implements Property<T> {
	
	private DBObject dbObject;
	private String key;
	private Class<T> valueClass;

	public MongoProperty(DBObject dbObject, String key, Class<T> valueClass) {
		super();
		this.dbObject = dbObject;
		this.key = key;
		this.valueClass = valueClass;
	}

	@SuppressWarnings("unchecked")
	public T getValue() {
		return (T) dbObject.get(key);
	}

	public void setValue(T newValue)
			throws com.vaadin.data.Property.ReadOnlyException {
		dbObject.put(key, newValue);
	}

	public Class<? extends T> getType() {
		return valueClass;
	}

}
