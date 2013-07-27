package com.github.lsiu.vaadin.mongo;

import java.util.Collection;

import com.mongodb.DBObject;
import com.vaadin.data.Item;
import com.vaadin.data.Property;

@SuppressWarnings("serial")
public class MongoItem implements Item {
	
	private DBObject dbObject;

	public MongoItem(DBObject dbObject) {
		super();
		this.dbObject = dbObject;
	}

	@SuppressWarnings("rawtypes")
	public Property getItemProperty(Object id) {
		return new MongoProperty<Object>(dbObject, (String)id, Object.class);
	}

	public Collection<?> getItemPropertyIds() {
		return dbObject.keySet();
	}

	@SuppressWarnings("rawtypes")
	public boolean addItemProperty(Object id, Property property)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	public boolean removeItemProperty(Object id)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

}
