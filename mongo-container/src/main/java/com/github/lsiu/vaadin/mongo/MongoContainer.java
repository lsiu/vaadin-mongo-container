package com.github.lsiu.vaadin.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.NewBSONDecoder;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.vaadin.data.Container;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.data.util.AbstractContainer;

@SuppressWarnings("serial")
public class MongoContainer extends AbstractContainer implements
		Container.Ordered, Container.Sortable {

	private DBCollection collection;

	private String idField;

	public MongoContainer(DBCollection collection, String idField) {
		this.collection = collection;
		this.idField = idField;
	}

	@Override
	public Item getItem(Object itemId) {
		DBObject dbObject = collection.findOne(new BasicDBObject(idField,
				itemId));
		return new MongoItem(dbObject);
	}

	@Override
	public Collection<?> getContainerPropertyIds() {
		// FIXME: This may not be the best way
		return collection.findOne().keySet();
	}

	@Override
	public Collection<?> getItemIds() {
		BasicDBObject filter = null;
		if (idField.equalsIgnoreCase("_id")) {
			filter = new BasicDBObject("_id", 1);
		} else {
			filter = new BasicDBObject("_id", 0);
			filter.put(idField, 1);
		}

		List<DBObject> list = collection.find(new BasicDBObject(), filter)
				.toArray();
		List<Object> ids = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			ids.add(list.get(i).get(idField));
		}
		return ids;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Property getContainerProperty(Object itemId, Object propertyId) {
		DBObject object = collection
				.findOne(new BasicDBObject(idField, itemId));
		if (object == null) {
			return null;
		} else {
			return new MongoProperty<Object>(object,
					String.valueOf(propertyId), Object.class);
		}
	}

	@Override
	public Class<?> getType(Object propertyId) {
		String key = String.valueOf(propertyId);
		DBObject object = collection.findOne(new BasicDBObject(key,
				new BasicDBObject("$exists", true)));
		Object field = object.get(String.valueOf(propertyId));
		return field == null ? Object.class : field.getClass();
	}

	@Override
	public int size() {
		long count = collection.getCount();
		return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
	}

	@Override
	public boolean containsId(Object itemId) {
		return collection.findOne(new BasicDBObject(idField, itemId)) == null ? false
				: true;
	}

	@Override
	public Item addItem(Object itemId) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object addItem() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	public boolean removeItem(Object itemId)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	public boolean addContainerProperty(Object propertyId, Class<?> type,
			Object defaultValue) throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	public boolean removeContainerProperty(Object propertyId)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	public boolean removeAllItems() throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object nextItemId(Object itemId) {
		DBCursor cur = collection
				.find(new BasicDBObject(this.idField, new BasicDBObject("$gt",
						itemId))).sort(sortField).limit(2);
		if (cur.hasNext()) {
			cur.next(); // this is the actual doc with itemId. We need the next
						// one.
			if (cur.hasNext()) {
				return cur.next().get(idField);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	@Override
	public Object prevItemId(Object itemId) {
		BasicDBObject negatedSortedField = new BasicDBObject();
		for(String key: sortField.keySet()) {
			int asc = (int)sortField.get(key);
			negatedSortedField.put(key, asc);
		}
		
		DBCursor cur = collection
				.find(new BasicDBObject(this.idField, new BasicDBObject("$lt",
						itemId))).sort(negatedSortedField).limit(2);

		if (cur.hasNext()) {
			cur.next(); // this is the actual doc with itemId. We need the next
						// one.
			if (cur.hasNext()) {
				return cur.next().get(idField);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	@Override
	public Object firstItemId() {
		DBCursor cursor = collection.find().sort(sortField).limit(1);
		return cursor.hasNext() ? cursor.next().get(idField) : null;
	}

	@Override
	public Object lastItemId() {
		long count = collection.count();
		if (count > Integer.MAX_VALUE) {
			throw new IllegalStateException(
					"Collection size greater than Integer.MAX_VALUE ("
							+ Integer.MAX_VALUE + ")");
		}
		int n = (int) count;
		DBCursor cursor = collection.find().skip(n - 1).limit(1)
				.sort(sortField);
		return cursor.next().get(idField);
	}

	@Override
	public boolean isFirstId(Object itemId) {
		DBCursor cursor = collection.find().limit(1).sort(sortField);
		DBObject dbObject = null;
		if (cursor.hasNext()) {
			dbObject = cursor.next();
			return itemId.equals(dbObject.get(idField));
		}
		return false;
	}

	@Override
	public boolean isLastId(Object itemId) {
		long count = collection.count();
		if (count > Integer.MAX_VALUE) {
			throw new IllegalStateException(
					"Collection size greater than Integer.MAX_VALUE ("
							+ Integer.MAX_VALUE + ")");
		}
		int n = (int) count;
		DBCursor cursor = collection.find().skip(n - 1).limit(1)
				.sort(sortField);
		DBObject dbObject = null;
		if (cursor.hasNext()) {
			dbObject = cursor.next();
			return itemId.equals(dbObject.get(idField));
		}
		return false;
	}

	@Override
	public Object addItemAfter(Object previousItemId)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Item addItemAfter(Object previousItemId, Object newItemId)
			throws UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	private BasicDBObject sortField = new BasicDBObject("_id", 1);

	@Override
	public void sort(Object[] propertyId, boolean[] ascending) {
		sortField = new BasicDBObject();
		if (propertyId != null) {

			for (int i = 0; i < propertyId.length; i++) {
				boolean asc = false;
				if (ascending != null && i < ascending.length) {
					asc = ascending[i];
				}
				sortField.append(String.valueOf(propertyId[i]), asc);
			}
		}
	}

	@Override
	public Collection<?> getSortableContainerPropertyIds() {
		List<DBObject> indexes = collection.getIndexInfo();
		List<Object> indexedPropertyIds = new ArrayList<>();
		for (DBObject i : indexes) {
			DBObject dbObject = (DBObject) i.get("key");
			Object key = dbObject.keySet().iterator().next();
			indexedPropertyIds.add(key);
		}
		return indexedPropertyIds;
	}
}
