package com.github.lsiu.vaadin.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.bson.NewBSONDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static final Logger logger = LoggerFactory.getLogger(MongoContainer.class);

	private DBCollection collection;

	private String idField;

	private long timeToLive = 1000;
	
	private int pageSize = 25;

	private SortedMap<Object, MongoCacheValue> cacheMap = Collections
			.synchronizedSortedMap(new TreeMap<Object, MongoCacheValue>());

	private MongoCacheValue lastIdCache = new MongoCacheValue();

	public MongoContainer(DBCollection collection, String idField) {
		this.collection = collection;
		this.idField = idField;
	}

	@Override
	public Item getItem(Object itemId) {
		Object object = cacheMap.get(itemId);
		if (object != null) {
			MongoCacheValue value = (MongoCacheValue)object;
			if (System.currentTimeMillis() - value.retrieveTs < timeToLive) {
				logger.trace("getItem: {} cache hit!", itemId);
				return (MongoItem)value.value;
			}
		}
		logger.trace("getItem: {} cache missed!");
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
		logger.trace("getItemIds");
		throw new UnsupportedOperationException("This is an expensive call and is deprecated in Vaadin. See https://vaadin.com/forum#!/thread/19709");
//		BasicDBObject filter = null;
//		if (idField.equalsIgnoreCase("_id")) {
//			filter = new BasicDBObject("_id", 1);
//		} else {
//			filter = new BasicDBObject("_id", 0);
//			filter.put(idField, 1);
//		}
//
//		List<DBObject> list = collection.find(new BasicDBObject(), filter)
//				.toArray();
//		List<Object> ids = new ArrayList<>();
//		for (int i = 0; i < list.size(); i++) {
//			ids.add(list.get(i).get(idField));
//		}
//		return ids;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public Property getContainerProperty(Object itemId, Object propertyId) {
		Item item = getItem(itemId);
		return item.getItemProperty(propertyId);
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
		logger.trace("size");
		long count = collection.getCount();
		return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
	}

	@Override
	public boolean containsId(Object itemId) {
		logger.trace("containsId: {}", itemId);
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
		
		SortedMap<Object, MongoCacheValue> subMap = cacheMap.tailMap(itemId);
		if (subMap != null) {
			if (subMap.keySet().isEmpty() == false) {
				Iterator<Object> itr = subMap.keySet().iterator();
				if (itr.hasNext()) {
					Object key = itr.next(); // this is the current itemId
					if (itr.hasNext()) {
						key = itr.next();
						MongoCacheValue value = cacheMap.get(key);
						if (value != null && System.currentTimeMillis() - value.retrieveTs < timeToLive) {
							logger.trace("nextItemId {} cache hit!", itemId);
							return key;
						}
					}
				}
			}
		}
		logger.trace("nextItemId {} cache missed!", itemId);
		DBCursor cur = collection
				.find(new BasicDBObject(this.idField, new BasicDBObject("$gte",
						itemId))).sort(sortField).limit(pageSize + 1);
		
		long retrieveTs = System.currentTimeMillis();
		while(cur.hasNext()) {
			DBObject dbObject = cur.next();
			Object key = dbObject.get(idField);
			
			MongoCacheValue value = new MongoCacheValue();
			value.retrieveTs = retrieveTs;
			value.value = new MongoItem(dbObject);
			
			cacheMap.put(key, value);
		}
		
		subMap = cacheMap.tailMap(itemId);
		Iterator<Object> itr = subMap.keySet().iterator();
		itr.next(); // this is the actual doc with itemId. We need the next one.
		if (itr.hasNext()) {
			Object keyNext = itr.next();
			return keyNext;
		} else {
			return null;
		}
	}

	@Override
	public Object prevItemId(Object itemId) {
		logger.trace("prevItemId {}", itemId);
		BasicDBObject negatedSortedField = new BasicDBObject();
		for (String key : sortField.keySet()) {
			int asc = (int) sortField.get(key);
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
		logger.trace("firstItemId");
		DBCursor cursor = collection.find().sort(sortField).limit(1);
		return cursor.hasNext() ? cursor.next().get(idField) : null;
	}

	@Override
	public Object lastItemId() {
		logger.trace("isLastItem");
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
		logger.trace("isFirstId {}", itemId);
		if (itemId == null)
			return false;
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
		logger.trace("isLastId {}", itemId);
		long retrieveTs = lastIdCache.retrieveTs;
		Object cachedLastId = lastIdCache.value;
		if ((System.currentTimeMillis() - retrieveTs) < timeToLive
				&& cachedLastId != null) {
			logger.trace("isLastId cache hit!");
			return cachedLastId.equals(itemId);
		}
		logger.trace("isLastId cache missed!");
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
			Object lastId = dbObject.get(idField);
			lastIdCache.value = lastId;
			lastIdCache.retrieveTs = System.currentTimeMillis();
			return itemId.equals(lastId);
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
