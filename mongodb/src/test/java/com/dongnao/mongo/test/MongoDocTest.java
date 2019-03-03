package com.dongnao.mongo.test;

import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.addEachToSet;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class MongoDocTest {

	private static final Logger logger = LoggerFactory.getLogger(MongoDocTest.class);

	private MongoDatabase db;

	private MongoCollection<Document> doc;

	private MongoClient client;

	@Before
	public void init() {

		client = new MongoClient("192.168.3.104", 27022);
		db = client.getDatabase("tom");
		doc = db.getCollection("users");
	}

	@Test
	public void testInsert() {

		Document document = new Document();
		document.append("username", "cang");
		document.append("country", "USA");
		document.append("age", 20);
		document.append("length", 1.77f);
		document.append("salary", new BigDecimal("655.32"));

		Map<String, String> address = new HashMap<String, String>();
		address.put("aCode", "411000");
		address.put("add", "长沙");
		document.append("address", address);

		Map<String, Object> hobby = new HashMap<String, Object>();
		hobby.put("movies", Arrays.asList("aa", "bb"));
		hobby.put("city", Arrays.asList("东莞", "东京"));
		document.append("hobby", hobby);

		Document document2 = new Document();
		document2.append("username", "chen");
		document2.append("country", "JP");
		document2.append("age", 20);
		document2.append("length", 1.77f);
		document2.append("salary", new BigDecimal("655.32"));

		Map<String, String> address2 = new HashMap<String, String>();
		address2.put("aCode", "112221");
		address2.put("add", "长沙");
		document2.append("address", address2);

		Map<String, Object> hobby2 = new HashMap<String, Object>();
		hobby2.put("movies", Arrays.asList("aa", "bb"));
		hobby2.put("city", Arrays.asList("东莞", "东京"));
		document2.append("hobby", hobby2);

		doc.insertMany(Arrays.asList(document, document2));
	}

	@Test
	public void testDelete() {

		//delete from users where username="cang"
		DeleteResult result = doc.deleteMany(eq("username", "cang"));
		logger.info(String.valueOf(result.getDeletedCount()));

		//delete from users where age>8 and age<25
		DeleteResult result2 = doc.deleteMany(and(gt("age", 8), lt("age", 25)));
		logger.info(String.valueOf(result2.getDeletedCount()));

	}

	@Test
	public void testUpdate() {
		
		//update  users  set age=6 where username = 'lison' 
		doc.updateMany(eq("username","lison"),new Document("$set",new Document("age",6)));
		
		//update users  set favorites.movies add "小电影2 ", "小电影3" where favorites.cites  has "东莞"
		doc.updateMany(eq("favorites.cites","东莞"),addEachToSet("favorites.movies", Arrays.asList("小电影2","小电影3")));
	}

	@Test
	public void testFind() {
		final List<Document> result = new ArrayList<Document>();

		Block<Document> printBlock = new Block<Document>() {

			public void apply(Document t) {

				logger.info("result:" + t.toJson());
				result.add(t);
			}
		};

		//select * from users where favourite.cites has "东莞" ,"东京" 
		FindIterable<Document> find = doc.find(all("hobby.city", Arrays.asList("东莞", "东京")));
		find.forEach(printBlock);
		logger.info("aaaa" + String.valueOf(result.size()));
	}

}
