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
import java.util.List;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dongnao.mongo.entity.Address;
import com.dongnao.mongo.entity.Favorites;
import com.dongnao.mongo.entity.User;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class MongoPojoTest {

	private static final Logger logger = LoggerFactory.getLogger(MongoPojoTest.class);

	private MongoDatabase db;

	private MongoCollection<User> doc;

	private MongoClient client;

	@Before
	public void init() {

		//编解码器的list，存放pojo编解码器 和默认编解码器  最终生成编解码器注册中心
		List<CodecRegistry> codecRegistes = new ArrayList<CodecRegistry>();

		//将默认编解码器加入到集合中
		codecRegistes.add(MongoClient.getDefaultCodecRegistry());

		//生成pojo编解码器
		CodecRegistry pojoProviders = CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build());

		//将pojo编解码器加入到编解码器集合中
		codecRegistes.add(pojoProviders);

		//生成编解码器注册中心
		CodecRegistry registry = CodecRegistries.fromRegistries(codecRegistes);

		MongoClientOptions build = MongoClientOptions.builder().codecRegistry(registry).build();
		ServerAddress serverAddress = new ServerAddress("192.168.3.103", 27022);

		client = new MongoClient(serverAddress, build);
		db = client.getDatabase("tom");
		doc = db.getCollection("users", User.class);
	}

	@Test
	public void testInsert() {

		User user = new User();
		user.setUsername("cang");
		user.setCountry("USA");
		user.setAge(20);
		user.setLenght(1.77f);
		user.setSalary(new BigDecimal("6265.22"));
		Address address1 = new Address();
		address1.setaCode("411222");
		address1.setAdd("sdfsdf");
		user.setAddress(address1);
		Favorites favorites1 = new Favorites();
		favorites1.setCites(Arrays.asList("东莞", "东京"));
		favorites1.setMovies(Arrays.asList("西游记", "一路向西"));
		user.setFavorites(favorites1);

		User user1 = new User();
		user1.setUsername("chen");
		user1.setCountry("China");
		user1.setAge(30);
		user1.setLenght(1.77f);
		user1.setSalary(new BigDecimal("6885.22"));
		Address address2 = new Address();
		address2.setaCode("411000");
		address2.setAdd("我的地址2");
		user1.setAddress(address2);
		Favorites favorites2 = new Favorites();
		favorites2.setCites(Arrays.asList("珠海", "东京"));
		favorites2.setMovies(Arrays.asList("东游记", "一路向东"));
		user1.setFavorites(favorites2);

		doc.insertMany(Arrays.asList(user, user1));
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

		//update  users  set age=6 where username = 'cang' 
	//	doc.updateMany(eq("username", "chen"), new Document("$set", new Document("age", 6)));

		//update users  set favorites.movies add "小电影2 ", "小电影3" where favorites.cites  has "东莞"
		doc.updateMany(eq("favorites.cites", "东京"), addEachToSet("favorites.movies", Arrays.asList("小电影2", "小电影3")));
	}

	@Test
	public void testFind() {
		final List<User> result = new ArrayList<User>();

		Block<User> printBlock = new Block<User>() {

			public void apply(User user) {

				logger.info("result:" + user);
				result.add(user);
			}
		};

		//select * from users where favourite.cites has "东莞" ,"东京" 
		FindIterable<User> find = doc.find(all("favorites.cites", Arrays.asList("珠海", "东京")));
		find.forEach(printBlock);
		logger.info("aaaa" + String.valueOf(result.size()));
	}

}
