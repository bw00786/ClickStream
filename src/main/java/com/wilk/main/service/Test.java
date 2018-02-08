package com.wilk.main.service;
import java.net.UnknownHostException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class Test {

public static void main(String[] args) throws UnknownHostException,
        MongoException {

    Mongo mongo = new Mongo("localhost", 27017);
    DB db = mongo.getDB("yeahMongo");

    Employee employee = new Employee();
    employee.setNo(3L);
    employee.setName("Lee");

    DBCollection employeeCollection = null ;
    employeeCollection = db.getCollection(Employee.COLLECTION_NAME);

    employeeCollection.save(employee);

    System.err.println(employeeCollection.findOne());

}


}