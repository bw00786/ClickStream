package com.wilk.main.service;

import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SparkQueryImpl implements SparkQuery{

    @Override
    public  List<Row> listOfClickStreams(String search,String groupBy, SparkSession spark, Dataset names2,String searchType) {
      long startTime = System.currentTimeMillis();
      StringBuilder builder = new StringBuilder(  );
      builder.append("select ");
      builder.append(searchType);
      builder.append(", count(distinct session_id),count(distinct profile_id) from ");
      builder.append(names2);
      builder.append(" where user_domain = ");
      builder.append(searchType);
      builder.append(" = ");
      builder.append(search);
      builder.append(" group by user_domain order by ");
      builder.append(groupBy);
      String sqlQuery = builder.toString();
      Dataset<Row> names = spark.sql(sqlQuery);


      return names.collectAsList();

  }

}
