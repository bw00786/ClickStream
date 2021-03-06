package com.wilk.main.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public interface SparkQuery {

    public List<Row> listOfClickStreams(String search, String groupBy, SparkSession spark, Dataset names2, String searchType) ;
}
