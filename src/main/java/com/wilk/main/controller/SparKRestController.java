package com.wilk.main.controller;

import com.wilk.main.service.SparkQueryImpl;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.GATEWAY_TIMEOUT;


@RequestMapping("/clickStreamData")
@RestController
public class SparKRestController {

    private static String searchType;
    private static final String DELIMITER = ",";
    private static final int pageSize = 25;


    @Autowired
    public SparKRestController(final SparkQueryImpl sparkQueryImpl) {
        this.sparkQueryImpl = sparkQueryImpl;
    }

    @Autowired
    private SparkQueryImpl sparkQueryImpl;



    private SparkSession spark = SparkSession.builder().master( "local" ).appName( "SparkConnectorIntro" ).config( "spark.sql.shuffle.partitions", 6 ).config( "spark.executor.memory", "2g" ).getOrCreate();

   private Dataset<Row> dataframe = spark.sqlContext().read().format("com.databricks.spark.csv").option("header","true")
           .option( "delimiter",DELIMITER ).option("inferSchema", "true").csv( "/tmp/clickStream.csv" );


    @GetMapping("/domain_name/{domainId}/{groupBy}",params = { "page", "size" })
    public List<Row> getSparkQueryResults(@PathVariable String domainId, @PathVariable String groupBy, SparkSession spark, Dataset dataframe) {
        searchType = "user_domain";
        return sparkQueryImpl.listOfClickStreams( domainId,groupBy,spark,dataframe,searchType );
    }

    @GetMapping("/url_id/{url}/{groupBy}")
    public List<Row> getSparkQueryResults2(@PathVariable String url, @PathVariable String groupBy, SparkSession spark, Dataset dataframe) {
        searchType = "url";
        return sparkQueryImpl.listOfClickStreams( url,groupBy,spark,dataframe, searchType);
    }
    @ExceptionHandler
    @ResponseStatus(BAD_REQUEST)
    public String handleBadRequestException(MalFormedHeaderException e) {
       return e.getMessage();
    }

    @ExceptionHandler
    @ResponseStatus(GATEWAY_TIMEOUT)
    public String handleServerUnavailableException(erverUnavailableException e) {
        return e.getMessage();
    }



}
