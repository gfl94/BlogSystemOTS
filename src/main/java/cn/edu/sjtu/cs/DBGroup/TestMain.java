package cn.edu.sjtu.cs.DBGroup;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.*;

import java.util.Random;

/**
 * Created by gefei on 16-6-3.
 */
public class TestMain {
    public static final String endPoint = "http://SJTUBlogOTS.cn-hangzhou.ots.aliyuncs.com";
    public static final String accessId = "PL2Bu1QHcXUnpiGd";
    public static final String accessKey = "Ho9qXXj7vzgRQYOnXzaOJdXkBYxeJd";
    public static final String instanceName = "SJTUBlogOTS";

    public static void main(String[] args){
//        OTSClient client = new OTSClient(endPoint, accessId, accessKey, instanceName);
//        TableMeta tableMeta = new TableMeta("BlogInfoTable");
//        tableMeta.addPrimaryKeyColumn("UserID", PrimaryKeyType.INTEGER);
//        tableMeta.addPrimaryKeyColumn("ArticleID", PrimaryKeyType.INTEGER);
//        tableMeta.addPrimaryKeyColumn("ActionType", PrimaryKeyType.INTEGER);
//
//        CapacityUnit capacityUnit = new CapacityUnit(10, 2);
//
//        try{
//            CreateTableRequest request = new CreateTableRequest();
//            request.setTableMeta(tableMeta);
//            request.setReservedThroughput(capacityUnit);
//            client.createTable(request);
//            System.out.println("Table successfully created.");
//        } catch (ClientException ex){
//            ex.printStackTrace();
//            System.out.println("Create table failed");
//        } catch (OTSException ex){
//            ex.printStackTrace();
//            System.out.println("OTS Error");
//        }
//
//        ListTableResult result = client.listTable();
//
//        for (String tableName: result.getTableNames()){
//            System.out.println("Table: " + tableName);
//        }
//
//        RowPrimaryKey primaryKey = new RowPrimaryKey();
//        primaryKey.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(1));
//        primaryKey.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1000));
//
//        RowPutChange rowChange = new RowPutChange("SampleTable3");
//        rowChange.setPrimaryKey(primaryKey);
//
//        rowChange.addAttributeColumn("col0", ColumnValue.fromLong(10));
//        rowChange.addAttributeColumn("col1", ColumnValue.fromLong(1111111));
//        rowChange.addAttributeColumn("col2", ColumnValue.fromString("上海交通大学"));
//        rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
//
//        try{
//            PutRowRequest request = new PutRowRequest();
//            request.setRowChange(rowChange);
//            client.putRow(request);
//            System.out.println("Add row successfully");
//        } catch (Exception e){
//            e.printStackTrace();
//            System.out.println("Error put");
//        }
//
//        client.shutdown();

        BlogSystem system = new BlogSystem(endPoint, accessId, accessKey, instanceName);
//        system.clearAllContent();

        Random random = new Random();
        random.setSeed(System.currentTimeMillis());

        // Add 100 articles written by a random author from 1000 - 1009
        for (int i = 0 ; i < 100; i++){
            int author = 1000 + random.nextInt(10);
            System.out.println("Author " + author + " writes Article " + i);
            system.addArticle(author, i, "Article content: " + i);
        }

        // for 1000 operations on these articles
        for (int i = 0; i < 50; ++i){
            int author = 1000 + random.nextInt(10);
            int article = random.nextInt(100);
            int operation = random.nextInt(2); // comment or like
            switch (operation + 1){
                case BlogSystem.COMMENT_ARTICLE:
                    System.out.println("Author " + author + " comments on article " + article);
                    system.commentArticle(author, article, "comment: " + i);
                    break;
                case BlogSystem.LIKE_ARTICLE:
                    System.out.println("Author " + author + " like the article " + article);
                    system.likeArticle(author, article);
                    break;
                default:
                    System.out.println("Hello world");
            }
        }

        for (int i = 0; i < 10; ++i){
            System.out.println("------------------");
            System.out.println("query author " + i);
            system.queryUser(1000 + i);
            System.out.println("\n\n");
        }
        System.out.println("------------------");






//        system.printAllRows();

        system.clearAllContent();
        system.close();
        System.out.println("Hello world");
    }
}
