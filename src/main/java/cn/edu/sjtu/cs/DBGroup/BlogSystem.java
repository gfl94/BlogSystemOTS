package cn.edu.sjtu.cs.DBGroup;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.*;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by gefei on 16-6-3.
 */
public class BlogSystem {
    private OTSClient mOTSClient;

    public static final int ADD_ARTICLE = 0;
    public static final int COMMENT_ARTICLE = 1;
    public static final int LIKE_ARTICLE = 2;

    public static final int[] ALL_OPERATIONS = {ADD_ARTICLE, COMMENT_ARTICLE, LIKE_ARTICLE};

    private final String[] OperationMessages = {"Article addition", "Article Comment", "Article Like"};

    private final String sTableName = "BlogInfoTable";
    private int count;

    private class BlogInfoRow{

    }

    public BlogSystem(String endPoint, String accessId, String accessKey, String instanceName){
        if (mOTSClient == null){
            mOTSClient = new OTSClient(endPoint, accessId, accessKey, instanceName);

            ListTableResult tableList = mOTSClient.listTable();
            if (tableList.getTableNames().contains(sTableName)){
                System.out.println("Table already exists");
                return;
            }


            TableMeta tableMeta = new TableMeta(sTableName);
            tableMeta.addPrimaryKeyColumn("Md5Count", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("UserID", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("ArticleID", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("ActionType", PrimaryKeyType.INTEGER);

            CapacityUnit capacityUnit = new CapacityUnit(0, 0);
            try{
                CreateTableRequest request = new CreateTableRequest();
                request.setTableMeta(tableMeta);
                request.setReservedThroughput(capacityUnit);
                mOTSClient.createTable(request);
                System.out.println("Initialization finished");
            } catch (Exception e){
                System.out.println("Initialization failed");
            }
        }
        count = 0;
    }

    private void addRowToTable(int userId, int articleId, int type, String content){
        String opertion_message = OperationMessages[type];
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        String str = new Integer(count).hashCode() % 1000 + "" + count;
        count++;
        primaryKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.fromString(str));
        primaryKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(userId));
        primaryKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId));
        primaryKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(type));

        RowPutChange addRow = new RowPutChange(sTableName);
        addRow.setPrimaryKey(primaryKey);

        if (content != null)
            addRow.addAttributeColumn("content", ColumnValue.fromString(content));
        addRow.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
        try{
            PutRowRequest request = new PutRowRequest();
            request.setRowChange(addRow);
            mOTSClient.putRow(request);
            System.out.println(opertion_message + ": success");
        } catch (ClientException e){
            System.out.println(opertion_message + ": ClientException");
        } catch (OTSException e){
            System.out.println(opertion_message + ": OTSException");
        }
    }

    public void addArticle(int userId, int articleId, String content){
        addRowToTable(userId, articleId, ADD_ARTICLE, content);
//        RowPrimaryKey primaryKey = new RowPrimaryKey();
//        primaryKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(userId));
//        primaryKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId));
//        primaryKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(ADD_ARTICLE));
//
//        RowPutChange addRow = new RowPutChange(sTableName);
//        addRow.setPrimaryKey(primaryKey);
//
//        addRow.addAttributeColumn("content", ColumnValue.fromString(content));
//        addRow.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
//        try{
//            PutRowRequest request = new PutRowRequest();
//            request.setRowChange(addRow);
//            mOTSClient.putRow(request);
//            System.out.println("Article addition success");
//        } catch (ClientException e){
//            System.out.println("Article addition: ClientException");
//        } catch (OTSException e){
//            System.out.println("Article addition: OTSException");
//        }
    }

    public void commentArticle(int userId, int articleId, String content){
        addRowToTable(userId, articleId, COMMENT_ARTICLE, content);
    }

    public void likeArticle(int userId, int articleId){
        addRowToTable(userId, articleId, LIKE_ARTICLE, null);
    }

    private List<Row> queryRangeForSingleActionType(int userId_low, int userId_high, int articleId_low,
                            int articleId_high, int actionType){
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(sTableName);
        RowPrimaryKey inclusiveStartKey = new RowPrimaryKey();
        inclusiveStartKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MIN);
        inclusiveStartKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(userId_low));
        inclusiveStartKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId_low));
        inclusiveStartKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(actionType));
        RowPrimaryKey exclusiveEndKey = new RowPrimaryKey();
        exclusiveEndKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MAX);
        exclusiveEndKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(userId_high));
        exclusiveEndKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId_high));
        exclusiveEndKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(actionType + 1));

        criteria.setInclusiveStartPrimaryKey(inclusiveStartKey);
        criteria.setExclusiveEndPrimaryKey(exclusiveEndKey);
        criteria.setDirection(Direction.FORWARD);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);

        try{
            GetRangeResult result = mOTSClient.getRange(request);
            return result.getRows();
        } catch (OTSException ots){
            ots.printStackTrace();
            System.out.println("OTSException");
        } catch (ClientException cli){
            System.out.println("ClientException");
        }
        return new ArrayList<Row>();
    }

    private List<Row> queryRange(int userId_low, int userId_high, int articleId_low,
                            int articleId_high, int actionType[]){
        List<Row> result = new ArrayList<Row>();
        for (int type : actionType){
            result.addAll(queryRangeForSingleActionType(userId_low, userId_high,
                    articleId_low, articleId_high, type));
        }
        return result;
    }

    private void printRows(List<Row> toBePrinted){
        for (Row row: toBePrinted){
            long author = row.getColumns().get("UserID").asLong();
            long article = row.getColumns().get("ArticleID").asLong();
            long type = row.getColumns().get("ActionType").asLong();
            String message;

            switch (((int) type)){
                case ADD_ARTICLE:
                    message = "" + author + " writes article " + article + ", content: " + row.getColumns().get("content").asString();
                    break;
                case COMMENT_ARTICLE:
                    message = "" + author + " comments on article " + article + ", content: " + row.getColumns().get("content").asString();
                    break;
                case LIKE_ARTICLE:
                    message = "" + author + " like the article " + article;
                    break;
                default:
                    message = "empty";
            }

//            String message = (row.getColumns().get("UserId").asLong() + ", "
//                    + row.getColumns().get("ArticleId") + ", ");
//            message += "";

//            for (String key: row.getColumns().keySet()){
//                System.out.print(row.getColumns().get(key) + ",");
//            }
            System.out.println(message);
        }
    }

    public void queryArticle(int articleId){
//        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(sTableName);
//        RowPrimaryKey inclusiveStartKey = new RowPrimaryKey();
//        inclusiveStartKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MIN);
//        inclusiveStartKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.INF_MIN);
//        inclusiveStartKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId));
//        inclusiveStartKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(0));
//        RowPrimaryKey exclusiveEndKey = new RowPrimaryKey();
//        exclusiveEndKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MAX);
//        exclusiveEndKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.INF_MAX);
//        exclusiveEndKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(articleId + 1));
//        exclusiveEndKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(100));
//
//        criteria.setInclusiveStartPrimaryKey(inclusiveStartKey);
//        criteria.setExclusiveEndPrimaryKey(exclusiveEndKey);
//        criteria.setDirection(Direction.FORWARD);
//        criteria.setLimit(1);
//
//        GetRangeRequest request = new GetRangeRequest();
//        request.setRangeRowQueryCriteria(criteria);
//
//        try{
//            GetRangeResult result = mOTSClient.getRange(request);
//            printRows(result.getRows());
////            return result.getRows();
//        } catch (OTSException ots){
//            ots.printStackTrace();
//            System.out.println("OTSException");
//        } catch (ClientException cli){
//            System.out.println("ClientException");
//        }
        List<Row> result = queryRange(Integer.MIN_VALUE, Integer.MAX_VALUE, articleId, articleId + 1,
                ALL_OPERATIONS);
        printRows(result);
    }

    public void queryUser(int userId){
        printRows(queryRange(userId, userId + 1, Integer.MIN_VALUE, Integer.MAX_VALUE, ALL_OPERATIONS));
    }

    public void printAllRows(){
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(sTableName);
        RowPrimaryKey inclusiveStartKey = new RowPrimaryKey();
        inclusiveStartKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MIN);
        inclusiveStartKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(0));
        inclusiveStartKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(0));
        inclusiveStartKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(0));
        RowPrimaryKey exclusiveEndKey = new RowPrimaryKey();
        exclusiveEndKey.addPrimaryKeyColumn("Md5Count", PrimaryKeyValue.INF_MAX);
        exclusiveEndKey.addPrimaryKeyColumn("UserID", PrimaryKeyValue.fromLong(10));
        exclusiveEndKey.addPrimaryKeyColumn("ArticleID", PrimaryKeyValue.fromLong(10));
        exclusiveEndKey.addPrimaryKeyColumn("ActionType", PrimaryKeyValue.fromLong(10));

        criteria.setInclusiveStartPrimaryKey(inclusiveStartKey);
        criteria.setExclusiveEndPrimaryKey(exclusiveEndKey);
        criteria.setDirection(Direction.FORWARD);

        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);

        GetRangeResult result = mOTSClient.getRange(request);
        List<Row> rows = result.getRows();

        for (Row row: rows){
            for (String a : row.getColumns().keySet()){
                System.out.print(a + ": "+row.getColumns().get(a).toString() + ", ");
            }
            System.out.println();
        }
    }

    public void close(){
        if (mOTSClient != null)
            mOTSClient.shutdown();
    }

    public void clearAllContent(){
        DeleteTableRequest request = new DeleteTableRequest();
        request.setTableName(sTableName);
        try{
            mOTSClient.deleteTable(request);
            System.out.println("Delete the table successfully");
        } catch (Exception e){
            System.out.println("delete failure");
        }
    }
}
