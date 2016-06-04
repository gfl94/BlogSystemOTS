package cn.edu.sjtu.cs.DBGroup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
/**
 * Created by gefei on 16-6-4.
 */
public class UnitTest {
    public static final String endPoint = "http://SJTUBlogOTS.cn-hangzhou.ots.aliyuncs.com";
    public static final String accessId = "PL2Bu1QHcXUnpiGd";
    public static final String accessKey = "Ho9qXXj7vzgRQYOnXzaOJdXkBYxeJd";
    public static final String instanceName = "SJTUBlogOTS";

    public static void main(String[] args) throws IOException{
        BlogSystem system = new BlogSystem(endPoint, accessId, accessKey, instanceName);
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());


        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        system.addArticle(1, 1001, "aaaa");
        system.addArticle(2, 1002, "bbbb");
        system.addArticle(3, 1001, "cccc");
        System.out.println("-------------------");

        system.commentArticle(1003, 1, "good job");
        system.likeArticle(1001, 3);

        system.queryArticle(3);
        system.queryUser(1001);


        system.close();
        System.out.println("Hello world");
    }
}
