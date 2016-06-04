package cn.edu.sjtu.cs.DBGroup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

/**
 * Created by gefei on 16-6-3.
 */
public class RandomTest {
    public static final String endPoint = "http://SJTUBlogOTS.cn-hangzhou.ots.aliyuncs.com";
    public static final String accessId = "PL2Bu1QHcXUnpiGd";
    public static final String accessKey = "Ho9qXXj7vzgRQYOnXzaOJdXkBYxeJd";
    public static final String instanceName = "SJTUBlogOTS";

    public static void main(String[] args) throws IOException{
        BlogSystem system = new BlogSystem(endPoint, accessId, accessKey, instanceName);
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

        system.printAllRows();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("-------------------");
        System.out.println("Your userID(only integer allowed): ");
        int userId = Integer.parseInt(br.readLine().trim());

        boolean endFlag = false;

        while (!endFlag) {
            System.out.println("Input the operation type: \n 0 - Add article \n 1 - Read article \n 2 - Query author \n 3 - End operation");
            int operation = Integer.parseInt(br.readLine().trim());
            int articleId;
            switch (operation) {
                case 0:
                    System.out.print("Your articleID(integer): ");
                    articleId = Integer.parseInt(br.readLine().trim());
                    System.out.println();
                    System.out.println("Add your article content here (only single line content is allowed): ");
                    String content = br.readLine();
                    system.addArticle(userId, articleId, content);
                    break;
                case 1:
                    System.out.print("ArticleId: ");
                    articleId = Integer.parseInt(br.readLine().trim());
                    System.out.println();
                    system.queryArticle(articleId);
                    System.out.println("operation you want to do?\n 0 - Comment \n 1 - like");
                    break;
                case 3:
                    endFlag = true;
                    break;
            }
        }

//        system.clearAllContent();
        system.close();
        System.out.println("Hello world");
    }
}
