/*
 * Copyright 2010-2011 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.BatchDeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeletableItem;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

public class SimpleDBSample {

    public static void main(String[] args) throws Exception {

        AmazonSimpleDB sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
                SimpleDBSample.class.getResourceAsStream("AwsCredentials.properties")));
        //リージョンのエンドポイント指定
        sdb.setEndpoint(args[0]);
        
        int roopCount = 100;
        long insertTimeSum = 0;
        long selectTimeSum = 0;
        long updateTimeSum = 0;
        long deleteTimeSum = 0;
        long startTime;
        long finishedTime;
        
        try {
            // ドメインの作成
            String myDomain = "MyStore";
            sdb.createDomain(new CreateDomainRequest(myDomain));
            
            for(int i=0;i<roopCount;i++){
	            // 初期データの投入
	            sdb.batchPutAttributes(new BatchPutAttributesRequest(myDomain, createSampleData()));
	            
	            // データの1件挿入(PutAttributes API)
	            ReplaceableItem rItem = new ReplaceableItem("Item_06").withAttributes(
	                    new ReplaceableAttribute("Category", "Car Parts", true),
	                    new ReplaceableAttribute("Subcategory", "Exhaust", true),
	                    new ReplaceableAttribute("Name", "O2 Sensor", true),
	                    new ReplaceableAttribute("Make", "Audi", true),
	                    new ReplaceableAttribute("Model", "TT Coupe", true),
	                    new ReplaceableAttribute("Year", "2009", true),
	                    new ReplaceableAttribute("Year", "2010", true),
	                    new ReplaceableAttribute("Year", "2011", true));
	            PutAttributesRequest putAttributesRequest = new PutAttributesRequest(myDomain,rItem.getName() ,rItem.getAttributes());
	            startTime = System.currentTimeMillis();
	            sdb.putAttributes(putAttributesRequest);
	            finishedTime = System.currentTimeMillis();
	            insertTimeSum += finishedTime - startTime;
	            
	            // データの取得（Select API)
	            String selectExpression = "select * from `" + myDomain + "` where Category = 'Clothes'";
	        	SelectRequest selectRequest = new SelectRequest(selectExpression);
	        	startTime = System.currentTimeMillis();
	        	sdb.select(selectRequest);
	        	finishedTime = System.currentTimeMillis();
	        	selectTimeSum += finishedTime - startTime;
	
	            // データの更新(PutAttributes API)
	            List<ReplaceableAttribute> replaceableAttributes = new ArrayList<ReplaceableAttribute>();
	            replaceableAttributes.add(new ReplaceableAttribute("Size", "Medium", true));
	            startTime = System.currentTimeMillis();
	            sdb.putAttributes(new PutAttributesRequest(myDomain, "Item_03", replaceableAttributes));
	            finishedTime = System.currentTimeMillis();
	            updateTimeSum += finishedTime - startTime;
	            
	            // データの削除(DeleteAttributes API)
	            startTime = System.currentTimeMillis();
	            sdb.deleteAttributes(new DeleteAttributesRequest(myDomain, "Item_03"));
	            finishedTime = System.currentTimeMillis();
	            deleteTimeSum += finishedTime - startTime;
	        	
	        	// データの削除
	            sdb.batchDeleteAttributes(new BatchDeleteAttributesRequest(myDomain,deleteSampleData()));
            }
            //ドメインの削除
            sdb.deleteDomain(new DeleteDomainRequest(myDomain));
            
            System.out.println("insert:AVG:"+(float)insertTimeSum/roopCount);
            System.out.println("select:AVG:"+(float)selectTimeSum/roopCount);
            System.out.println("update:AVG:"+(float)updateTimeSum/roopCount);
            System.out.println("delete:AVG:"+(float)deleteTimeSum/roopCount);
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon SimpleDB, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with SimpleDB, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
    
    /**
     * Creates an array of SimpleDB DeletableItems.
     *
     * @return An array of delete item data.
     */
    private static List<DeletableItem> deleteSampleData() {
    	List<DeletableItem> deleteData = new ArrayList<DeletableItem>();
    	deleteData.add(new DeletableItem().withName("Item_01"));
    	deleteData.add(new DeletableItem().withName("Item_02"));
    	deleteData.add(new DeletableItem().withName("Item_04"));
    	deleteData.add(new DeletableItem().withName("Item_05"));
    	deleteData.add(new DeletableItem().withName("Item_06"));
    	return deleteData;
    }

    /**
     * Creates an array of SimpleDB ReplaceableItems populated with sample data.
     *
     * @return An array of sample item data.
     */
    private static List<ReplaceableItem> createSampleData() {
        List<ReplaceableItem> sampleData = new ArrayList<ReplaceableItem>();

        sampleData.add(new ReplaceableItem("Item_01").withAttributes(
                new ReplaceableAttribute("Category", "Clothes", true),
                new ReplaceableAttribute("Subcategory", "Sweater", true),
                new ReplaceableAttribute("Name", "Cathair Sweater", true),
                new ReplaceableAttribute("Color", "Siamese", true),
                new ReplaceableAttribute("Size", "Small", true),
                new ReplaceableAttribute("Size", "Medium", true),
                new ReplaceableAttribute("Size", "Large", true)));

        sampleData.add(new ReplaceableItem("Item_02").withAttributes(
                new ReplaceableAttribute("Category", "Clothes", true),
                new ReplaceableAttribute("Subcategory","Pants", true),
                new ReplaceableAttribute("Name", "Designer Jeans", true),
                new ReplaceableAttribute("Color", "Paisley Acid Wash", true),
                new ReplaceableAttribute("Size", "30x32", true),
                new ReplaceableAttribute("Size", "32x32", true),
                new ReplaceableAttribute("Size", "32x34", true)));

        sampleData.add(new ReplaceableItem("Item_03").withAttributes(
                new ReplaceableAttribute("Category", "Clothes", true),
                new ReplaceableAttribute("Subcategory", "Pants", true),
                new ReplaceableAttribute("Name", "Sweatpants", true),
                new ReplaceableAttribute("Color", "Blue", true),
                new ReplaceableAttribute("Color", "Yellow", true),
                new ReplaceableAttribute("Color", "Pink", true),
                new ReplaceableAttribute("Size", "Large", true),
                new ReplaceableAttribute("Year", "2006", true),
                new ReplaceableAttribute("Year", "2007", true)));

        sampleData.add(new ReplaceableItem("Item_04").withAttributes(
                new ReplaceableAttribute("Category", "Car Parts", true),
                new ReplaceableAttribute("Subcategory", "Engine", true),
                new ReplaceableAttribute("Name", "Turbos", true),
                new ReplaceableAttribute("Make", "Audi", true),
                new ReplaceableAttribute("Model", "S4", true),
                new ReplaceableAttribute("Year", "2000", true),
                new ReplaceableAttribute("Year", "2001", true),
                new ReplaceableAttribute("Year", "2002", true)));

        sampleData.add(new ReplaceableItem("Item_05").withAttributes(
                new ReplaceableAttribute("Category", "Car Parts", true),
                new ReplaceableAttribute("Subcategory", "Emissions", true),
                new ReplaceableAttribute("Name", "O2 Sensor", true),
                new ReplaceableAttribute("Make", "Audi", true),
                new ReplaceableAttribute("Model", "S4", true),
                new ReplaceableAttribute("Year", "2000", true),
                new ReplaceableAttribute("Year", "2001", true),
                new ReplaceableAttribute("Year", "2002", true)));

        return sampleData;
    }
}
