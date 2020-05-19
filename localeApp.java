package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class localeApp2 {

	private static String uniqueID = UUID.randomUUID().toString();
	private static final String JAR_BUCKET_NAME = "jarbucketkonam2";
	private static final String TASK_BUCKET_NAME = "taskbucketkonam2";
	private static final String COMPLETED_TASK_BUCKET_NAME = "completedtaskskonam2";
	private static final String CONVERTED_FILES_BUCKET_NAME = "convertedfileskonam2";
	private static final String MANAGER_WORKER_TASK_QUEUE_URL = "konamMANAGER_WORKER_TASK_QUEUE_URL2" ;
	private static final String MANAGER_WORKER_DONE_QUEUE_URL = "konamMANAGER_WORKER_DONE_QUEUE_URL2" ;
	private static final String MANAGER_APP_TASK_QUEUE_URL = 	 "konamMANAGER_APP_QUEUE_URL2";
	private static final String MANAGER_APP_DONE_QUEUE_URL = "konamMANAGER_APP_DONE_QUEUE_URL2";
	private static final String INPUT_THREAD_QUEUE_NAME = "konamINPUT_THREAD_QUEUE_URL2";
	private static int NumberOfTasksPerWorker = (int)(Math.random()*100);
	private static final long TIMEOUT_LONG = (long)(Math.random()*20000 + 15159);//2400000; 30000 // 40 minutes
	private static int NUM_OF_TASKS = 0;
	private static String shouldTerminate = "false";
	private static String managerID ="";
	private static String inputDirectoryName = ""; //args[0]; 
	private static String jarDirectoryName = ""; //args[1]; 
	private static String outputDirectoryName = ""; //args[3]
	


	

	public static void main(String[] args) throws IOException {

		AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());

		AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		AmazonS3 s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build(); 

		try {
			 inputDirectoryName = args[0] + "\\";
			 jarDirectoryName = args[1] + "\\";
			 outputDirectoryName = args[2] + "\\";
			 NumberOfTasksPerWorker = Integer.parseInt(args[3]);
			if(args[4].equals("terminate"))
					shouldTerminate = "true";			
			
			
			System.out.println("Begining!!!! " + TIMEOUT_LONG);
			
			if(!checkBucketExist(s3,CONVERTED_FILES_BUCKET_NAME )) {
				createBucket(s3, CONVERTED_FILES_BUCKET_NAME);
			}
			if(!checkBucketExist(s3,COMPLETED_TASK_BUCKET_NAME )) {
				createBucket(s3, COMPLETED_TASK_BUCKET_NAME);	
			}
			if(!checkBucketExist(s3,JAR_BUCKET_NAME )) {
				createBucket(s3, JAR_BUCKET_NAME);
				uploadFileToBucket(s3, JAR_BUCKET_NAME, jarDirectoryName);	
			}			
			if(!checkBucketExist(s3,TASK_BUCKET_NAME ))
				createBucket(s3, TASK_BUCKET_NAME);
			checkAndCreateQs(sqs);
			uploadFileToBucketAndSendsMessegeToManager(s3, sqs, TASK_BUCKET_NAME, inputDirectoryName);
			while(!checkForMessagesAndTakeNeededAction(ec2, sqs, s3, outputDirectoryName) || NUM_OF_TASKS>0) {}
			managerID = returnManagerID(ec2);
			if(managerID != null && Boolean.parseBoolean(shouldTerminate)) {
				sendTerminateMsgToManager(sqs);	
				System.out.println("terminate massage sent!!");
				try {TimeUnit.SECONDS.sleep(100);}
				catch(Exception e){
					System.out.println(e.getMessage());
				}
				TerminateInstancesRequest deleteRequest = new TerminateInstancesRequest().withInstanceIds(managerID);
				ec2.terminateInstances(deleteRequest);
				System.out.println("closing manager!!");
			}
			System.out.println("ENDDDD!!!!!! ");



		}catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		} catch (Exception e) {
			System.out.println("Caught Exception: " + e.getMessage());
		}
	}


	// ============================ EC2 Functions ===========================//

	public static String runNewManager(AmazonEC2 ec2){
		try {
			// Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
			// make manager instance
			String userData = "#!/bin/bash\n" + 
					"aws s3 cp s3://" + JAR_BUCKET_NAME + "/manager.jar manager.jar\n" + 
					"java -jar manager.jar";    		
			String base64UserData = new String(Base64.getEncoder().encodeToString(userData.getBytes()));

			RunInstancesRequest request = new RunInstancesRequest("ami-1853ac65", 1, 1).withKeyName("amazonKey").withUserData(base64UserData);

			request.setInstanceType(InstanceType.T2Micro.toString());

			// assign iam rule
			IamInstanceProfileSpecification iamProfile = new IamInstanceProfileSpecification();
			iamProfile.setName("Manager_Access_Role");
			request.setIamInstanceProfile(iamProfile); // attach IAM access role to manager instance

			// tags
			List<Tag> tagsList = new ArrayList<Tag>();
			tagsList.add(new Tag().withKey("Type").withValue("Manager"));
			tagsList.add(new Tag().withKey("Name").withValue("Manager"));
			TagSpecification tagSpec = new TagSpecification().withTags(tagsList).withResourceType("instance");
			request.setTagSpecifications(Arrays.asList(tagSpec));

			// run manager instance
			System.out.println("new mannager was created!!" );
			return ec2.runInstances(request).getReservation().getInstances().get(0).getInstanceId();
			

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
		return null;	
	}

	public static Boolean managerCheck(AmazonEC2 ec2) {
		Boolean managerExcists = false;
		List<Reservation> reservationsList = ec2.describeInstances().getReservations();
		for(Reservation reservation: reservationsList) {
			List<Instance> instancesList = reservation.getInstances();
			for(Instance instance : instancesList) {
				for(Tag tag : instance.getTags()) {
					if(tag.getKey().equals("Type") && tag.getValue().equals("Manager")){
						if(instance.getState().getCode() == 16 || instance.getState().getCode() == 0) {
							managerExcists = true;
							break; // a manager is running
						}
						else if (instance.getState().getCode() == 64 || instance.getState().getCode() == 80) {
							TerminateInstance(instance, ec2);
						}// if an instance is ending, delet the instance
					}    				
				}
				if(managerExcists == true) break;
			}
			if(managerExcists == true) break;
		}
		return managerExcists;
	}
	
	public static String returnManagerID(AmazonEC2 ec2) {
		boolean managerExcists = false;
		String managerID = null;
		List<Reservation> reservationsList = ec2.describeInstances().getReservations();
		for(Reservation reservation: reservationsList) {
			List<Instance> instancesList = reservation.getInstances();
			for(Instance instance : instancesList) {
				for(Tag tag : instance.getTags()) {
					if(tag.getKey().equals("Type") && tag.getValue().equals("Manager")){
						if(instance.getState().getCode() == 16 || instance.getState().getCode() == 0) {
							managerID = instance.getInstanceId();
							managerExcists = true;
							break; // a manager is running
						}
						else if (instance.getState().getCode() == 64 || instance.getState().getCode() == 80) {
							TerminateInstance(instance, ec2);
						}// if an instance is ending, delet the instance
					}    				
				}
				if(managerExcists == true) break;
			}
			if(managerExcists == true) break;
		}
		return managerID;
	}

	private static void TerminateInstance(Instance managerInstance, AmazonEC2 ec2) {
		TerminateInstancesRequest deleteRequest = new TerminateInstancesRequest().withInstanceIds(managerInstance.getInstanceId());
		ec2.terminateInstances(deleteRequest);
	}


	// ============================ S3 Functions ============================//

	public static void createBucket(AmazonS3 s3, String bucketName){
		try {
			s3.createBucket(bucketName);
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} 
	}
	
	public static void uploadFileToBucket(AmazonS3 s3, String bucketName, String inputDirectoryName){

		try {
			File dir = new File(inputDirectoryName);
			for (File file : dir.listFiles()) {
				String key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
				PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
				s3.putObject(req);
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} 

	}

	public static void uploadFileToBucketAndSendsMessegeToManager(AmazonS3 s3,AmazonSQS sqs, String bucketName, String inputDirectoryName){

		try {
			File dir = new File(inputDirectoryName);
			for (File file : dir.listFiles()) {
				String key = (file.getName()+uniqueID).replace('\\', '_').replace('/','_').replace(':', '_');
				PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
				s3.putObject(req);
				NUM_OF_TASKS++;
				sendMassegeToQuque(sqs, MANAGER_APP_TASK_QUEUE_URL, key, NumberOfTasksPerWorker);//3 mission per worker
				
			}
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} 

	}

	public static void downlaodObjectFromBucket(AmazonS3 s3, String bucketName, String key, String outputFileName){
		try {
			System.out.println("Downloading an object");
			String prefixOfBucket = "https://s3.amazonaws.com/completedtaskskonam/";
			key = key.substring(prefixOfBucket.length()+1, key.length());
			System.out.println("Bucketname: " + bucketName);
			System.out.println("key: " + key);
			s3.getObject(new GetObjectRequest(bucketName, key), new File(outputFileName));
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}		
	}

	public static void deleteFileFromBucket(AmazonS3 s3, String bucketName, String key){

		/*
		 * Delete an object - Unless versioning has been turned on for your bucket,
		 * there is no way to undelete an object, so use caution when deleting objects.
		 */
		try {
			System.out.println("Deleting an object\n");
			s3.deleteObject(bucketName, key);          
			System.out.println();
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	public static void deleteBucket(AmazonS3 s3, String bucketName){
		/*
		 * Delete a bucket - A bucket must be completely empty before it can be
		 * deleted, so remember to delete any objects from your buckets before
		 * you try to delete them.
		 */
		try {
			System.out.println("Deleting bucket " + bucketName + "\n");
			s3.deleteBucket(bucketName); 
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}

	}

	public static boolean checkBucketExist(AmazonS3 s3, String bucketName) {
		for (Bucket bucket : s3.listBuckets()) {
			if(bucket.getName().equals(bucketName)) {
				System.out.println("bucket found, no need to create new bucket: " + bucketName);
				return true; 	
			}
		}
		System.out.println("bucket not found, creates new bucket: " + bucketName);
		return false;
	}

	//============================ SQS Functions ===========================//

	public static boolean checkQ(AmazonSQS sqs ,String QName) {
		return sqs.listQueues().getQueueUrls().contains(QName);
	}

	public static void createQ(AmazonSQS sqs ,String QName){
		System.out.println("Creating a new SQS queue called " + QName + "\n");
		CreateQueueRequest appToManagerQueueRequest = new CreateQueueRequest(QName);
		sqs.createQueue(appToManagerQueueRequest);
		System.out.println(QName + " was Created\n");
	}

	public static void checkAndCreateQs(AmazonSQS sqs) {
		if(!checkQ(sqs, MANAGER_APP_TASK_QUEUE_URL)) {
			createQ(sqs, MANAGER_APP_TASK_QUEUE_URL);
		}else System.out.println(MANAGER_APP_TASK_QUEUE_URL + " Already exists!\n");
		if(!checkQ(sqs, MANAGER_WORKER_DONE_QUEUE_URL)) {
			createQ(sqs, MANAGER_WORKER_DONE_QUEUE_URL);
		}else System.out.println(MANAGER_WORKER_DONE_QUEUE_URL + " Already exists!\n");
		if(!checkQ(sqs, INPUT_THREAD_QUEUE_NAME)) {
			createQ(sqs, INPUT_THREAD_QUEUE_NAME);
		}else System.out.println(INPUT_THREAD_QUEUE_NAME + " Already exists!\n");
		if(!checkQ(sqs, MANAGER_APP_DONE_QUEUE_URL)) {
			createQ(sqs, MANAGER_APP_DONE_QUEUE_URL);
		}else System.out.println(MANAGER_APP_DONE_QUEUE_URL + " Already exists!\n");
		if(!checkQ(sqs, MANAGER_WORKER_TASK_QUEUE_URL)) {
			createQ(sqs, MANAGER_WORKER_TASK_QUEUE_URL);
		}else System.out.println(MANAGER_WORKER_TASK_QUEUE_URL + " Already exists!\n");

	}
	
	public static void checkAndPurgeQs(AmazonSQS sqs) {
		if(!checkQ(sqs, MANAGER_APP_TASK_QUEUE_URL)) {
			sqs.purgeQueue(new PurgeQueueRequest(MANAGER_APP_TASK_QUEUE_URL));
		}else System.out.println(MANAGER_APP_TASK_QUEUE_URL + " Already purged!\n");
		if(!checkQ(sqs, MANAGER_WORKER_DONE_QUEUE_URL)) {
			sqs.purgeQueue(new PurgeQueueRequest(MANAGER_WORKER_DONE_QUEUE_URL));
		}else System.out.println(MANAGER_WORKER_DONE_QUEUE_URL + " Already purged!\n");
		if(!checkQ(sqs, INPUT_THREAD_QUEUE_NAME)) {
			sqs.purgeQueue(new PurgeQueueRequest(INPUT_THREAD_QUEUE_NAME));
		}else System.out.println(INPUT_THREAD_QUEUE_NAME + " Already purged!\n");
		if(!checkQ(sqs, MANAGER_APP_DONE_QUEUE_URL)) {
			sqs.purgeQueue(new PurgeQueueRequest(MANAGER_APP_DONE_QUEUE_URL));
		}else System.out.println(MANAGER_APP_DONE_QUEUE_URL + " Already purged!\n");
		if(!checkQ(sqs, MANAGER_WORKER_TASK_QUEUE_URL)) {
			sqs.purgeQueue(new PurgeQueueRequest(MANAGER_WORKER_TASK_QUEUE_URL));
		}else System.out.println(MANAGER_WORKER_TASK_QUEUE_URL + " Already purged!\n");

	}

	public static void sendMassegeToQuque(AmazonSQS sqs, String queueURL, String inputFileName, int NumberOfTasksPerWorker){
		try {

			System.out.println("Sending a message to " + queueURL.toString() +".\n");
			final Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue("new PDF task"));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(uniqueID));
			messageAttributes.put("BucketName",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(TASK_BUCKET_NAME));
			messageAttributes.put("FileName",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(inputFileName));
			messageAttributes.put("numOfWorkersPerInputFile",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(String.valueOf(NumberOfTasksPerWorker)));
			messageAttributes.put("shouldTerminate",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue("false"));
			messageAttributes.put("numOfTask",new MessageAttributeValue()
					.withDataType("String")
					.withStringValue(String.valueOf(NUM_OF_TASKS)));

			final SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("new task file");
			sendMessageRequest.withQueueUrl(queueURL);
			sendMessageRequest.withMessageAttributes(messageAttributes);
			sqs.sendMessage(sendMessageRequest);

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	private static void sendTerminateMsgToManager(AmazonSQS sqs){
		final Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("Type", new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("new PDF task"));
		messageAttributes.put("localAppID", new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("1"));
		messageAttributes.put("BucketName",new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("1"));
		messageAttributes.put("FileName",new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("1"));
		messageAttributes.put("numOfWorkersPerInputFile",new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("1"));
		messageAttributes.put("shouldTerminate",new MessageAttributeValue()
				.withDataType("String")
				.withStringValue("true"));
		messageAttributes.put("numOfTask",new MessageAttributeValue()
				.withDataType("String")
				.withStringValue(String.valueOf(NUM_OF_TASKS)));


		final SendMessageRequest sendMessageRequest = new SendMessageRequest();
		sendMessageRequest.withMessageBody("terminate");
		sendMessageRequest.withQueueUrl(MANAGER_APP_TASK_QUEUE_URL);
		sendMessageRequest.withMessageAttributes(messageAttributes);
		sqs.sendMessage(sendMessageRequest);	
	}

	public static List<Message> getMassageFromQueue(AmazonSQS sqs, String queueURL) {

		try {
			System.out.println(" Trying to receive messages from " + queueURL.toString() + ".\n");
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL)
					.withMaxNumberOfMessages(1)
					.withVisibilityTimeout(0);
			List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
			return messages;
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		return null;
	}

	public static void deleteMassageFromQueue(AmazonSQS sqs, String messageRecieptHandle, String queueURL) {

		try {

			System.out.println("Deleting a message.\n");
			sqs.deleteMessage(new DeleteMessageRequest(queueURL, messageRecieptHandle));

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}

	public void deleteQueue(AmazonSQS sqs, String queueURL) {

		try {


			// Delete a queue
			System.out.println("Deleting the test queue.\n");
			sqs.deleteQueue(new DeleteQueueRequest(queueURL));

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered " +
					"a serious internal problem while trying to communicate with SQS, such as not " +
					"being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}


	//============================ local App Functions ===========================//	

	private static boolean checkForMessagesAndTakeNeededAction(AmazonEC2 ec2, AmazonSQS sqs, AmazonS3 s3, String outputFileName) throws IOException {
		long lastMsgTime = System.currentTimeMillis();
		while(true) {
			if(System.currentTimeMillis() > lastMsgTime + TIMEOUT_LONG) {
				if(!managerCheck(ec2)) {
					uniqueID = UUID.randomUUID().toString();
					NUM_OF_TASKS = 0;
					uploadFileToBucketAndSendsMessegeToManager(s3, sqs, TASK_BUCKET_NAME, inputDirectoryName);
					managerID =runNewManager(ec2);
					System.out.println("managaer was down start new manager" + "managerID:"+managerID);
					return false;
				}
				lastMsgTime = System.currentTimeMillis();
			}

			List<Message> messages = getMassageFromQueue(sqs, MANAGER_APP_DONE_QUEUE_URL);

			if(!messages.isEmpty()) {
				return downloadMassegesAndClearSQSAndBuckets(sqs, s3, messages, outputFileName);
				 
			} else {
				try {TimeUnit.SECONDS.sleep(5);}
				catch(Exception e){
					System.out.println(e.getMessage());
				}
			}
		}
	}


	private static boolean downloadMassegesAndClearSQSAndBuckets(AmazonSQS sqs, AmazonS3 s3, List<Message> messages, String outputFileName) throws IOException {
		for (Message message : messages) {
			Map<String,MessageAttributeValue> messageAttributes = message.getMessageAttributes();
			if(messageAttributes.containsKey("Type") && messageAttributes.get("Type").getStringValue().equals("done task")
					&& messageAttributes.containsKey("BucketName") 
					&& messageAttributes.containsKey("FileName")
					&& messageAttributes.containsKey("localAppID"))
			{
				String bucketName = messageAttributes.get("BucketName").getStringValue();
				String FileBucketUrl = messageAttributes.get("FileName").getStringValue();
				String localAppID1 = messageAttributes.get("localAppID").getStringValue();

				System.out.println("Local app got done task massage");
				System.out.println("BucketNAme : " + bucketName + " FileBucketUrl: " + FileBucketUrl);
				System.out.println("localAppID : "+ localAppID1);
				System.out.println("downloading file");

				//DownloadFile
				if(localAppID1.equals(uniqueID)) {
				downlaodObjectFromBucket(s3,bucketName, FileBucketUrl, outputFileName + "inputTest" + uniqueID +".txt");
				System.out.println("creating File " + outputFileName + "inputTest" + uniqueID +".txt");
				}else {
					System.out.println("nnot correct local app ID");
					return false;
				}
				
				try {InputStream inputstream = new FileInputStream(outputFileName + "inputTest" + uniqueID +".txt");
	            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputstream));
	            String line;
	            PrintWriter out = new PrintWriter(outputFileName + "inputTest" + uniqueID + ".HTML", "UTF-8");
	            out.println("<html>\n");
	            out.println("    <h2>Distriduted System Programming : Assignment 1</h2>\n" +
	            			"    <h3>Results of LocalApp ID : " + uniqueID + "</h3> <br>");
	            out.println("<body>");

	            while ((line = bufferedReader.readLine()) != null)
	                out.println(line + "<br>");
	            bufferedReader.close();

	            out.println("</body>\n</html>");
	            out.close();

				} catch (Exception ex){
	            ex.printStackTrace();
				}
				//deleting task from manager queue
				System.out.println("deleteing message");
				deleteMassageFromQueue(sqs, message.getReceiptHandle(),MANAGER_APP_DONE_QUEUE_URL);
				NUM_OF_TASKS--;
				return true;
			}
		}
		return false;
	}

}







