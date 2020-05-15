package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	private String[] portNumbers = {"5554", "5556", "5558", "5560", "5562"};
	public Uri uri = null;
	int process_id;
	int indexOfCurrentNode;
	static String nodeId = null;

	static String portStr = null;

	private String allAvb = "all";

	private static String insertKV = "INSERT_KV";
	private static String insAck = "ACK_INS";
	private static String replicaKV = "REPLICA_KV";
	private static String replicaReply = "REPLICA_REPLY";
	private static String orignalKV = "ORIGINAL_KV";
	private static String orignalReply = "ORIGINAL_REPLY";
	private static String replyexp = "Expect_REPLY";
	private static String query = "QUERY";
	private static String queryReply = "QUERY_REPLY";
	private static String sendOK = "SEND_OK";
	private static String recvOK = "RECV_OK";
	private static String queryAll = "QUERY_ALL";
	private static String queryAllReply = "QUERY_ALL_REPLY";
	private static String delAll = "DELETE_ALL";
	private static String del = "DELETE";

	private String sendTo = "send_to";
	private String queryFrom = "query_from";


	//ports of preds and succ
	String pred1 = null;
	String pred2 = null;
	String succ1 = null;
	String succ2 = null;


	private String msg = null;
	private String delim = "$!";


	private ArrayList<String> allNodes = new ArrayList<String>();
	private HashMap<String,String> senderMap = new HashMap();
	private HashMap<String,String> queryAllR = new HashMap();
	private HashMap<String,String> queryR = new HashMap();
	private HashMap<String,String> replicas = new HashMap();
	private HashMap<String,String> orignals = new HashMap();


	private int queriesaccepted = 0;
	private int expectedReplies = 0;
	private int queryaccepted = 0;
	//private int insertDone = 0;
	private int replicaExpected = 0;
	private int replicaAccepted = 0;
	private int orignalExpected = 0;
	private int orignalAccepted = 0;

	//exp locks
	  ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
	  Lock readLock = readWriteLock.readLock();
	Lock writeLock = readWriteLock.writeLock();

	//ReentrantLock readLock = new ReentrantLock(true);
	  //ReentrantLock writeLock = new ReentrantLock(true);

	public boolean fileDelete(File file){
		return file.delete();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String hashKey = null;
		//ReentrantLock r = new ReentrantLock();
		try {
			hashKey  = genHash(selection);

		} catch(NoSuchAlgorithmException e ) {
			Log.e("delete", "No Such Algorithm exception caught");
			e.printStackTrace();
		}
		Log.d("del", "selection: "+selection+"  selectionArgs: "+selectionArgs);

		if(selection.equals("@"))
		{
			writeLock.lock();
			try {
				//r.lock();
				String[] fileList = getContext().fileList();
				int files = getContext().fileList().length;
				File dir = getContext().getFilesDir();
				for (int i = 0; i < files; i++) {
					File file = new File(dir, fileList[i]);
					//boolean deleted = file.delete();
					//writeLock.lock();
					boolean deleted = fileDelete(file);
					Log.d("Delete file :", "filename : " + fileList[i] + " deleteflag : " + deleted);
					//writeLock.unlock();
				}
			}catch (Exception e) {
				Log.e("exception", "File delete failed");
				e.printStackTrace();
			} finally {
				//r.unlock();
				writeLock.unlock();
			}
			return 1;
		}

		else if(selection.equals("*"))
		{
			String reply = delAll + delim + portStr + delim +"@";
			String[] toClient = {allAvb,reply};
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
			try {
				Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
			}catch(Exception e){
				Log.e("Failed Client Task", "error: "+e.getMessage());
				e.printStackTrace();
			}
			return 1;
		}

		else {
			String keyStoreS = senderMap.get(allNodes.get(0));
			int insIndex = 0;
			for(int i =0; i<allNodes.size(); i++){
				if(i == 0) {
					if (hashKey.compareTo(allNodes.get(i)) >= 0 && hashKey.compareTo(allNodes.get(allNodes.size() - 1)) > 0) {

						keyStoreS = senderMap.get(allNodes.get(i));
						insIndex = i;
						break;
					}
				}
				else if((hashKey.compareTo(allNodes.get(i)) <= 0 && hashKey.compareTo(allNodes.get(i-1)) > 0)){
					keyStoreS = senderMap.get(allNodes.get(i));
					insIndex = i;
				}
			}
			Log.d("delete for", "port" + keyStoreS+" s:"+selection);

			String[] toClient;
			String delKV = del + delim + portStr + delim + selection;

			String keyStoreS1 = senderMap.get(allNodes.get((5 + 1 + insIndex) % 5));
			String keyStoreS2 = senderMap.get(allNodes.get((5 + 2 + insIndex) % 5));

//exp
			if(keyStoreS.equals(portStr) ||  keyStoreS1.equals(portStr) || keyStoreS2.equals(portStr))
			{ // || selectionArgs != null
				writeLock.lock();
				try {
					//r.lock();
					File dir = getContext().getFilesDir();
					File file = new File(dir, selection);
					//boolean deleted = file.delete();
					//writeLock.lock();
					boolean deleted = fileDelete(file);
					//writeLock.unlock();
					Log.v("file deleted", selection + "deleteFlag : "+deleted);


				} catch (Exception e) {
					Log.e("exception", "File delete failed");
					e.printStackTrace();
				}finally {
					//r.unlock();
					writeLock.unlock();
				}
				//toClient = new String[]{sendTo, Integer.toString(2), keyStoreS2, keyStoreS, delKV};
			}

			if(selectionArgs == null) {
				Log.d("sending del", "to:"+keyStoreS2+" "+keyStoreS1+" "+keyStoreS);
				toClient = new String[]{sendTo, Integer.toString(3), keyStoreS2, keyStoreS1, keyStoreS, delKV};
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
				try {
					Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
					Log.d("another log", "bites the dust");
				}catch(Exception e){
					Log.e("Failed Client Task", "error: "+e.getMessage());
					e.printStackTrace();
				}
			}
			return 1;
		}

			//return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void insertLocal(Uri uri, ContentValues values){
		FileOutputStream fileOutputStream;
		//ReentrantLock r = new ReentrantLock();
		writeLock.lock();
		try {
			//r.lock();
			//writeLock.lock();
			fileOutputStream = getContext().openFileOutput(values.get("key").toString(), Context.MODE_PRIVATE);
			fileOutputStream.write(values.get("value").toString().getBytes());
			//writeLock.unlock();
			fileOutputStream.flush();
			fileOutputStream.close();
		} catch (Exception e) {
			Log.e("Error", e.getMessage());
		} finally {
			//r.unlock();
			writeLock.unlock();
		}
		Log.v("insert", values.toString());
		Log.d("insert:", "key: "+values.get("key").toString()+" at port_str: "+ portStr);
		return;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String hashKey = null;
		try {
			hashKey  = genHash(values.get("key").toString());

		} catch(NoSuchAlgorithmException e ) {
			Log.e("insert", "No Such Algorithm exception caught");
			e.printStackTrace();
		}

		String keyStoreS = senderMap.get(allNodes.get(0));
		int insIndex = 0;
		for(int i =0; i<allNodes.size(); i++){
			if(i == 0) {
				if (hashKey.compareTo(allNodes.get(i)) >= 0 && hashKey.compareTo(allNodes.get(allNodes.size() - 1)) > 0) {

					keyStoreS = senderMap.get(allNodes.get(i));
					insIndex = i;
					break;
				}
			}
			else if((hashKey.compareTo(allNodes.get(i)) <= 0 && hashKey.compareTo(allNodes.get(i-1)) > 0)){
				keyStoreS = senderMap.get(allNodes.get(i));
				insIndex = i;
			}
		}
		Log.d("send insert to", ""+ keyStoreS);

		String[] toClient;

		String keyStoreS1 = senderMap.get(allNodes.get((5 + 1 + insIndex)%5));
		String keyStoreS2 = senderMap.get(allNodes.get((5 + 2 + insIndex)%5));

		Log.d("sending to ports", "1. "+keyStoreS+" 2. "+keyStoreS1+" 3. "+keyStoreS2);

		String insertKv = insertKV +delim  + portStr + delim + values.get("key").toString() + delim + values.get("value").toString();

		if(keyStoreS.equals(portStr)){
			toClient = new String[]{sendTo, Integer.toString(2), keyStoreS1, keyStoreS2, insertKv};
			insertLocal(null, values);
		}else {
			toClient = new String[]{sendTo, Integer.toString(3), keyStoreS, keyStoreS1, keyStoreS2, insertKv};
		}
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
		try {
			Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
		}catch(Exception e){
			Log.e("Failed Client Task", "error: "+e.getMessage());
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Log.d("Change", "4321**** readWritelock");
		//local sender Map & ordered list of nodes
		for ( int i = 0 ; i<portNumbers.length ; i++) {

			try {
				String temp = genHash(portNumbers[i]);
				senderMap.put(temp, portNumbers[i]);
				allNodes.add(temp);

			} catch(NoSuchAlgorithmException e ) {
				Log.e("Generate Sender Map", "No Such Algorithm exception caught ");
				e.printStackTrace();
			}
		}
		Log.d("sender map", ""+ Collections.singletonList(senderMap));
		allNodes = sortlist(allNodes);
		Log.d("all nodes", ""+ Collections.singletonList(allNodes));


		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4); //example 5554
		process_id = Integer.parseInt(portStr);
		Log.d("port_str", portStr);

		Uri.Builder builder = new Uri.Builder();
		builder.scheme("content").authority("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
		uri = builder.build();

		//get nodeId
		try {
			nodeId = genHash(portStr);
		} catch (NoSuchAlgorithmException e) {
			Log.e("getHash", "On create : No Such Algorithm exception caught ");
			e.printStackTrace();
		}

		//delete(null, "@", new String[]{"noFwding"});

		indexOfCurrentNode = allNodes.indexOf(nodeId);
		//pred1 & pred2 for recovery
		pred1 = senderMap.get(allNodes.get((5 - 1 + indexOfCurrentNode)%5));
		pred2 = senderMap.get(allNodes.get((5 - 2 + indexOfCurrentNode)%5));
		//succ1 and succ2 for replication
		succ1 = senderMap.get(allNodes.get((5 + 1 + indexOfCurrentNode)%5));
		succ2 = senderMap.get(allNodes.get((5 + 2 + indexOfCurrentNode)%5));

		//create server socket
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("serverSocket", "Can't create a ServerSocket");
			return false;
		}

		//server may be joining for 1st time or rejoining?

		//code...?



		//if server is rejoining
		String replicaKv = replicaKV+delim+portStr+delim+"@";
		String[] toClient;
		Log.d("join query", "to: "+ pred1 +" "+ succ1 +" "+succ2);
		//toClient = new String[]{queryFrom, Integer.toString(3), pred1,succ1,succ2, replicaKv};
		toClient = new String[]{sendTo, Integer.toString(1), pred1, replicaKv};
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
		try {
			Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
			Log.d("return rcvd", "Integer i "+i);
		}catch(Exception e){
			Log.e("Failed Client Task", "error: "+e.getMessage());
			e.printStackTrace();
		}


		toClient = new String[]{sendTo, Integer.toString(1), succ1, replicaKv};
		//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
		try {
			Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
		}catch(Exception e){
			Log.e("Failed Client Task", "error: "+e.getMessage());
			e.printStackTrace();
		}

		toClient = new String[]{sendTo, Integer.toString(1), succ2, replicaKv};
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
		try {
			Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
		}catch(Exception e){
			Log.e("Failed Client Task", "error: "+e.getMessage());
			e.printStackTrace();
		}

		try {
			Thread.sleep(100);
		}catch(InterruptedException e) {
			e.printStackTrace();
		}


		//replicaAccepted = 0;

		/*synchronized (this ) {
			replicaAccepted = 0;
			replicaExpected = 3;
		}

		while(replicaAccepted < replicaExpected)
		{
			Log.d("waiting","to reInsert node");
		}

		insertAllLocal();
		*/

		//insert check send rcv
/*
		if(portStr.equals("5554")) {
			String[] toClient;
			String keyStoreS = senderMap.get(allNodes.get(0));
			String keyStoreS1 = senderMap.get(allNodes.get((5 + 1 + 0) % 5));
			String keyStoreS2 = senderMap.get(allNodes.get((5 + 2 + 0) % 5));

			Log.d("sending to ports", "1. " + keyStoreS + " 2. " + keyStoreS1 + " 3. " + keyStoreS2);

			String insertKv = insertKV + delim + portStr + delim + "mad" + delim + "lad";
			toClient = new String[]{sendTo, Integer.toString(3), keyStoreS, keyStoreS1, keyStoreS2, insertKv};
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
		}

 */



		//end exp....

		return true;
	}

	private ArrayList sortlist(ArrayList input)
	{
		Collections.sort(input, new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				return s1.compareTo(s2);
			}
		});
		return input;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub

		String hashKey = null;

		//ReentrantLock r = new ReentrantLock();

		try {
			hashKey  = genHash(selection);

		} catch(NoSuchAlgorithmException e ) {
			Log.e("insert", "No Such Algorithm exception caught");
			e.printStackTrace();
		}

		Log.d("query", "query for "+selection+ " generated");
		if(selection.equals("@"))
		{
			String[] filelist = getContext().fileList();
			int files = getContext().fileList().length;
			MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
			for (int i = 0; i < files; i++) {
				String fileN = filelist[i];
				//StringBuffer file
				String msgValue = "";
				readLock.lock();
				try {
					//r.lock();

					FileInputStream fileInputStream = getContext().openFileInput(fileN);
					BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
					//readLock.lock();
					while (bufferedInputStream.available() > 0) {
						msgValue += (char) bufferedInputStream.read();
					}
					//readLock.unlock();
					bufferedInputStream.close();
				} catch (FileNotFoundException e) {
					Log.e("FNF", "File Not Found To Read for iteration : " + i);
				} catch (IOException e) {
					Log.e("IOE", "Error Reading File for iteration : " + i);
				}finally {
					//r.unlock();
					readLock.unlock();
				}
				Log.d("@ query", "K:"+fileN);
				cursor.addRow(new String[]{fileN, msgValue});
			}
			return cursor;
		}

		else if(selection.equals("*")) {
			//String headcount = sendOK+delim+portStr;
			//String[] countS = {allAvb, headcount};
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, countS);



			String reply = queryAll + delim + portStr + delim + "@";
			String[] toClient = {allAvb, reply};

			/*
			try {
				Thread.sleep(100);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}
			 */

			//NEED TO MOVE IT BELOW SYNC??
			//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
			//synchronized (this ) {

				queriesaccepted = 0;
				expectedReplies = allNodes.size();
			//}

			try {
				Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
			}catch(Exception e){
				Log.e("Failed Client Task", "error: "+e.getMessage());
				e.printStackTrace();
			}

			//queriesaccepted = 0;
			//??
			/*synchronized (this ) {

				queriesaccepted = 0;
				expectedReplies = allNodes.size();
			}*/

			while(queriesaccepted < expectedReplies){

			}
			Log.d("waited enough", "qa "+ queriesaccepted + " er "+ expectedReplies);

			MatrixCursor queryAllReplies = new MatrixCursor(new String[]{"key", "value"});
			Log.d("total rows",""+queryAllR.size());
			//exp1
			//ReentrantLock r1 = new ReentrantLock();
			//r1.lock();
			for (String key:queryAllR.keySet())
			{
				Log.d("Got Reply", " k: " + key + " v: " + queryAllR.get(key));
				String value = queryAllR.get(key);
				String [] row = {key,value};
				queryAllReplies.addRow(row);
			}
			//r1.unlock();
			queryAllR.clear();
			return queryAllReplies;

		}

		else {
			String keyStoreS = senderMap.get(allNodes.get(0));
			int insIndex = 0;
			for(int i =0; i<allNodes.size(); i++){
				if(i == 0) {
					if (hashKey.compareTo(allNodes.get(i)) >= 0 && hashKey.compareTo(allNodes.get(allNodes.size() - 1)) > 0) {

						keyStoreS = senderMap.get(allNodes.get(i));
						insIndex = i;
						break;
					}
				}
				else if((hashKey.compareTo(allNodes.get(i)) <= 0 && hashKey.compareTo(allNodes.get(i-1)) > 0)){
					keyStoreS = senderMap.get(allNodes.get(i));
					insIndex = i;
				}
			}
			Log.d("query belongs to", ""+ keyStoreS);

			String[] toClient;

			String keyStoreS1 = senderMap.get(allNodes.get((5 + 1 + insIndex)%5));
			String keyStoreS2 = senderMap.get(allNodes.get((5 + 2 + insIndex)%5));

			String queryKV = query + delim + portStr + delim + selection;

			if(keyStoreS2.equals(portStr)||keyStoreS1.equals(portStr)||keyStoreS.equals(portStr)){
				//toClient = new String[]{sendTo, Integer.toString(2), keyStoreS1, keyStoreS2, insertKv};
				String msgValue = "";
				Log.d("query","handled locally");
				readLock.lock();
				try {
					//r.lock();
					FileInputStream fileInputStream = getContext().openFileInput(selection);
					BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
					//readLock.lock();
					while (bufferedInputStream.available() > 0) {
						msgValue += (char) bufferedInputStream.read();
					}
					//readLock.unlock();
					bufferedInputStream.close();
					Log.d("local:", "sel: "+selection+" val: "+msgValue);
				} catch (FileNotFoundException e) {
					Log.e("FNF", "File Not Found To Read for iteration ");
				} catch (IOException e) {
					Log.e("IOE", "Error Reading File for iteration");
				}finally {
					//r.unlock();
					readLock.unlock();
				}
				String[] columnNames= {"key", "value"};
				MatrixCursor msgCursor= new MatrixCursor(columnNames);
				msgCursor.addRow(new String[]{selection, msgValue});

				return msgCursor;
			}

			else
				{
				toClient = new String[]{queryFrom, Integer.toString(3), keyStoreS2, keyStoreS1, keyStoreS, queryKV};
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
					try {
						Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
					}catch(Exception e){
						Log.e("Failed Client Task", "error: "+e.getMessage());
						e.printStackTrace();
					}

				//queryaccepted = 0;

				/*synchronized (this ) {

					queryaccepted = 0;

				}

				while(queryaccepted < 1) {

				}*/
				Log.d("wait","for query reply");
				while(queryR.get(selection) == null)
				{
					//Log.d("wait","for query reply");
				}
				Log.d("done", "waiting");

				MatrixCursor cursorReply = new MatrixCursor(new String[]{"key", "value"});
				//ReentrantLock r1 = new ReentrantLock();
				//exp1
				//r1.lock();
				for (String key:queryR.keySet())
				{
					Log.d("Got Reply", " k: " + key + " v: " + queryR.get(key));
					String value = queryR.get(key);
					String [] row = {key,value};
					cursorReply.addRow(row);
				}
				//r1.unlock();
				queryR.clear();
				return cursorReply;

			}


		}
		//return null;
	}

	//synchronized
	public  void insertAllLocal()
	{
		if(replicas.isEmpty())
		{
			return;
		}

		for (String key:replicas.keySet())
		{
			String hashKey = null;
			try {
				hashKey  = genHash(key);

			}
			catch(NoSuchAlgorithmException e ) {
				Log.e("insert", "No Such Algorithm exception caught");
				e.printStackTrace();
			}

			Log.d("kvpair to insert","k: "+key+" v: "+replicas.get(key));

			if(
					//local normal (greater than pred1 && less than current)
					((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-1)%5)) > 0) && (hashKey.compareTo(allNodes.get(indexOfCurrentNode)) <= 0))
					//local is index = 0
					|| ((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-1)%5)) > 0) && indexOfCurrentNode == 0)
					|| ((hashKey.compareTo(allNodes.get(indexOfCurrentNode)) <= 0) && indexOfCurrentNode == 0)
					//replica pred1
					//local normal (greater than pred2 && less than pred1)
					||((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-2)%5)) > 0) && (hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-1)%5)) <= 0))
					//pred1 is index = 0
					|| ((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-2)%5)) > 0) && (indexOfCurrentNode -1) == 0)
					|| ((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-1)%5)) <= 0) && (indexOfCurrentNode -1) == 0)
					//replica pred2
					//local normal (greater than pred3 && less than pred2)
					||((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-3)%5)) > 0) && (hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-2)%5)) <= 0))
					//pred1 is index = 0
					|| ((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-3)%5)) > 0) && (indexOfCurrentNode -2) == 0)
					|| ((hashKey.compareTo(allNodes.get((5+indexOfCurrentNode-2)%5)) <= 0) && (indexOfCurrentNode -2) == 0)
			)
			{
				Log.d("Inserting","k: "+key+" v: "+replicas.get(key));
				ContentValues values = new ContentValues();
				values.put("key",key);
				values.put("value",replicas.get(key));
				insertLocal(null, values);
			}

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			// ServerSocket serverSocket = sockets[0];
			while (true) {
				try {
					Socket socket = sockets[0].accept();


					//BufferedReader inputStream = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					//String msg = inputStream.readLine();
					DataInputStream input = new DataInputStream(socket.getInputStream());
					String msg = input.readUTF();
					Log.d("msg recv on server", " " + msg);


					//String message = "OK";
					//Log.d("server send", " msg " + message);
					DataOutputStream output = new DataOutputStream(socket.getOutputStream());
					//output.writeUTF(message);
					String message1 = "TASK_DONE";
					Log.d("server send", " msg " + message1);
					output.writeUTF(message1);
					output.flush();

					if (msg != null) {
						String[] arr = msg.split("\\" + delim);

						publish_Progress(arr);

					}
					//inputStream.close();
					socket.close();

				} catch (IOException e) {
					Log.e("socket server", "IOException" + e.getMessage());
				}
			}
		}

		//protected void onProgressUpdate(String...msgs) {
		protected void publish_Progress(String...msgs) {
			Log.d("In", "Publish Progress");
			//process incoming msgs
			Log.d("msgs", " " + msgs[0]);

			if (msgs[0].equals(insertKV)){
				Log.d("insert", " "+ msgs[2]);
				String senderNode = msgs[1];
				String key = msgs[2];
				String value = msgs[3];
				String hashKey = null;

				try {
					hashKey  = genHash(key);

				} catch(NoSuchAlgorithmException e ) {
					Log.e("insert", "No Such Algorithm exception caught");
					e.printStackTrace();
				}


				Log.d("Insert server","insert to this node");
				ContentValues values = new ContentValues();
				values.put("key",key);
				values.put("value",value);
				Log.d("send", "to insert");

				insertLocal(null, values);
				/*FileOutputStream fileOutputStream;
				try {
					fileOutputStream = getContext().openFileOutput(values.get("key").toString(), Context.MODE_PRIVATE);
					fileOutputStream.write(values.get("value").toString().getBytes());
					fileOutputStream.flush();
					fileOutputStream.close();
				} catch (Exception e) {
					Log.e("Error", e.getMessage());
				}
				Log.v("insert", values.toString());*/

				//String reply = insAck+delim+"ok insert done"+delim+portStr;
				//String[] toClient = {sendTo,Integer.toString(1),senderNode,reply};
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);

			}

			else if(msgs[0].equals(insAck))
			{
				Log.d(msgs[1], msgs[2]);
				//insertDone++;
			}

			else if(msgs[0].equals(query)) {
				String senderNode = msgs[1];
				String selection = msgs[2];
				String reply = queryReply +delim+ portStr;
				Log.d("server query",portStr +" "+ selection);
				Cursor cursor = query(null, null, selection,  null, null);
				cursor.moveToFirst();
				String key = cursor.getString(0);
				String value = cursor.getString(1);
				reply += delim + key + "-" + value;

				String[] toClient = {sendTo,Integer.toString(1),senderNode,reply};
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
				try {
					Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
				}catch(Exception e){
					Log.e("Failed Client Task", "error: "+e.getMessage());
					e.printStackTrace();
				}
			}

			else if(msgs[0].equals(queryReply))
			{
				String senderNode = msgs[1];
				String[] kvPair = msgs[2].split("-");
				//exp987
				if(queryR.get(kvPair[0]) == null) {
					//can also try lock
					queryR.put(kvPair[0], kvPair[1]);
					queryaccepted++;
					Log.d("server query r", "" + kvPair[1]);
				}
			}

			else if(msgs[0].equals(queryAll)){
				String senderNode = msgs[1];
				String selection = msgs[2];
				Log.d("server queryAll", "selection: "+msgs[2]+" port: "+ portStr);
				if(selection.equals("@")){
					String reply = queryAllReply+delim+portStr;
					Cursor cursor = query(null, null, selection,  null, null);
					for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext())
					{
						String key = cursor.getString(0);
						String value = cursor.getString(1);
						reply += delim + key + "-" + value;
					}
					Log.d("response qall", "" + reply);
					String[] toClient = {sendTo,Integer.toString(1),senderNode,reply};
					//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
					try {
						Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
					}catch(Exception e){
						Log.e("Failed Client Task", "error: "+e.getMessage());
						e.printStackTrace();
					}
				}
			}

			else if(msgs[0].equals(queryAllReply)){
				String senderNode = msgs[1];
				Log.d("qall", "msgs length "+msgs.length);
				String[] sliceKV = Arrays.copyOfRange(msgs, 2, msgs.length);
				//String[] sliceKV = null;
				//sliceKV = msgs[2].split(delim)
				for(String row : sliceKV)
				{
					String [] kvPair = row.split("-");
					queryAllR.put(kvPair[0],kvPair[1]);
					Log.d("KV", "K: "+ kvPair[0]+" V: "+kvPair[1]);
				}
				Log.d("sizeof", "queries accepted: "+queriesaccepted +"expected queries "+ expectedReplies);
				queriesaccepted++;
				Log.d("sizeof", "queries accepted: "+queriesaccepted +"expected queries "+ expectedReplies);
			}

			else if(msgs[0].equals(delAll) || msgs[0].equals(del))
			{
				Log.d("server del", "selection: "+msgs[2]+" port: "+ portStr);

				String senderNode = msgs[1];
				String selection = msgs[2];
				if(selection.equals("@") || selection.equals("*"))
				{
				int temp = delete(null, selection, new String[]{"noFwding"});
				}
				else if(!senderNode.equals(portStr))
				{
					Log.d("will","delete selection "+selection);
					//ReentrantLock r = new ReentrantLock();
					writeLock.lock();
					try {
						//r.lock();
						Log.d("deleting file", "@843");
						File dir = getContext().getFilesDir();
						File file = new File(dir, selection);
						//boolean deleted = file.delete();
						//writeLock.lock();
						boolean deleted = fileDelete(file);
						//writeLock.unlock();
						Log.v("file deleted server", selection + "deleteFlag : "+deleted);
					} catch (Exception e) {
						Log.e("exception", "File delete failed");
						e.printStackTrace();
					}finally {
						//r.unlock();
						writeLock.unlock();
					}
				}

			}

			else if(msgs[0].equals(replicaKV))
			{
				String senderNode = msgs[1];
				String selection = msgs[2];
				Log.d("server recover", "selection: "+msgs[2]+" port: "+ portStr);
				if(selection.equals("@")){
					String reply = replicaReply+delim+portStr;
					Cursor cursor = query(null, null, selection,  null, null);
					for(cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext())
					{
						String key = cursor.getString(0);
						String value = cursor.getString(1);
						reply += delim + key + "-" + value;
					}
					Log.d("response qall", "" + reply);
					String[] toClient = {sendTo,Integer.toString(1),senderNode,reply};
					//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
					try {
						Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
					}catch(Exception e){
						Log.e("Failed Client Task", "error: "+e.getMessage());
						e.printStackTrace();
					}
				}
			}

			else if(msgs[0].equals(replicaReply))
			{
				String senderNode = msgs[1];
				Log.d("replicaReply", "msgs length "+msgs.length);
				//ReentrantLock re = new ReentrantLock();
				//re.lock();
				String[] sliceKV = Arrays.copyOfRange(msgs, 2, msgs.length);
				for(String row : sliceKV)
				{
					String [] kvPair = row.split("-");
					replicas.put(kvPair[0],kvPair[1]);
					Log.d("KV", "K: "+ kvPair[0]+" V: "+kvPair[1]);
				}

				//replicaAccepted++;
				//re.unlock();
				//if(replicaAccepted >= 3)
				//{
					insertAllLocal();
				//	replicaAccepted = 0;
				//}
			}

			else if(msgs[0].equals(sendOK)){
				String senderNode = msgs[1];
				String reply = recvOK+delim+"ok"+delim+portStr;
				String[] toClient = {sendTo,Integer.toString(1),senderNode,reply};
				//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient);
				try {
					Integer i = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toClient).get();
				}catch(Exception e){
					Log.e("Failed Client Task", "error: "+e.getMessage());
					e.printStackTrace();
				}
			}
			else if(msgs[0].equals(recvOK))
			{
				String senderNode = msgs[2];
				Log.d("ok from", ""+senderNode);
				expectedReplies++;
				Log.d("expectedReplies", ""+expectedReplies);
			}

		}


	}

	//client task only needs to fwd the generated msg to servers
	//private class ClientTask extends AsyncTask<String, Void, Void> {
	private class ClientTask extends AsyncTask<String, Void, Integer> {

		@Override
		//protected Void doInBackground(String... msgs) {
		protected Integer doInBackground(String... msgs) {
			Log.d("in", "client task");

			if(msgs[0].equals(sendTo)) {
				//0 sendTo, 1: total, 2-2+total: ports, 2+total+1: msg;
				//process msg
				Log.d("send insert to", "total "+ msgs[1]);
				int totalPorts = Integer.parseInt(msgs[1]);
				//PLog.d("send insert to", "total in int"+ totalPorts);
				//Log.d("to", " "+ msgs[2] + " " + msgs[3] + " "+ msgs[4]);
				String message = msgs[msgs.length -1];
				String repExp = msgs[msgs.length - 2];



				for(int i = 2; i < 2+totalPorts; i++) {
					try {
						//expppp 1234
						Socket socket = new Socket();
						InetAddress inetAddress= inetAddress = InetAddress.getByAddress(new byte[]{10, 0, 2, 2}); //getByName("localhost");
						int port = Integer.parseInt(msgs[i]) * 2;
						SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
						socket.connect(socketAddress, 2000);
						socket.setSoTimeout(1500);


						//Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(msgs[i]) * 2));
						//socket.setSoTimeout(1000);

						//BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
						//Log.d("client msg", " sendTo " + message);
						//outputStream.write(message);
						//outputStream.flush();

						Log.d("client msg", " sendTo " + message);
						DataOutputStream output = new DataOutputStream(socket.getOutputStream());
						output.writeUTF(message);
						output.flush();

						DataInputStream input = new DataInputStream(socket.getInputStream());
						//String msg  = input.readUTF();
						//Log.d("msg recv on client", " " + msg);



						//input = new DataInputStream(socket.getInputStream());
						String msg1 = input.readUTF();
						Log.d("msg recv on client", "sendTo --> last: " + msg1);



						socket.close();

						/*if(message.contains(insertKV)) {
							Log.d("insert", "waiting for confirmation");
							//synchronized (this) {
								insertDone = 0;
							//}
							while (insertDone < 1) {

							}
							Log.d("insert", "done");
						}*/

					}catch (Exception e) {
						Log.e("socket client", "Exception: " + e.getMessage());
						e.printStackTrace();
					}
					/*catch (UnknownHostException e) {
						Log.e("socket client", "UnknownHostException: " + e.getMessage());
					} catch (IOException e) {
						Log.e("socket client", "IOException: " + e.getMessage());
					}*/
				}
			}

			else if(msgs[0].equals(allAvb)) {
				for(int i=0; i < allNodes.size(); i++)
				{
					Log.d("current node", " "+ allNodes.get(i));
					String sendPort = senderMap.get(allNodes.get(i));
					Log.d("send port", " "+ sendPort);
					try {

						//expppp 1234
						Socket socket = new Socket();
						InetAddress inetAddress= inetAddress = InetAddress.getByAddress(new byte[]{10, 0, 2, 2}); //getByName("localhost");
						//int port = Integer.parseInt(msgs[i]) * 2;
						SocketAddress socketAddress=new InetSocketAddress(inetAddress, Integer.parseInt(sendPort)*2);
						socket.connect(socketAddress, 2000);
						socket.setSoTimeout(1500);

						//Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sendPort)*2);
						//socket.setSoTimeout(500);
						//BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
						//Log.d("client msg", " allAvb " + msgs[1]);
						//outputStream.write(msgs[1]);
						//outputStream.flush();

						Log.d("client msg", " sendTo " + msgs[1]);
						DataOutputStream output = new DataOutputStream(socket.getOutputStream());
						output.writeUTF(msgs[1]);
						output.flush();

						DataInputStream input = new DataInputStream(socket.getInputStream());
						//String msg = input.readUTF();
						//Log.d("msg recv on client", " " + msg);


						//exp4812
						//input = new DataInputStream(socket.getInputStream());
						String msg1 = input.readUTF();
						Log.d("msg recv on client", "allAVB --> last: " + msg1);

						socket.close();
					}/*catch (UnknownHostException e) {
						Log.e("sock client", "all avb UnknownHostException " + allNodes.get(i));
					} catch (IOException e) {
						Log.e("socket client", "Exception: " + e.getMessage());
						expectedReplies --;
						Log.d("decrease er", "er: "+expectedReplies);
					}*/
					catch (Exception e) {
						Log.e("socket client", "Exception: " + e.getMessage());
						expectedReplies --;
						Log.d("decrease er", "er: "+expectedReplies);
					}
				}
			}

			else if(msgs[0].equals(queryFrom)) {
				String message = msgs[msgs.length - 1];
				try {
					//expppp 1234
					Socket socket = new Socket();
					InetAddress inetAddress= inetAddress = InetAddress.getByAddress(new byte[]{10, 0, 2, 2}); //getByName("localhost");
					int port = Integer.parseInt(msgs[2]) * 2;
					SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
					socket.connect(socketAddress, 2000);
					socket.setSoTimeout(1500);

					//Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(msgs[2]) * 2));
					//socket.setSoTimeout(500);
					//BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
					//Log.d("client msg", " queryFrom " + message + " to " + msgs[2]);
					//outputStream.write(message);
					//outputStream.flush();
					Log.d("client msg", " queryFrom " + message + " to " + msgs[2]);
					DataOutputStream output = new DataOutputStream(socket.getOutputStream());
					output.writeUTF(message);
					output.flush();

					DataInputStream input = new DataInputStream(socket.getInputStream());
					//String msg = input.readUTF();
					//Log.d("msg recv on client", " " + msg);


					//exp4812
					//input = new DataInputStream(socket.getInputStream());
					String msg1 = input.readUTF();
					Log.d("msg recv on client", "qfrm --> last: " + msg1);

					socket.close();
				} catch (IOException e) {
					Log.e("socket client", "IO Exception: " + e.getMessage());
					e.printStackTrace();
					try {
						//expppp 1234
						Socket socket = new Socket();
						InetAddress inetAddress= inetAddress = InetAddress.getByAddress(new byte[]{10, 0, 2, 2}); //getByName("localhost");
						int port = Integer.parseInt(msgs[3]) * 2;
						SocketAddress socketAddress=new InetSocketAddress(inetAddress, port);
						socket.connect(socketAddress, 2000);
						socket.setSoTimeout(1500);

						//Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(msgs[3]) * 2));
						//socket.setSoTimeout(500);
						//BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
						//Log.d("client nested msg", " " + message);
						//outputStream.write(message);
						//outputStream.flush();

						Log.d("client msg", " queryFrom " + message + " to " + msgs[3]);
						DataOutputStream output = new DataOutputStream(socket.getOutputStream());
						output.writeUTF(message);
						output.flush();

						DataInputStream input = new DataInputStream(socket.getInputStream());
						//String msg = input.readUTF();
						//Log.d("msg recv on client", " " + msg);

						//exp4812
						//input = new DataInputStream(socket.getInputStream());
						String msg1 = input.readUTF();

						socket.close();
					} catch (Exception er) {
						Log.e("client nested", "Exception: " + er.getMessage());
						e.printStackTrace();
					}
				} catch (Exception er) {
					Log.e("client nested er", "Exception: " + er.getMessage());
				}
			}

			try {
				Thread.sleep(20);
			}catch(InterruptedException e) {
				e.printStackTrace();
			}

			return 1;
		}
	}


		private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
