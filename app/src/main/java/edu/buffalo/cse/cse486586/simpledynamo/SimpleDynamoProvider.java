//References : //https://developer.android.com/reference/android/database/sqlite/SQLiteCursor.html

package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.Message;
import android.telephony.TelephonyManager;

import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.os.AsyncTask;
import android.util.Log;
import android.widget.TextView;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static android.content.ContentValues.TAG;



public class SimpleDynamoProvider extends ContentProvider {

	static final String[] Node_List= {"5562","5556","5554","5558","5560"};
	static final String[] Port_List= {"11124","11112","11108","11116","11120"};
	static final int SERVER_PORT = 10000;
	//private Semaphore sem_lock;
	private MyDBHandler myDB;
	private SQLiteDatabase sqlDB;
	boolean CHECK = true;
	String portStr = null;
	String myPort = null;
	//String keyhash = null;
	String porthash = null;
	//String insert_at_node = null;
	//String Coord = null;
	//String Succ1 = null;
	//String Succ2 = null;

	class Message {
		String k;
		String val;
		String txt;
		String port;

		public String getK() {
			return k;
		}

		public void setK(String k) {
			this.k = k;
		}

		public String getVal() {
			return val;
		}

		public void setVal(String val) {
			this.val = val;
		}

		public String getTxt() {
			return txt;
		}

		public void setTxt(String txt) {
			this.txt = txt;
		}

		public String getPort() {
			return port;
		}

		public void setPort(String port) {
			this.port = port;
		}

		@Override
		public String toString()
		{
			return txt + "$" + k + "$" + val + "$" + port;
		}
	}


	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String keyhash = null;
		String Succ1 = null;
		String Succ2 = null;
		String Coord = null;
		String key = (String) values.get("key");
		String value = (String) values.get("value");
		try {
			keyhash = genHash(key);
			porthash = genHash(portStr);
			//predhash = genHash(pred);
		int node_id = 0;
		if(genHash(Node_List[0]).compareTo(keyhash) >=0 || genHash(Node_List[4]).compareTo(keyhash)<0){
			node_id=0;
		}else if(genHash(Node_List[1]).compareTo(keyhash)>=0 && genHash(Node_List[0]).compareTo(keyhash)<0 ){
			node_id=1;
		}else if(genHash(Node_List[2]).compareTo(keyhash)>=0 && genHash(Node_List[1]).compareTo(keyhash)<0 ){
			node_id=2;
		}else if(genHash(Node_List[3]).compareTo(keyhash)>=0 && genHash(Node_List[2]).compareTo(keyhash)<0 ){
			node_id=3;
		}else if(genHash(Node_List[4]).compareTo(keyhash)>=0 && genHash(Node_List[3]).compareTo(keyhash)<0 ){
			node_id=4;
		}

		Coord = Integer.toString(Integer.parseInt(Node_List[node_id])*2);

		if (node_id == 3) {
			Succ1 = Integer.toString(Integer.parseInt(Node_List[4])*2);
			Succ2 = Integer.toString(Integer.parseInt(Node_List[0])*2);
		}
		else if (node_id == 4) {
			Succ1 = Integer.toString(Integer.parseInt(Node_List[0])*2);
			Succ2 = Integer.toString(Integer.parseInt(Node_List[1])*2);
		}
		else {
			Succ1 = Integer.toString(Integer.parseInt(Node_List[node_id + 1])*2);
			Succ2 = Integer.toString(Integer.parseInt(Node_List[node_id + 2])*2);
		}

		if((Integer.toString(Integer.parseInt(Node_List[node_id])*2)).equals(myPort)){
			sqlDB.insertWithOnConflict("GroupMessenger", null, values, SQLiteDatabase.CONFLICT_REPLACE);
			Log.v("Insert", "key inserted in insert method: " + key);
			Message msg = new Message();
			msg.setK(key);
			msg.setVal(value);
			msg.setTxt("Insert");
			Log.v("Insert", "inserted at current node myPort: " + myPort + "succ1: " + Succ1 + "succ2: " + Succ2);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ1 + "$" + Succ2);
		}
		else
		{
			Message msg = new Message();
			msg.setK(key);
			msg.setVal(value);
			msg.setTxt("Insert");
			Log.v("Insert", "inserted at other node myPort: " + myPort + "succ1: " + Succ1 + "succ2: " + Succ2 + "coord" + Coord);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ1 + "$" + Succ2 + "$" + Coord);
		}


		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.v ("Insert", "returning null");
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		myDB = new MyDBHandler(getContext(), null, null, 1);
		sqlDB = myDB.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.v("OnCreate ", "after server socket creation");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			//Log.v("OnCreate", "created server socket");
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket" + e.getMessage());
		}

		Message msg = new Message();
		//msg.setK(key);
		//msg.setVal(value);
		msg.setTxt("Node Recovery");
		//sem_lock = new Semaphore(1,true);
		CHECK = false;
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString());

		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			//Socket clientSocket;
			//Log.v("Server Task", "inside");
			try {
				while (true) {
					Log.v("Server Task ", "just inside server task");
					Socket clientSocket = serverSocket.accept();
					//clientSocket.setSoTimeout(2000);
					BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
					//Log.v("Server Task","Within while loop -> Server accepted -> created buffer");
					String mes = in.readLine();
					Log.v("Server Task ", "read message from client" + mes);
					//Log.v("Server Task","Within while loop " + mes);
					if (mes != null) {
						String[] divide = mes.split("\\$");
						Log.v("Server Task ", "message length: " + divide.length);
						if (divide.length == 6)
						{
							//insert coordinator here
							String ke = divide[1];
							String va = divide[2];
							String Succ1 = divide[4];
							String Succ2 = divide[5];
							if(ke != null && !ke.equals("null")) {
								ContentValues cv = new ContentValues();
								cv.put("key", ke);
								cv.put("value", va);
								sqlDB.insertWithOnConflict("GroupMessenger", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
								Log.v("Server Task", "key inserted in coordinator " + ke);
							}
							Message msg = new Message();
							msg.setK(ke);
							msg.setVal(va);
							msg.setTxt("Insert");
							Log.v("Server Task", "calling client for coordinator insert at succ1: " + Succ1 + " succ2: " + Succ2);
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ1 + "$" + Succ2);
						}
						else
						{
							String vers = divide[0];
							String ke = divide[1];
							String va = divide[2];
							String po = divide[3];
							if (vers.equals("Insert")) {
								if(ke != null && !ke.equals("null")) {
									Log.v("Server Task", "inserted in succ");
									ContentValues cv = new ContentValues();
									cv.put("key", ke);
									cv.put("value", va);
									sqlDB.insertWithOnConflict("GroupMessenger", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
									Log.v("Server Task", "key inserted at normal insert: " + ke);
								}
							}
							else if (vers.equals("Query")) {
								Cursor cursor = sqlDB.query("GroupMessenger", null, null, null, null, null, null, null);
								Log.v("Server Task", "Query from database returned: "+ DatabaseUtils.dumpCursorToString(cursor) + " cursor length: " + cursor.getCount());
								JSONArray jsonObjK = new JSONArray();
								JSONArray jsonObjV = new JSONArray();
								int i = 0;
								cursor.moveToFirst();
								while (!cursor.isAfterLast()) {
									Log.v("Server Task", "inside cursor query * iteration");
									jsonObjK.put(i, cursor.getString(cursor.getColumnIndex("key")));
									jsonObjV.put(i, cursor.getString(cursor.getColumnIndex("value")));
									Log.v("Server Task", "Query * result key & value extracted");
									i++;
									cursor.moveToNext();
								}
								Log.v("Server Task", "Query * result json key array: " + jsonObjK.toString() + "json value: " + jsonObjV.toString() + "extracted");
								JSONObject msgJson = new JSONObject();
								msgJson.put("key", jsonObjK);
								msgJson.put("value", jsonObjV);
								String msg = msgJson.toString();
								Log.v("Server Task", "Query * result putting in string: " + msg);
								PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
								cpw2.println(msg);
								//cursor.close();
								Log.v("Server Task", "Query * result sent to client via json object" + msg);
							}
							else if (vers.equals("Query for key")) {
								Cursor cursor = sqlDB.query("GroupMessenger", null, "key=?", new String[]{ke}, null, null, null, null);
								Log.v("Server Task", "Query for key from database returned: "+ DatabaseUtils.dumpCursorToString(cursor) + " cursor length: " + cursor.getCount());
								PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
								if(cursor.getCount() > 0) {
									JSONArray jsonObjK = new JSONArray();
									JSONArray jsonObjV = new JSONArray();
									int i = 0;
									cursor.moveToFirst();
									while (!cursor.isAfterLast()) {
										Log.v("Server Task", "inside cursor query key iteration");
										jsonObjK.put(i, cursor.getString(cursor.getColumnIndex("key")));
										jsonObjV.put(i, cursor.getString(cursor.getColumnIndex("value")));
										Log.v("Server Task", "Query key result key & value extracted");
										i++;
										cursor.moveToNext();
									}
									Log.v("Server Task", "Query key result json key array: " + jsonObjK.toString() + "json value: " + jsonObjV.toString() + "extracted");
									JSONObject msgJson = new JSONObject();
									msgJson.put("key", jsonObjK);
									msgJson.put("value", jsonObjV);
									String msg = msgJson.toString();
									Log.v("Server Task", "Query key result putting in string: " + msg);
									//PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
									cpw2.println(msg);
									//cursor.close();
									Log.v("Server Task", "Query key result sent to client via json object" + msg);
								}else {
									cpw2.println("nokey");
								}
							}
							else if (vers.equals("Delete")) {
								sqlDB.delete("GroupMessenger", null, null);
								Log.v("Server Task", "Deleted *: " + ke);
							}
							else if (vers.equals("Delete for key")) {
								sqlDB.delete("GroupMessenger", "key=?", new String[]{ke});
								Log.v("Server Task", "Deleted key: " + ke);
							}
							else if (vers.equals("Node Recovery")) {
								Log.v("Server Task", "Inside node recovery server task");
								PrintWriter cpw2 = new PrintWriter(clientSocket.getOutputStream(), true);
								Cursor cursor = sqlDB.query("GroupMessenger", null, null, null, null, null, null, null);
								Log.v("Server Task", "Query from database returned: "+ DatabaseUtils.dumpCursorToString(cursor) + " cursor length: " + cursor.getCount());
								if (cursor.getCount() > 0) {
								//Log.v("Server Task", "Node recovery query returned "+ DatabaseUtils.dumpCursorToString(cursor));
								JSONArray jsonObjK = new JSONArray();
								JSONArray jsonObjV = new JSONArray();
								int i = 0;
								cursor.moveToFirst();
								while (!cursor.isAfterLast()) {
									Log.v("Server Task", "inside cursor node recovery while with port: " + po + String.valueOf(inHashRange(cursor.getString(cursor.getColumnIndex("key")), po)));
									if (inHashRange(cursor.getString(cursor.getColumnIndex("key")), po)) {
										Log.v("Server Task", "inside cursor node recovery iteration");
										jsonObjK.put(i, cursor.getString(cursor.getColumnIndex("key")));
										jsonObjV.put(i, cursor.getString(cursor.getColumnIndex("value")));
										Log.v("Server Task", "Query node recovery result key & value extracted");
										i++;
									}
									cursor.moveToNext();
								}
								Log.v("Server Task", "Query node recovery result json key array: " + jsonObjK.toString() + "json value: " + jsonObjV.toString() + "extracted");
								JSONObject msgJson = new JSONObject();
								msgJson.put("key", jsonObjK);
								msgJson.put("value", jsonObjV);
								String msg = msgJson.toString();
								Log.v("Server Task", "Query node recovery result putting in string: " + msg);
								Log.v("Server Task", "Query node recovery result sent to client via json object" + msg);
								cpw2.println(msg);
								} else {
									cpw2.println("nodata");
								}
								//cursor.close();
								//CHECK = true;
							}
						}
					}
				}
			}catch(IOException e){
				e.printStackTrace();
			}catch (Exception e){
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, String> {

		@Override
		protected String doInBackground(String... msgs) {
			//Log.v("Client Task", "inside");
			try {
				//Log.v("Client Task", "my port:" + portStr + " dest port: " + msgs[1]);
				Log.v("Client Task", "reach client with message " + msgs[0]);
				if (msgs[0].contains("Node Recovery")) {
					//sem_lock.acquire();
					Log.v("Client Task", "inside node recovery");
					//calc two succ pred
					String Succ1 = null;
					String Succ2 = null;
					String Pred1 = null;
					String Pred2 = null;
					int index = Arrays.asList(Port_List).indexOf(myPort);
					Succ1 = Port_List[(index + 1) % 5];
					Succ2 = Port_List[(index + 2) % 5];
					Pred1 = Port_List[(index + 5 - 1) % 5];
					Pred2 = Port_List[(index + 5 - 2) % 5];
					//JSONArray jsonkey = new JSONArray();
					//JSONArray jsonvalue = new JSONArray();
					//JSONObject jobj = new JSONObject();
					//int k = 0;
					for (int i = 0; i < Port_List.length; i++) {
						if (Port_List[i].equals(myPort)) {
							continue;
						}
						Log.v("Client Task", "PORT LIST: " + Port_List[i] + "MYPORT: " + myPort);
						if (Port_List[i].equals(Succ1) || Port_List[i].equals(Succ2)) {
							try {
								Message msg = new Message();
								msg.setPort(myPort);
								msg.setTxt("Node Recovery");
								Log.v("Client Task","APPENDING PORT MESSAGE: " + msg.toString());
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(Port_List[i])));
								Log.v("Client Task ", "node recovery socket" + i + " created when current port " + Port_List[i] + " is a successor of " + myPort);
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msg.toString());
								socket.setSoTimeout(2000);
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								String msgReceived = in.readLine();
								if(msgReceived != null && !msgReceived.equals("nodata")) {
									Log.v("Client Task", "Read Bufferred Reader for query node recovery" + msgReceived);
									JSONObject jsonobj = new JSONObject(msgReceived);
									JSONArray jsonObjK = jsonobj.getJSONArray("key");
									JSONArray jsonObjV = jsonobj.getJSONArray("value");
									int j = 0;
									while (j < jsonObjK.length()) {
										if (jsonObjK.getString(j) != null && !jsonObjK.getString(j).equals("null")) {
											//jsonkey.put(k, jsonObjK.getString(j));
											Log.v("Client Task", "inside node recovery jsonObj while: " + jsonObjK.getString(j));
											//jsonvalue.put(k, jsonObjV.getString(j));
											//Log.v("Server Task","Query result key & value extracted" );

											//k++;
											//insert here
											ContentValues cv = new ContentValues();
											cv.put("key", jsonObjK.getString(j));
											cv.put("value", jsonObjV.getString(j));
											sqlDB.insertWithOnConflict("GroupMessenger", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
											Log.v("Client Task", "key inserted at a successor: " + jsonObjK.getString(j));
										}
										j++;
									}
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if (Port_List[i].equals(Pred1) || Port_List[i].equals(Pred2)) {
							try {
								Message msg = new Message();
								msg.setPort(Port_List[i]);
								msg.setTxt("Node Recovery");
								Log.v("Client Task","APPENDING PORT MESSAGE: " + msg.toString());
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(Port_List[i])));
								Log.v("Client Task ", "node recovery socket" + i + " created when current port " + Port_List[i] + " is a predecessor of " + myPort);
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msg.toString());
								socket.setSoTimeout(2000);
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								String msgReceived = in.readLine();
								if (msgReceived != null && !msgReceived.equals("nodata")) {
									Log.v("Client Task", "Read Bufferred Reader for query node recovery" + msgReceived);
									JSONObject jsonobj = new JSONObject(msgReceived);
									JSONArray jsonObjK = jsonobj.getJSONArray("key");
									JSONArray jsonObjV = jsonobj.getJSONArray("value");
									int j = 0;
									while (j < jsonObjK.length()) {
										if (jsonObjK.getString(j) != null && !jsonObjK.getString(j).equals("null")) {
											//jsonkey.put(k, jsonObjK.getString(j));
											Log.v("Client Task", "inside node recovery jsonObj while: " + jsonObjK.getString(j));
											//jsonvalue.put(k, jsonObjV.getString(j));
											//Log.v("Server Task","Query result key & value extracted" );

											//k++;
											ContentValues cv = new ContentValues();
											cv.put("key", jsonObjK.getString(j));
											cv.put("value", jsonObjV.getString(j));
											sqlDB.insertWithOnConflict("GroupMessenger", null, cv, SQLiteDatabase.CONFLICT_REPLACE);
											Log.v("Client Task", "key inserted at a predecessor: " + jsonObjK.getString(j));
										}
										j++;
									}
								}
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					//sem_lock.release();
					CHECK = true;
					/*jobj.put("key", jsonkey);
					Log.v("Client Task", "putting into obj for node recovery: " + jsonkey.toString());
					jobj.put("value", jsonvalue);
					Log.v("Client Task", "returning for node recovery json object: " + jobj.toString());
					return jobj.toString();*/
				} else if (msgs[1].contains("$")) {
					String[] Ports = msgs[1].split("\\$");
					if (msgs[0].contains("Insert")) {
						if (Ports.length == 2) {
							String replica1 = Ports[0];
							String replica2 = Ports[1];
							//Log.v("Client Task", "inserting at replica1: " + replica1 + " replica2: " + replica2 + "with message: "  + msgs[0].toString());
							try {
								Log.v("Client Task", "inserting at replica1: " + replica1 + " with message: "  + msgs[0].toString());
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica1)));
								Log.v("Client Task ", "insert socket 1 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Log.v("Client Task", "inserting at replica2: " + replica2 + "with message: "  + msgs[0].toString());
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica2)));
								Log.v("Client Task ", "insert socket 2 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if (Ports.length == 3) {
							String replica1 = Ports[0];
							String replica2 = Ports[1];
							String coord_node = Ports[2];
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(coord_node)));
								Log.v("Client Task ", "insert socket 3 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								Log.v("Client Task ", "coordinator sending replica ports: " + replica1 + replica2);
								cpw2.println(msgs[0] + "$" + replica1 + "$" + replica2);
								//cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica1)));
								Log.v("Client Task ", "insert socket 4 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica2)));
								Log.v("Client Task ", "insert socket 5 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					} else if (msgs[0].contains("Delete")) {
						if (Ports.length == 2) {
							String replica1 = Ports[0];
							String replica2 = Ports[1];
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica1)));
								Log.v("Client Task ", "delete socket 1 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica2)));
								Log.v("Client Task ", "delete socket 2 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if (Ports.length == 3) {
							String replica1 = Ports[0];
							String replica2 = Ports[1];
							String coord_node = Ports[2];
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(coord_node)));
								Log.v("Client Task ", "delete socket 3 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica1)));
								Log.v("Client Task ", "delete socket 4 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(replica2)));
								Log.v("Client Task ", "delete socket 5 created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if (Ports.length == 5) {
							for (int i = 0; i < Port_List.length; i++) {
								try {
									Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(Port_List[i])));
									Log.v("Client Task ", "delete socket" + i + "created");
									PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
									cpw2.println(msgs[0]);
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
					} else if (msgs[0].contains("Query")) {
						JSONArray jsonkey = new JSONArray();
						JSONArray jsonvalue = new JSONArray();
						JSONObject jobj = new JSONObject();
						int k = 0;
						for (int i = 0; i < Port_List.length; i++) {
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(Port_List[i])));
								Log.v("Client Task ", "query * socket" + i + "created");
								PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
								cpw2.println(msgs[0]);
								socket.setSoTimeout(2000);
								BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								String msgReceived = in.readLine();
								Log.v("Client Task", "Read Bufferred Reader for query *" + msgReceived);
								if(msgReceived != null) {
									JSONObject jsonobj = new JSONObject(msgReceived);
									JSONArray jsonObjK = jsonobj.getJSONArray("key");
									JSONArray jsonObjV = jsonobj.getJSONArray("value");
									int j = 0;
									while (j < jsonObjK.length()) {
										jsonkey.put(k, jsonObjK.getString(j));
										Log.v("Client Task", "inside jsonObj while: " + jsonObjK.getString(j));
										jsonvalue.put(k, jsonObjV.getString(j));
										//Log.v("Server Task","Query result key & value extracted" );
										j++;
										k++;
									}
								} else {
									continue;
								}
							} catch (IOException e) {
								e.printStackTrace();
								continue;
							}
						}
						jobj.put("key", jsonkey);
						Log.v("Client Task", "putting into obj: " + jsonkey.toString());
						jobj.put("value", jsonvalue);
						Log.v("Client Task", "returning json object: " + jobj.toString());
						return jobj.toString();
					}
				} else {
					while(!CHECK){

					}
					int index = Arrays.asList(Port_List).indexOf(msgs[1]);
					String pred = null;
					pred = Port_List[(index + 5 - 1) % 5];
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(msgs[1])));
						BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						Log.v("Client Task ", "query " + msgs[0].toString() + " for key in replica2 socket created for port msgs[1]: " + msgs[1]);
						PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
						cpw2.println(msgs[0]);
						socket.setSoTimeout(2000);
						String msgReceived = in.readLine();
						if(msgReceived == null) {
							throw new Exception();
						}
						else if(msgReceived.equals("nokey")) {
							throw new Exception();
						}
						Log.v("Client Task", "Read Bufferred Reader for query key replica2" + msgReceived);
						return msgReceived;
					} catch (Exception e) {
						e.printStackTrace();
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(pred)));
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							Log.v("Client Task ", "query " + msgs[0].toString() + " for key in replica1 socket created for port pred: " + pred);
							PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
							cpw2.println(msgs[0]);
							socket.setSoTimeout(2000);
							String msgReceived = in.readLine();
							if (msgReceived == null) {
								throw new Exception();
							}
							else if(msgReceived.equals("nokey")) {
								throw new Exception();
							}
							Log.v("Client Task", "Read Bufferred Reader for query key replica1" + msgReceived);
							return msgReceived;
						} catch (Exception ex) {
							ex.printStackTrace();
							String coord = Port_List[(index + 5 - 2) % 5];
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(coord)));
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							Log.v("Client Task ", "query " + msgs[0].toString() + " for key in coordinator socket created for port coord: " + coord);
							PrintWriter cpw2 = new PrintWriter(socket.getOutputStream(), true);
							cpw2.println(msgs[0]);
							socket.setSoTimeout(2000);
							String msgReceived = in.readLine();
							Log.v("Client Task", "Read Bufferred Reader for query key coordinator" + msgReceived);
							return msgReceived;
						}

					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return "";
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String Succ1 = null;
		String Succ2 = null;
		String Coord = null;
		String keyhash = null;
		try {
			//Cursor cursor = null;
			Log.v("query method ", "begins with key :" + selection);
			if (selection.equals("@")) {
				while (!CHECK) {

				}
				Log.v("Query", "Querying with key @");
				Cursor cursor = myDB.queryAll(sqlDB, selection);
				return cursor;
			}
			else if (selection.equals("*")) {
				Log.v("Query", "Querying with key *");
				Message msg = new Message();
				//Log.v("Query", "* got from portStr:"+ firstQueryPort);
				//msg.setSourcePort(firstQueryPort);
				msg.setK("*");
				msg.setTxt("Query");
				String receivedJsonSTring = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), 11108 + "$" + 11112 + "$" + 11116 + "$" + 11120 + "$" + 11124).get();
				Log.v("Query","Query * result received from client as : "+receivedJsonSTring);
				JSONObject json = new JSONObject(receivedJsonSTring);
				JSONArray jsonK = json.getJSONArray("key");
				JSONArray jsonV = json.getJSONArray("value");
				Log.v("Query","Getting * jsonKeyArray :"+ jsonK.toString() + "jsonValueArray :"+ jsonV.toString() );
				MatrixCursor cursorQuery = new MatrixCursor(new String[]{"key", "value"});
				int i = 0;
				while (i < jsonK.length())
				{
					Log.v("Query","inside MatrixCursor iterator" );
					cursorQuery.addRow(new Object[]{jsonK.get(i), jsonV.get(i)});
					Log.v("Query","inside * MatrixCursor key: "+ jsonK.get(i).toString()+ "value: "+ jsonV.get(i).toString());
					i++;
				}
				Log.v("Query", "Final query * result received key: "+ DatabaseUtils.dumpCursorToString(cursorQuery));
				return cursorQuery;
			}
			else {
				try  {
					keyhash = genHash(selection);
					porthash = genHash(portStr);
					//predhash = genHash(pred);
					int node_id = 0;
					if(genHash(Node_List[0]).compareTo(keyhash) >=0 || genHash(Node_List[4]).compareTo(keyhash)<0){
						node_id=0;
					}else if(genHash(Node_List[1]).compareTo(keyhash)>=0 && genHash(Node_List[0]).compareTo(keyhash)<0 ){
						node_id=1;
					}else if(genHash(Node_List[2]).compareTo(keyhash)>=0 && genHash(Node_List[1]).compareTo(keyhash)<0 ){
						node_id=2;
					}else if(genHash(Node_List[3]).compareTo(keyhash)>=0 && genHash(Node_List[2]).compareTo(keyhash)<0 ){
						node_id=3;
					}else if(genHash(Node_List[4]).compareTo(keyhash)>=0 && genHash(Node_List[3]).compareTo(keyhash)<0 ){
						node_id=4;
					}

					Coord = Integer.toString(Integer.parseInt(Node_List[node_id])*2);

					if (node_id == 3) {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[4])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[0])*2);
					}
					else if (node_id == 4) {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[0])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[1])*2);
					}
					else {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[node_id + 1])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[node_id + 2])*2);
					}

					if((Succ2).equals(myPort)){
						Cursor queryCursor = sqlDB.query("GroupMessenger", null, "key=?", new String[]{selection}, null, null, null, null);;
						while (queryCursor.getCount() == 0 ) {
							queryCursor = sqlDB.query("GroupMessenger", null, "key=?", new String[]{selection}, null, null, null, null);
						}
							Log.v("Query", "Query from database returned when myPort is successor2: "+ DatabaseUtils.dumpCursorToString(queryCursor) + " cursor length: " + queryCursor.getCount());
						Log.v("Query", "Query at successor2: " + Succ2 + " is myPort, query returned");
						return queryCursor;
					}
					else
					{
						Message msg = new Message();
						msg.setK(selection);
						//msg.setVal(value);
						msg.setTxt("Query for key");
						Log.v("Query", "Query for key: " + selection + " at: " + Succ2);
						String receivedJsonSTring = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ2).get();
						Log.v("Query","Query for key result received from client as : "+receivedJsonSTring);
						JSONObject json = new JSONObject(receivedJsonSTring);
						JSONArray jsonK = json.getJSONArray("key");
						JSONArray jsonV = json.getJSONArray("value");
						Log.v("Query","Getting key jsonKeyArray :"+ jsonK.toString() + "jsonValueArray :"+ jsonV.toString() );
						MatrixCursor cursorQuery = new MatrixCursor(new String[]{"key", "value"});
						int i = 0;
						while (i < jsonK.length())
						{
							Log.v("Query","inside query MatrixCursor iterator" );
							cursorQuery.addRow(new Object[]{jsonK.get(i), jsonV.get(i)});
							Log.v("Query","inside query MatrixCursor key: "+ jsonK.get(i).toString()+ "value: "+ jsonV.get(i).toString());
							i++;
						}
						return cursorQuery;
					}


				}catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}
		    }
		    catch(InterruptedException e){
				e.printStackTrace();
				Log.v("Query method","Interrupted Exception");
			} catch(ExecutionException e){
				e.printStackTrace();
				Log.v("Query method","Interrupted Exception");
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (Exception e){
				e.printStackTrace();
			}
		return null;
	}


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String Succ1 = null;
		String Succ2 = null;
		String Coord = null;
		String keyhash = null;
		try{
			if (selection.equals("@")) {
				myDB.deleteAll(sqlDB);
				return 1;
			}
			else if (selection.equals("*"))
			{
				Message msg = new Message();
				msg.setTxt("Delete");
				msg.setK(selection);
				//msg.setSourcePort(portStr);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), 11108 + "$" + 11112 + "$" + 11116 + "$" + 11120 + "$" + 11124);
				return 1;
			}
			else
			{
				try {
					Message msg = new Message();
					msg.setK(selection);
					//msg.setVal(value);
					msg.setTxt("Delete for key");
					keyhash = genHash(selection);
					porthash = genHash(portStr);
					//predhash = genHash(pred);
					int node_id = 0;
					if(genHash(Node_List[0]).compareTo(keyhash) >=0 || genHash(Node_List[4]).compareTo(keyhash)<0){
						node_id=0;
					}else if(genHash(Node_List[1]).compareTo(keyhash)>=0 && genHash(Node_List[0]).compareTo(keyhash)<0 ){
						node_id=1;
					}else if(genHash(Node_List[2]).compareTo(keyhash)>=0 && genHash(Node_List[1]).compareTo(keyhash)<0 ){
						node_id=2;
					}else if(genHash(Node_List[3]).compareTo(keyhash)>=0 && genHash(Node_List[2]).compareTo(keyhash)<0 ){
						node_id=3;
					}else if(genHash(Node_List[4]).compareTo(keyhash)>=0 && genHash(Node_List[3]).compareTo(keyhash)<0 ){
						node_id=4;
					}

					Coord = Integer.toString(Integer.parseInt(Node_List[node_id])*2);

					if (node_id == 3) {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[4])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[0])*2);
					}
					else if (node_id == 4) {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[0])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[1])*2);
					}
					else {
						Succ1 = Integer.toString(Integer.parseInt(Node_List[node_id + 1])*2);
						Succ2 = Integer.toString(Integer.parseInt(Node_List[node_id + 2])*2);
					}

					if((Coord).equals(myPort)) {
						sqlDB.delete("GroupMessenger", "key=?", new String[]{selection});
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ1 + "$" + Succ2);
					}
					else if((Succ1).equals(myPort)) {
						sqlDB.delete("GroupMessenger", "key=?", new String[]{selection});
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Coord + "$" + Succ2);
					}
					else if((Succ2).equals(myPort)) {
						sqlDB.delete("GroupMessenger", "key=?", new String[]{selection});
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Coord + "$" + Succ1);
					}
					else {
						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Succ1 + "$" + Succ2 + "$" + Coord);
					}

				}catch (NoSuchAlgorithmException e) {
					e.printStackTrace();
				}
			}

		}
		catch (Exception e){
			e.printStackTrace();
			Log.e("DELETE","exception");
		}
		return 0;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private boolean inHashRange(String key, String dest_port) {
		String keyhash = null;
		String predhash = null;
		String pred = null;
		int index = Arrays.asList(Port_List).indexOf(dest_port);
		pred = Port_List[(index + 5 - 1) % 5];
		Log.v("inHashRange", " key: " + key + " dest_port " + dest_port + " pred: " + pred + " myPort: " + myPort);
		try {
			keyhash = genHash(key);
			porthash = genHash(Integer.toString(Integer.parseInt(dest_port)/2));
			predhash = genHash(Integer.toString(Integer.parseInt(pred)/2));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		if ((predhash.compareTo(keyhash) < 0) && (keyhash.compareTo(porthash) <= 0))
		{
			return true;
		}
		else if ((predhash.compareTo(porthash) > 0) && (((keyhash.compareTo(predhash) > 0) || (keyhash.compareTo(porthash) < 0))))
		{
			return  true;
		}
		else
		{
			return false;
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
