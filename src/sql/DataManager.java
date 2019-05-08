package sql;

import java.io.*;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class DataManager {
	// Serialize object
	public static <T extends Serializable> byte[] serialize(T obj) {
		byte[] data = null;
		ByteArrayOutputStream byteOut;
		ObjectOutputStream objOut;

		try {
			byteOut = new ByteArrayOutputStream();
			objOut = new ObjectOutputStream(byteOut);

			objOut.writeObject(obj);
			data = byteOut.toByteArray();

			byteOut.close();
			objOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return data;
	}

	// Deserialize object
	public static Object deserialize(byte[] data) {
		Object retobj = null;
		if (data != null) {
			ByteArrayInputStream byteIn;
			ObjectInputStream objIn;

			try {
				byteIn = new ByteArrayInputStream(data);
				objIn = new ObjectInputStream(byteIn);

				retobj = objIn.readObject();

				byteIn.close();
				objIn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return retobj;
	}

	// Insert byte data in key-value pair
	public static void insert(Database myDatabase, String key, byte[] data) {
		Cursor cursor = null;
		try {
			cursor = myDatabase.openCursor(null, null);
			DatabaseEntry db_key = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry db_data = new DatabaseEntry(data);
			cursor.put(db_key, db_data);
		} catch (Exception e) {
			e.printStackTrace();
		}
		cursor.close();
	}

	// Replace value with new byteData
	public static void replace(Database myDatabase, String key, byte[] newData) {
		Cursor cursor = null;
		try {
			cursor = myDatabase.openCursor(null, null);
			DatabaseEntry db_key = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry db_value = new DatabaseEntry();
			DatabaseEntry new_value;
			if (cursor.getSearchKey(db_key, db_value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				new_value = new DatabaseEntry(newData);
				cursor.putCurrent(new_value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		cursor.close();
	}

	// Get byte data from key
	public static byte[] search(Database myDatabase, String key) {
		Cursor cursor = null;
		byte[] value = null;
		try {
			cursor = myDatabase.openCursor(null, null);
			DatabaseEntry db_key = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry db_value = new DatabaseEntry();
			if (cursor.getSearchKey(db_key, db_value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				value = db_value.getData();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		cursor.close();
		return value;
	}

	// Delete key-value pair
	public static void delete(Database myDatabase, String key) {
		Cursor cursor = null;
		try {
			cursor = myDatabase.openCursor(null, null);
			DatabaseEntry db_key = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry db_value = new DatabaseEntry();
			if (cursor.getSearchKey(db_key, db_value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				cursor.delete();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		cursor.close();
	}
}
