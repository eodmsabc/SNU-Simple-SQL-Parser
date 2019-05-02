package SQL;

import java.io.Serializable;

public class Value implements Comparable<Value>, Serializable {
	private static final long serialVersionUID = 1L;
	
	DataType type;
	
	Integer intVal;
	String strVal;
	Date dateVal;
	
	public Value() {
		type = DataType.TYPE_INT;
		intVal = null;
		strVal = null;
		dateVal = null;
	}
	
	public Value(int i) {
		type = DataType.TYPE_INT;
		intVal = new Integer(i);
	}
	
	public Value(String s) {
		type = DataType.TYPE_CHAR;
		strVal = s;
	}
	
	public Value(Date d) {
		type = DataType.TYPE_DATE;
		dateVal = d;
	}

	public boolean isNull() {
		if (type == DataType.TYPE_INT && intVal == null) return true;
		return false;
	}
	
	@Override
	public int compareTo(Value other) {
		if (type == DataType.TYPE_CHAR) {
			return strVal.compareTo(other.strVal);
		}
		else if (type == DataType.TYPE_DATE) {
			return dateVal.compareTo(other.dateVal);
		}
		else {
			return intVal - other.intVal;
		}
	}
	
	@Override
	public String toString() {
		String ret = "";
		switch(type) {
		case TYPE_INT:
			ret = intVal.toString();
			break;
		case TYPE_CHAR:
			ret = strVal.toString();
			break;
		case TYPE_DATE:
			ret = dateVal.toString();
			break;
		}
		return ret;
	}
	
	public static boolean typeCheck(Value a, Value b) {
		if (a.type == b.type) return true;
		else return false;
	}
}
