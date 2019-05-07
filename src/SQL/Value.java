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
		intVal = i;
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
	
	public void setNull() {
		type = DataType.TYPE_INT;
		intVal = null;
	}
	
	public int getLength() {
		return toString().length();
	}
	
	public static boolean comparable(Value v1, Value v2) {
		// TODO Null 값 질문에 달리는 답에 따라 적절하게 바꿔야 할듯
		if (v1.isNull() || v2.isNull()) {
			return true;
		}
		
		return typeCheck(v1, v2);
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
			return intVal.compareTo(other.intVal);
		}
	}
	
	@Override
	public String toString() {
		if (isNull()) {
			return "null";
		}
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
