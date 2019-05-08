package sql;

import java.io.Serializable;

public class Date implements Comparable<Date>, Serializable {

	private static final long serialVersionUID = 1L;

	public Date(String rawDate) {
		year = Integer.parseInt(rawDate.substring(0, 4));
		month = Integer.parseInt(rawDate.substring(5, 7));
		date = Integer.parseInt(rawDate.substring(8, 10));
	}
	
	/*
	public Date(int y, int m, int d) {
		year = y;
		month = m;
		date = d;
	}
	*/
	
	int year;
	int month;
	int date;
	
	public int getValue() {
		// What about calculating actual date difference
		return year * 10000 + month * 100 + date;
	}
	
	public static boolean isValid(int y, int m, int d) {
		if (y < 0 || m < 0 || d < 0) {
			return false;
		}
		else {
			return true;
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		
		if (!Date.class.isAssignableFrom(obj.getClass())) {
			return false;
		}
		
		final Date other = (Date) obj;
		return (this.getValue() == other.getValue());
	}
	
	@Override
	public int compareTo(Date other) {
		return this.getValue() - other.getValue();
	}
	
	@Override
	public String toString() {
		return year + (month < 10? "-0" : "-") + month + (date < 10? "-0" : "-") + date;
	}
}
