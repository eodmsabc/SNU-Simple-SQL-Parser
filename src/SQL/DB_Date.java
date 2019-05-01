package SQL;

import java.io.Serializable;

public class DB_Date implements Comparable<DB_Date>, Serializable {

	private static final long serialVersionUID = 1L;

	public DB_Date(int y, int m, int d) {
		year = y;
		month = m;
		date = d;
	}
	
	int year;
	int month;
	int date;
	
	public int getIntDate() {
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
		
		if (!DB_Date.class.isAssignableFrom(obj.getClass())) {
			return false;
		}
		
		final DB_Date other = (DB_Date) obj;
		return (this.getIntDate() == other.getIntDate());
	}
	
	@Override
	public int compareTo(DB_Date other) {
		return this.getIntDate() - other.getIntDate();
	}
	
	@Override
	public String toString() {
		return year + (month < 10? "-0" : "-") + month + (date < 10? "-0" : "-") + date;
	}
}
