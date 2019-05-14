package sql;

import java.io.Serializable;
import java.util.ArrayList;

public class ForeignKeyConstraint implements Serializable {
	private static final long serialVersionUID = 1L;
	
	ArrayList<String> foreignKeys;
	String refTable;
	ArrayList<String> referingKeys;
	boolean nullable = false;		// Pre-calculation for cascade deletion
	
	public ForeignKeyConstraint(ArrayList<String> fkey, String table, ArrayList<String> rkey) {
		foreignKeys = fkey;
		refTable = table;
		referingKeys = rkey;
	}
}
