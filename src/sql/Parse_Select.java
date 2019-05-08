package sql;

import java.util.ArrayList;

public class Parse_Select {
	public ArrayList<Rename> selected;
	public ArrayList<Rename> tables;
	public BooleanExpression boolPredicate;
	
	public Parse_Select(ArrayList<Rename> s, ArrayList<Rename> tables, BooleanExpression boolPredicate) {
		selected = s;
		this.tables = tables;
		if (boolPredicate == null) {
			this.boolPredicate = new BooleanExpression(null);
		}
		else {
			this.boolPredicate = boolPredicate;
		}
	}
	
	public DBMessage validCheck() {

		if(!newTableNameDuplicateCheck(tables)) {
			return new DBMessage(MsgType.WhereAmbiguousReference);
		}
		
		return null;
	}
	
	private boolean newTableNameDuplicateCheck(ArrayList<Rename> tables) {
		ArrayList<String> newNameDupCheck = new ArrayList<String>();
		
		for (Rename r : tables) {
			if (newNameDupCheck.contains(r.newName)) {
				// Duplicate table name TODO need to check
				return false;
			}
			else {
				newNameDupCheck.add(r.newName);
			}
		}
		return true;
	}
}
