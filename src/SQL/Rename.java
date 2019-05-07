package SQL;

public class Rename {
	public String tableName;
	public String columnName;
	public String newName;
	
	public Rename(String tableName, String columnName, String newName) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.newName = newName;
	}
	
	public Rename(String columnName) {
		this.columnName = columnName;
	}
}
