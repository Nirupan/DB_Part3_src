package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;

public class IndexScanNode extends PlanNode.Leaf{
	public List<String> table=new ArrayList<String>();
	static int iCnt= 0;
	public ExprTree condition;
    public List<Schema.Table> schema=new ArrayList<Schema.Table>();

	public IndexScanNode(Structure struct, Type type) {
		super(PlanNode.Type.INDEXSCAN);
		// TODO Auto-generated constructor stub
	}

	@Override
	public List<Var> getSchemaVars() {
		// TODO Auto-generated method stub

	    List<Schema.Var> vars = new ArrayList<Schema.Var>();
	    for(Schema.Column c : schema.get(iCnt)){
	      vars.add(c.name);
	    }
	    return vars;
	  
	}

	@Override
	public String toString(String indent) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder("INDEXSCAN");
	    String sep = "";
	    
	    sb.append(" [");
	    sb.append(condition);
	    sep = ", ";
	    Iterator itr=table.iterator();
		while (itr.hasNext()) {
			sb.append("; ");
			sb.append(itr.next());
			sb.append("(");
			for (Schema.Var v : getSchemaVars()) {
				
				sb.append(v.name);
				sb.append(sep);
			}
			sb.replace(sb.length()-2, sb.length(), "");
			iCnt++;
			sb.append(")");
		}
		sb.append(")]");
		return sb.toString();
	}
	
	
	
	
}