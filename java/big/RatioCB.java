package big;

import org.apache.spark.sql.Row;

public class RatioCB {

	private Integer idItem;
	private Integer idItemTent;
	private Integer similitud;
	
	
	@Override
	public Integer toInteger() {
		return "RatioCB [idItem=" + idItem + ", idItemTent=" + idItemTent + ", similitud=" + similitud + "]";
	}



	public Integer getIdItemTent() {
		return idItemTent;
	}



	public Integer getIdItem() {
		return idItem;
	}



	public void setIdItem(Integer idItem) {
		this.idItem = idItem;
	}



	public Integer getSimilitud() {
		return similitud;
	}



	public void setSimilitud(Integer similitud) {
		this.similitud = similitud;
	}



	public void setIdItemTent(Integer idItemTent) {
		this.idItemTent = idItemTent;
	}



	
	
	public RatioCB() {
		// TODO Auto-generated constructor stub
		
	
	
	}
	public RatioCB(Row _x, Integer index){
		idItem=_x.getInt(0);
		idItemTent=_x.getInt(0);
		similitud=_x.getInt(0);
	}

}
