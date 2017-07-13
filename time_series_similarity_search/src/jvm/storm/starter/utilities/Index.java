package storm.starter.utilities;

public class Index implements Comparable<Index>{
	
	private double value;
	private int index;
	
	public Index()
	{
		this.value = -1;
		this.index = -1;
	}
	
	public Index(double value, int index){
		this.value = value;
		this.index = index;
	}
	
	public double getValue() {
		return value;
	}
	
	public void setValue(double value) {
		this.value = value;
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}

	@Override
	public int compareTo(Index o) {
		
		// From high to low
		if(Math.abs(this.value) > Math.abs(o.value))
		{
			return -1;
		}
		else
		{
			return 1;
		}


	}

	
}
