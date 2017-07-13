package storm.starter.utilities;

import java.util.ArrayDeque;


public class Tools {
	 
	//create envelope for target series
	public void lower_upper_lemire(double[] data, double[] l, double[] u, int size, int warp)
	{
		ArrayDeque<Integer> du = new ArrayDeque<Integer>(2*warp+2);
		ArrayDeque<Integer> dl = new ArrayDeque<Integer>(2*warp+2);
		
		du.addLast(0);
		dl.addLast(0);
	
		
		for(int i=1; i<size; i++)
		{
			if(i>warp)
			{
				u[i-warp-1] = data[du.getFirst()];
				l[i-warp-1] = data[dl.getFirst()];
			}
			
			if(data[i] > data[i-1])
			{
				du.removeLast();
				while(!du.isEmpty() && data[i]>data[du.getLast()])
					du.removeLast();	  
			}
			else
			{
				dl.removeLast();
				while(!dl.isEmpty() && data[i]<data[dl.getLast()])
					dl.removeLast();				  
			}
			
			du.addLast(i);
			dl.addLast(i);
				
			
			if(i == 2 * warp + 1 + du.getFirst())
				du.removeFirst();
			else if(i == 2 * warp + 1 + dl.getFirst())
				dl.removeFirst();
						
		}
		
		for(int i = size; i < size+warp+1; i++)
		{
		
			u[i-warp-1] = data[du.getFirst()];
			l[i-warp-1] = data[dl.getFirst()];
			
			if(i - du.getFirst() >= 2 * warp + 1)
				du.removeFirst();
			if(i - dl.getFirst() >= 2 * warp + 1)
				dl.removeFirst();		  
		
		}
		
	} 	
	
	
	
	public int min(int x, int y)
	{
		if(x < y)
		{
			return x;
		}
		else
		{
			return y;
		}
		
	}	
	
	public double min(double x, double y)
	{
		if(x < y)
		{
			return x;
		}
		else
		{
			return y;
		}
		
	}

	public int max(int x, int y)
	{
		if(x > y)
		{
			return x;
		}
		else
		{
			return y;
		}
		
	}		
	
	public double max(double x, double y)
	{
		if(x > y)
		{
			return x;
		}
		else
		{
			return y;
		}
		
	}	
	
	public double dist(double x, double y)
	{
		return (x-y)*(x-y);	
	}		

}
