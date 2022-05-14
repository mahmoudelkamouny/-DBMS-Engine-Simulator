package SLOKS;

import java.awt.Dimension;
import java.awt.Polygon;

public class polygon extends java.awt.Polygon implements Comparable<polygon> {
    double area;
    
	public polygon() {
		super();
	}

	public polygon(int[] x, int[] y, int z) {
		super(x,y,z);
		Dimension d=getBounds().getSize();
		area=d.getWidth()*d.getHeight();
	}

	@Override
	public int compareTo(polygon p) {
	if(this.area<p.area){
		return -1;
	}
	else if(this.area>p.area){
		return 1;
	}
	else
		return 0;
		
	}
	
	public int mycompareto(polygon p) {
		for (int i = 0; i < this.xpoints.length; i++) {
			if (this.xpoints[i] > p.xpoints[i]) {
				return 1;
			} else if (this.xpoints[i] < p.xpoints[i]) {
				return -1;
			}
		}
		for (int i = 0; i < this.ypoints.length; i++) {
			if (this.ypoints[i] > p.ypoints[i]) {
				return 1;
			} else if (this.ypoints[i] < p.ypoints[i]) {
				return -1;
			}
		}
		return 0;

	}

	public String toString(){
		String s="";
		for(int i=0;i<this.xpoints.length;i++){
			s=s+"("+this.xpoints[i]+""+","+this.ypoints[i]+""+")";
    }
		s=s+" "+"area = "+this.area;
		return s;
	}
	

}
