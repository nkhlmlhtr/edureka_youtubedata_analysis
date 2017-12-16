import java.util.Arrays;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class practice 
{
	public static void main(String[] args)
	{
		String input = "I am the best";
		String record;
		String related_ids="";
		String[] arr = input.split(" ");
		String[] previos = Arrays.copyOfRange(arr,0,2);
		StringBuilder sb = new StringBuilder();
		for(String word:previos)
		{
			sb.append(word);
			sb.append(" ");
		}
		record = sb.toString();
		for(int i=2;i<arr.length;i++)
		{
			if(i==2)
			{
				related_ids=related_ids+arr[i].toString();
			}
			else
			{
				related_ids=related_ids+"~"+arr[i].toString();
			}
		}
		record=record+related_ids;
		System.out.println(record);
		
	}
}
