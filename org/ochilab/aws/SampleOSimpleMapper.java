public class SampleOSimpleMapper {

/**
 * 使い方を抜粋してますので、適時コピペして試して下さい。
 * @author Y.Ochi
 *         http://www.ochi-lab.org/research/project/osimplemapper
 * 
 */

	public static void main(String[] args) {
		
		//これらの値は絶対にGithubにアップしないでね
		String access_key="";
		String secret_key="";
		String endPoint="";
		
		OSimpleMapper osm = new OSimpleMapper(access_key,secret_key,endPoint,"");

    //追加		
	  testTable tb = new testTable();
		tb.setAge("41");
		tb.setItemName("0010");
		tb.setName("ochi");
		osm.put(tb);
		
		//testTable tb = (testTable)osm.get(testTable.class, "0003");
		//System.out.println("(main)name="+tb.getName());
		
		String[] a;
		List<testTable> list;
		try {
			//SQLで検索
			System.out.println("SQLで検索");
			list = osm.select(testTable.class, "select * from testTable");
			
			for(testTable t: list){
				System.out.println("itemName="+t.getItemName());
				a= t.getName();
				for(String s:a){
					System.out.println("name="+s);
				}
				System.out.println("age="+t.getAge());
				System.out.println("------");
			}
						
			//itemNameで検索
			System.out.println("itemNameで検索");
			
			testTable t = (testTable)osm.get(testTable.class, "0003");
			System.out.println("itemName="+t.getItemName());
			a= t.getName();
			for(String s:a){
				System.out.println("name="+s);
			}
			System.out.println("age="+t.getAge());
			System.out.println("------");
		
		  削除	
			//osm.delete(testTable.class, "0001");
			System.out.println("end");
			
			
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
