package org.ochilab.aws;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;

import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

/**
 * AWS-SDKネイティブ対応のSimpleDB用 ORマッパー
 * @author Youji Ochi, Yuki Takubo
 *         http://www.ochi-lab.org/research/project/osimplemapper
 * 
 */
public class OSimpleMapper {

	private AmazonSimpleDBClient sdb;
	private String prefix;
	private Map<Class<?>,ClassData> _class_classData__Map=new HashMap<Class<?>,ClassData>();
	
	
	
	public OSimpleMapper(String awsId, String secretKey, String endPoint,
			String prefix) {// コンストラクター
		_OSimpleDB(awsId, secretKey, endPoint, prefix);
	}

	public OSimpleMapper(String awsId, String secretKey, String endPoint) {// コンストラクター
		this.prefix = "";
		_OSimpleDB(awsId, secretKey, endPoint, prefix);
	}

	private void _OSimpleDB(String awsId, String secretKey, String endPoint,
			String prefix) {
		sdb = new AmazonSimpleDBClient(
				new BasicAWSCredentials(awsId, secretKey));
		sdb.setEndpoint(endPoint);
		this.prefix = prefix;
	}

	/*
	 * 新規登録処理
	 */
	public void put(Object o) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {

		HashMap<String, String> map = this.mapping(o);
		String itemName = map.get("itemName");
		map.remove("itemName");
		PutAttributesRequest request = new PutAttributesRequest();
		Set<String> keyList = map.keySet();
		for (String key : keyList) {

			ReplaceableAttribute data = new ReplaceableAttribute();
			data.withName(key).withValue(map.get(key));

			request.withDomainName(o.getClass().getSimpleName())
					.withItemName(itemName).withAttributes(data);

			sdb.putAttributes(request);
		}

		//System.out.println(o.getClass().getSimpleName());

	}

	
	/*
	 * ドメインの削除
	 */
	public void deleteDomain(String domainName) {

		DeleteDomainRequest request = new DeleteDomainRequest(domainName);
		sdb.deleteDomain(request);
		System.out.println("ドメイン名:" + domainName + "を削除しました");
	}

	/*
	 * item名指定による削除
	 */
	public void delete(Class<?> c, String itemName) {
		sdb.deleteAttributes(new DeleteAttributesRequest(c.getSimpleName(),itemName));
	}

	/**
	 * Select文による検索
	 * @param c データクラス名
	 * @param query クエリー
	 * @return
	 * @throws SecurityException
	 * @throws IllegalArgumentException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	public List select(Class<?> c, String query) throws SecurityException,
			IllegalArgumentException, InstantiationException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException {
		List list = new ArrayList();
		SelectRequest request = new SelectRequest(query, true);
		SelectResult result = sdb.select(request); //検索メソッド本体
		for (Item row : result.getItems()) {
			
			Map<String, String[]> map = new HashMap<String, String[]>();
			String[] temp = new String[1];
			temp[0]=row.getName();
			map.put("ItemName", temp);
			for (Attribute attr : row.getAttributes()) {
			
				//既に存在していれば	
				if(map.containsKey(attr.getName())){
					List<String> sList = new ArrayList<String>(Arrays.asList(map.get(attr.getName())));
					sList.add(attr.getValue());
					String[] array=(String[])sList.toArray(new String[0]);
					map.put(attr.getName(), array);					
				}else{ //初めてなら
		
					String value[] = new String[1];
					value[0] = attr.getValue();
					map.put(attr.getName(), value);					
				}
				
			}
			//Object obj = this.mapToObject(c.newInstance().getClass(), map);
			//Object obj = this.mapListToObjectList(map,c);
			//list.add(obj);
			list.add(map);
		}
		
		
		return mapListToObjectList(list, c);
		
		//return list;

	}

	/**
	 * アイテム名による検索
	 * 
	 */
	public Object get(Class<?> c, String itemName) throws SecurityException,
			IllegalArgumentException, InstantiationException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException {
		Object obj = null;
		GetAttributesResult getAttrResult = sdb
				.getAttributes(new GetAttributesRequest().withDomainName(
						c.getSimpleName()).withItemName(itemName));

		Map<String, String[]> map = new HashMap<String, String[]>();

		for (Attribute attr : getAttrResult.getAttributes()) {
			String value[] = new String[1];
			value[0] = attr.getValue();
			map.put(attr.getName(), value);
		}
		
		String[] temp = new String[1];
		temp[0]=itemName;
		map.put("ItemName", temp);
		List list = new ArrayList();
		list.add(map);
		List result = mapListToObjectList(list, c);
		//obj = this.mapToObject(c, map);

		return result.get(0);
	}

	private HashMap<String, String> mapping(Object o)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {

		Object instance = o;
		Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
		Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
		HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
																		// Method>を取得

		//List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
																								// フィールドの値>のList
		HashMap<String, String> ffMap = new HashMap<String, String>();// 一人ずつのデータのために毎回HashMapをニュー
		for (Map.Entry<String, Method> map : fMethodMap.entrySet()) {

			Method method = map.getValue();
			ffMap.put(map.getKey(), method.invoke(o).toString());

		}
		return ffMap;
	}

	/**
	 * 
	 * @param list
	 * @return
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
//	private List<HashMap<String, String>> mapping(List<Object> list)
//			throws IllegalArgumentException, IllegalAccessException,
//			InvocationTargetException {
//
//		if (list.size() < 1) {// まず初めにlistの要素数が１以上であるかどうかチェック！
//			return null;
//		} else {
//			Object instance = list.get(0);// とりあえずlistに格納されている一個目のオブジェクトを取得する。
//			Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
//			Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
//			HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
//																			// Method>を取得
//			List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
//			for (int i = 0; i < list.size(); i++) {// オブジェクトの数だけ繰り返す。
//				Object object = list.get(i);
//				HashMap<String, String> ffMap = new HashMap<String, String>();// 一人ずつのデータのために毎回HashMapをニュー
//																				// 無駄な気がするけどこう書かなければ。
//				for (Map.Entry<String, Method> map : fMethodMap.entrySet()) {
//
//					Method method = map.getValue();
//					ffMap.put(map.getKey(), method.invoke(object).toString());
//
//				}
//				returnMapList.add(ffMap);
//			}
//			return returnMapList;
//		}
//	}

	private HashMap<String, Method> getFieldMNames(Method[] methods) {// Method[]
																		// から、フィールド名とゲッターメソッドをゲットしてマップにまとめて返すよ。
		String methodName, fieldName;
		HashMap<String, Method> map = new HashMap<String, Method>();
		for (int i = 0; i < methods.length; i++) {
			methodName = methods[i].getName();
			if (methodName.startsWith("get") && (methodName != "getClass")) {
				fieldName = methodName.replaceAll("get", "");
				fieldName = fieldName.substring(0, 1).toLowerCase()
						+ fieldName.substring(1);// ゲッターメソッドが用意されているフィールド名を取得
				map.put(fieldName, methods[i]);// フィールド名、メソッド名のマップを取得
			}
		}
		return map;
	}


//	private Object mapToObject(Class<?> c, Map<String, String[]> map)
//			throws InstantiationException, IllegalAccessException,
//			SecurityException, NoSuchMethodException, IllegalArgumentException,
//			InvocationTargetException {
//		Class<?> clazz = null;
//		Object obj = null;
//		obj = c.newInstance();
//		clazz = obj.getClass();
//		for (String key : map.keySet()) {// このfor文で一つのオブジェクトが完成！
//			String setterMethodName = "set" + key.substring(0, 1).toUpperCase()
//					+ key.substring(1);// mapのキー値（フィールド名）からセッターメソッド名を推測）
//			String[] setterMethodArgument = (String[]) map.get(key);// mapの中身から、セッターメソッドの引数を入手
//			Method method = clazz.getMethod(setterMethodName,
//					new Class[] { String.class });// ↑の２値からセッターメソッド作成
//			method.invoke(obj, setterMethodArgument);// セッターメソッド実行
//
//		}
//		// System.out.println(clazz.toString());
//		return obj;
//	}	
//	
//	
	private <T> List<T> mapListToObjectList(List<Map<String, String[]>> inputList, Class<?> clazz) {
		if (inputList.size() == 0) {
			return null;
		}
		if(!_class_classData__Map.containsKey(clazz)){//オブジェクトのフィールド　メソッドマップが登録されているかどうかチェック
			add__classClassData_Map(clazz);//されていなかったら、このメソッドで登録する。
		}
		
		ClassData classData = _class_classData__Map.get(clazz);
		Map<String, Method> map_fieldName_setterMethod = classData.getFieldNameSetterMethod_Map();
		
		List returnList = new ArrayList();// リターン用のリストを作成
		
		
		for (int i = 0; i < inputList.size(); i++) {// 各オブジェクトごとに繰り返す。
			Map<String, String[]> dataMap = inputList.get(i);// 入力されたListからMap<String, String[]>を取り出す
			try {
				//T createdInstance = (T) clazz.newInstance();// とりあえず、型に合ったインスタンスを作成
				Object createdInstance =  clazz.newInstance();
				for (String key : map_fieldName_setterMethod.keySet()) {// instanceを完成させるfor文
					Method setterMethod = map_fieldName_setterMethod.get(key);// setterメソッド取得
					Class[] setterMethodArgumentClasses = setterMethod.getParameterTypes();//全ての引数の型を取得
					String setterMethodArgumentClassName=setterMethodArgumentClasses[0].getName();//セッターメソッドの引数は必ず一つなので０個目の引数のクラス名だけ取得

					if(setterMethodArgumentClassName.equals("java.lang.String")){
						setterMethod.invoke(createdInstance, dataMap.get(key)[0]);// ０個目だけ入れる
					}
					else{
						setterMethod.invoke(createdInstance,(Object)dataMap.get(key));// 配列ごと入れる
					}
				}
				returnList.add(createdInstance);
			
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		return returnList;
	}
	
	private void add__classClassData_Map(Class<?> clazz){
		String className=clazz.getSimpleName();
		
		ClassData cd=new ClassData();
		String domainName = prefix +className; 
		cd.setDomainName(domainName);
		////////////////////////////////////////////////fieldNameFieldvalueArray_Map,fieldNameFieldvalue_Mapを完成させるブロック
		Method[] declaredMethods = clazz.getDeclaredMethods();
		Map<String,Method> fieldNameGetterMethod_Map = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		Map<String,Method> fieldNameSetterMethod_Map = new HashMap<String, Method>();
		for (Method method : declaredMethods) {// このクラスで宣言された全てのメソッド　getClassは含まない
			String methodName = method.getName();
			if (methodName.matches("^get[\\dA-Z]\\w*")) {// getterメソッドの場合
				String key = methodName.substring(3);//4文字目以降だけを取得
				key = key.substring(0, 1).toLowerCase()+key.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameGetterMethod_Map.put(key, method);
			}else if(methodName.matches("^set[\\dA-Z]\\w*")){
				String fieldName = methodName.substring(3);//4文字目以降だけを取得
				fieldName = fieldName.substring(0, 1).toLowerCase()+fieldName.substring(1);//先頭文字だけを小文字にする。field名がkeyになる。
				fieldNameSetterMethod_Map.put(check_IS_itemName(fieldName), method);
			}
		}
		cd.setFieldNameGetterMethod_Map(fieldNameGetterMethod_Map);
		cd.setFieldNameSetterMethod_Map(fieldNameSetterMethod_Map);
		_class_classData__Map.put(clazz, cd);
	}

	private String check_IS_itemName(String word){
		if(word.equals("itemName")){
			word="ItemName";
		}
		return word;
	}
	
	private class ClassData{
		private String domainName;
		private Map<String, Method> fieldNameGetterMethod_Map;// = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		private Map<String, Method> fieldNameSetterMethod_Map;// = new HashMap<String, Method>();// フィールドname,GetterMethodMap
		
		private String getDomainName() {
			return domainName;
		}
		private void setDomainName(String domainName) {
			this.domainName = domainName;
		}
		private Map<String, Method> getFieldNameGetterMethod_Map() {
			return fieldNameGetterMethod_Map;
		}
		private void setFieldNameGetterMethod_Map(Map<String, Method> fieldNameGetterMethod_Map) {
			this.fieldNameGetterMethod_Map = fieldNameGetterMethod_Map;
		}
		private Map<String, Method> getFieldNameSetterMethod_Map() {
			return fieldNameSetterMethod_Map;
		}
		private void setFieldNameSetterMethod_Map(Map<String, Method> fieldNameSetterMethod_Map) {
			this.fieldNameSetterMethod_Map = fieldNameSetterMethod_Map;
		}
		
	}
}
