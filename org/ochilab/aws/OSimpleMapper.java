package org.ochilab.aws;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
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
import com.amazonaws.services.simpledb.util.SimpleDBUtils;

/**
 * 
 * @author Youji Ochi, Yuki Takubo
 *         http://www.ochi-lab.org/research/project/osimplemapper
 * 
 */
public class OSimpleMapper {

	private AmazonSimpleDBClient sdb;
	private String prefix;
	private String endPoint;

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

		System.out.println(o.getClass().getSimpleName());

	}

	public void deleteDomain(String domainName) {

		DeleteDomainRequest request = new DeleteDomainRequest(domainName);
		sdb.deleteDomain(request);
		System.out.println("ドメイン名:" + domainName + "を削除しました");
	}

	public void delete(Class<?> c, String itemName) {
		sdb.deleteAttributes(new DeleteAttributesRequest(c.getSimpleName(),itemName));
	}

	public List select(Class<?> c, String query) throws SecurityException,
			IllegalArgumentException, InstantiationException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException {
		List list = new ArrayList();
		SelectRequest request = new SelectRequest(query, true);
		SelectResult result = sdb.select(request);
		for (Item row : result.getItems()) {
			Map<String, String[]> map = new HashMap<String, String[]>();

			for (Attribute attr : row.getAttributes()) {
				String value[] = new String[1];
				value[0] = attr.getValue();
				map.put(attr.getName(), value);
			}
			Object obj = this.mapToObject(c.newInstance().getClass(), map);
			list.add(obj);
		}
		return list;

	}

	public Object get(Class<?> c, String itemName) throws SecurityException,
			IllegalArgumentException, InstantiationException,
			IllegalAccessException, NoSuchMethodException,
			InvocationTargetException {
		Map result;
		Object obj = null;
		;

		GetAttributesResult getAttrResult = sdb
				.getAttributes(new GetAttributesRequest().withDomainName(
						c.getSimpleName()).withItemName(itemName));

		Map<String, String[]> map = new HashMap<String, String[]>();

		for (Attribute attr : getAttrResult.getAttributes()) {

			String value[] = new String[1];
			value[0] = attr.getValue();
			map.put(attr.getName(), value);
		}

		obj = this.mapToObject(c, map);

		return obj;
	}

	private HashMap<String, String> mapping(Object o)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {

		Object instance = o;
		Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
		Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
		HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
																		// Method>を取得

		List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
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
	private List<HashMap<String, String>> mapping(List<Object> list)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {

		if (list.size() < 1) {// まず初めにlistの要素数が１以上であるかどうかチェック！
			return null;
		} else {
			Object instance = list.get(0);// とりあえずlistに格納されている一個目のオブジェクトを取得する。
			Class<?> c = instance.getClass();// そのオブジェクト内の調査準備
			Method[] methods = c.getMethods();// そのオブジェクト内のメソッドを全部取得
			HashMap<String, Method> fMethodMap = getFieldMNames(methods);// Method[]から、Map<フィールド名,
																			// Method>を取得
			List<HashMap<String, String>> returnMapList = new ArrayList<HashMap<String, String>>();// 最後に返すマップリスト、Map<フィールド名,
			for (int i = 0; i < list.size(); i++) {// オブジェクトの数だけ繰り返す。
				Object object = list.get(i);
				HashMap<String, String> ffMap = new HashMap<String, String>();// 一人ずつのデータのために毎回HashMapをニュー
																				// 無駄な気がするけどこう書かなければ。
				for (Map.Entry<String, Method> map : fMethodMap.entrySet()) {

					Method method = map.getValue();
					ffMap.put(map.getKey(), method.invoke(object).toString());

				}
				returnMapList.add(ffMap);
			}
			return returnMapList;
		}
	}

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

	private Object mapToObject(Class<?> c, Map<String, String[]> map)
			throws InstantiationException, IllegalAccessException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			InvocationTargetException {
		Class<?> clazz = null;
		Object obj = null;
		obj = c.newInstance();
		clazz = obj.getClass();
		for (String key : map.keySet()) {// このfor文で一つのオブジェクトが完成！
			String setterMethodName = "set" + key.substring(0, 1).toUpperCase()
					+ key.substring(1);// mapのキー値（フィールド名）からセッターメソッド名を推測）
			String[] setterMethodArgument = (String[]) map.get(key);// mapの中身から、セッターメソッドの引数を入手
			Method method = clazz.getMethod(setterMethodName,
					new Class[] { String.class });// ↑の２値からセッターメソッド作成
			method.invoke(obj, setterMethodArgument);// セッターメソッド実行

		}
		// System.out.println(clazz.toString());
		return obj;
	}

}
