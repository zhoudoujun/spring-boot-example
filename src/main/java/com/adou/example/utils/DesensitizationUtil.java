package com.adou.example.utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.alibaba.fastjson.JSONObject;
public class DesensitizationUtil {
	private static final Logger LOG = LoggerFactory.getLogger(DesensitizationUtil.class);
	private static final String REPLACE_STRING = "*";
	private DesensitizationUtil() {}
	public static String getDesensitizationData(String data,String... keys) {
		String result = new String(data == null?"":data);
		try {
			JSONObject json = JSONObject.parseObject(result);
			for(String key:keys) {
				String value = json.getString(key);
				StringBuilder newValue = new StringBuilder("");
				if(!StringUtils.isEmpty(value)) {
					if(value.length() == 1) {
						newValue.append(REPLACE_STRING);
					}else if(value.length() == 2) {
						newValue.append(value.substring(0, 1) + REPLACE_STRING);
					}else {
						int valueLength = value.length();
						String firstChar = value.substring(0,1);
						String lastChar = value.substring(valueLength - 1, valueLength);
						newValue.append(firstChar);
						for(int replaceStringSize = 0; replaceStringSize < valueLength - 2; replaceStringSize ++) {
							newValue.append(REPLACE_STRING);
						}
						newValue.append(lastChar);
					}
				}
				json.replace(key, newValue.toString());
			}
			result = json.toJSONString();
		}catch (Exception e) {
			LOG.error("Desensitization Data " + data + " error", e);
		}
		return result;
	}
}

