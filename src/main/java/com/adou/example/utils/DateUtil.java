package com.adou.example.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期格式化
 * 
 * @author zhoudoujun01
 *
 */
public class DateUtil {
	
	public static String currentDateString(final String pattern) {
		return format(currentDate(), pattern);
	}

	public static Date currentDate() {
		return new Date();
	}

	public static String format(final Date date, final String pattern) {
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}
}
