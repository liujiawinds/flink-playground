package org.hansight.finkplayground.utils;

import com.google.common.base.Preconditions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.jayway.jsonpath.JsonPath;

import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Type;

/**
 * It might be changed across versions.
 */
public class JsonHelper {

    public static String toJson(Object obj) {
        return JSON.toJSONString(obj);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        try {
            return JSON.parseObject(json, classOfT);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to parse json:%s into object:%s. Exception:%s", json, classOfT.getName(), e));
        }
    }

    public static <T> T fromJson(String json, Type typeOfT) {
        try {
            return JSON.parseObject(json, typeOfT);
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to parse json:%s into object:%s. Exception:%s", json, typeOfT.toString(), e));
        }
    }

    public static String getValueByPath(String jsonString, String jsonPath) {
        if (jsonString == null || jsonPath == null) {
            return null;
        }

        String obj;
        try {
            obj = JsonPath.read(jsonString, jsonPath);
        } catch (Exception e) {
            return null;
        }
        return obj;
    }

    public static <T> T getValueByPath(JSONObject jsonObject, String jsonPath, Class<T> tClass, T defValue) {
        T res = getValueByPath(jsonObject, jsonPath, tClass);
        if (res != null) {
            return res;
        } else {
            return defValue;
        }
    }

    public static <T> T getValueByPath(JSONObject jsonObject, String jsonPath, Class<T> tClass) {
        Preconditions.checkArgument(StringUtils.isNotBlank(jsonPath), "jsonPath is blank");
        Preconditions.checkNotNull(tClass, "tClass is blank");

        if (jsonObject == null) {
            return null;
        }

        Object obj;
        try {
            obj = JSONPath.eval(jsonObject, jsonPath);
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "failed to eval jsonObject[%s], jsonPath[%s], tClass[%s]", jsonObject, jsonPath, tClass));
        }
        if (obj == null) {
            return null;
        }
        return tClass.cast(obj);
    }
}
