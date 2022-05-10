package org.yunqiacademy.centralsystem.plugins.service;

import java.util.List;
import java.util.Map;

public interface OdpsService {
    List<Map<String, Object>> getOdpsData(String cityName, Double minLongitude, Double maxLongitude,
                                          Double minLatitude, Double maxLatitude);
}
