package com.lastingwar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2020-11-25 19:23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlViewCount {
    private String url;
    private Long windowEnd;
    private Long count;
}
