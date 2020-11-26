package com.lastingwar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2020-11-25 18:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
