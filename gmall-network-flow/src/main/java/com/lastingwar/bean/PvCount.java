package com.lastingwar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2020-11-26 10:06
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PvCount {
    private String pv;
    private Long windowEnd;
    private Long count;
}
