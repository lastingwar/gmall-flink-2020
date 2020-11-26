package com.lastingwar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2020-11-25 11:59
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserData {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
