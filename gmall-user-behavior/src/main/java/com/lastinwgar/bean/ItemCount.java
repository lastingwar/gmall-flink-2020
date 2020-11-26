package com.lastinwgar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yhm
 * @create 2020-11-25 13:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ItemCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
