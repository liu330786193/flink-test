package com.lyl.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author lyl
 * @description: TODO
 * @date 2021/10/21 15:02
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    private Integer id;
    private Long ts;
    private Integer vc;

}
