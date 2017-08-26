package com.wintoo.model;

import lombok.Data;

import java.util.Date;

/**
 * Created by Jason on 15/8/26.
 */
@Data
public class EquipError {
    private String uuid;
    private String equipid;
    private Date   inserttime;
    private String status;
    private String remark;
}
