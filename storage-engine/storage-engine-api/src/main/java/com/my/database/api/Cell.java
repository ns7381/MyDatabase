package com.my.database.api;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Cell {
    private String type;
    private String size;
    private String name;
    private Object val;
    private boolean isPrimary;

    public Cell(String name) {
        this.name = name;
    }

    public Cell(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public Cell(String name, Object val) {
        this.name = name;
        this.val = val;
    }

    public Cell(String type, String name, Object val) {
        this.type = type;
        this.name = name;
        this.val = val;
    }

}
