package com.my.database.api;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Cell {
    private String type;
    private String size;
    private String name;
    private String val;

    public Cell(String type, String name, String val) {
        this.type = type;
        this.name = name;
        this.val = val;
    }

    public Cell(String name) {
        this.name = name;
    }
}
