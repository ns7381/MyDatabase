package com.my.database.api;

import com.google.common.base.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class Row {
    private List<Cell> cells;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Row row = (Row) o;
        return Objects.equal(cells, row.cells);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cells);
    }
}
