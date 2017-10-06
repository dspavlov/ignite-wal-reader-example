package org.demo;

/**
 * Emulates business object
 */
public class SomeBusinessObject {
    private String value;
    private final int index;

    public SomeBusinessObject(String value, int index) {
        this.value = value;
        this.index = index;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    @Override public String toString() {
        return "SomeBusinessObject{" +
            "value='" + value + '\'' +
            ", index=" + index +
            '}';
    }
}
