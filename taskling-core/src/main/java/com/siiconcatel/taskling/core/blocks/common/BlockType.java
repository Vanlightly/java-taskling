package com.siiconcatel.taskling.core.blocks.common;

import java.util.HashMap;
import java.util.Map;

public enum BlockType
{
    NotDefined(0),
    NumericRange(1),
    DateRange(2),
    List(3),
    Object(4);

    private int numVal;

    BlockType(int numVal) {
        this.numVal = numVal;
    }

    public int getNumVal() {
        return numVal;
    }


    private static Map map = new HashMap<>();
    public static BlockType valueOf(int blockType) {
        return (BlockType) map.get(blockType);
    }

    static {
        for (BlockType blockType : BlockType.values()) {
            map.put(blockType.numVal, blockType);
        }
    }
}
