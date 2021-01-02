package com.ada.common.collections;

public interface ChangeAction<FROM, TO> {
    TO action(FROM from);
}
