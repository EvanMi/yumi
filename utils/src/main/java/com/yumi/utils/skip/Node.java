package com.yumi.utils.skip;

public class Node  <T extends Comparable<? super T>>{
    // 要保存到数据
    private T data;
    private Node<T>[] forward;

    @SuppressWarnings("unchecked")
    public Node(int level, T data) {
        this.forward = (Node<T>[]) new Node[level];
        this.data = data;
    }

    public T getData() {
        return data;
    }

    public Node<T>[] getForward() {
        return forward;
    }
}
