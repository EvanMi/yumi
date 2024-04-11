package com.yumi.utils.skip;

import java.util.Random;

public class SkipList<T extends Comparable<? super T>> {
    private static int MAX_LEVEL = 16;
    //当前最大层数
    private int levelCount = 0;
    private Node<T> head = new Node<>(MAX_LEVEL, null);
    private Random  r = new Random();
    private double p = 0.5;

    private int cnt;


    private int randomLevel() {
        int level = 1;
        for (int i = 1; i < MAX_LEVEL; i++) {
            if (r.nextGaussian() < this.p) {
                level++;
            } else {
                break;
            }
        }
        return level;
    }

    @SuppressWarnings("unchecked")
    public void insert(T value) {
        int level = this.randomLevel();
        if (level > this.levelCount) {
            this.levelCount = level;
        }
        Node<T> newNode = new Node<>(level, value);
        Node<T>[] update = (Node<T>[]) new Node[level];
        for (int i = 0; i < level; i ++) {
            update[i] = this.head;
        }

        Node<T> p = this.head;

        for (int i = level - 1; i >= 0; i--) {
            while (p.getForward()[i] != null && p.getForward()[i].getData().compareTo(value) < 0) {
                p = p.getForward()[i];
            }
            update[i] = p;
        }

        if (update[0].getForward()[0] != null && update[0].getForward()[0].getData().compareTo(value) == 0) {
            return;
        }

        for (int i = 0; i < level; i++) {
            newNode.getForward()[i] = update[i].getForward()[i];
            update[i].getForward()[i] = newNode;
        }
        this.cnt++;
    }

    public Node<T> find(T value) {
        Node<T> p = this.head;
        for (int i = this.levelCount - 1; i >= 0; i--) {
            while (p.getForward()[i] != null && p.getForward()[i].getData().compareTo(value) < 0) {
                p = p.getForward()[i];
            }
        }

        if (p.getForward()[0] != null && p.getForward()[0].getData().compareTo(value) == 0) {
            return p.getForward()[0];
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public void delete(T value) {
        Node<T>[] update = (Node<T>[])new Node[this.levelCount];
        for (int i = 0; i < this.levelCount; i++) {
            update[i] = head;
        }
        Node<T> p = head;
        for (int i = this.levelCount - 1; i >= 0; i--) {
            while (p.getForward()[i] != null && p.getForward()[i].getData().compareTo(value) < 0) {
                p = p.getForward()[i];
            }
            update[i] = p;
        }

        if (p.getForward()[0] != null && p.getForward()[0].getData().compareTo(value) == 0) {
            for (int i = this.levelCount - 1; i >= 0; i--) {
                if (update[i].getForward()[i] != null && update[i].getForward()[i].getData().compareTo(value) == 0) {
                    update[i].getForward()[i] = update[i].getForward()[i].getForward()[i];
                }
            }
            this.cnt--;
        }
    }

    public int size() {
        return this.cnt;
    }
}
