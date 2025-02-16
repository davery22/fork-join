package io.avery.util;

import java.lang.ref.WeakReference;
import java.util.*;

// TODO: Implement RandomAccess?
// TODO: can rootShift get too big? (ie shift off the entire index -> 0, making later elements unreachable)
//  - I think the answer is "yes, but we'd still find later elements by linear searching a size table"
// TODO: Right shifts can wrap! eg: (int >>> 35) == (int >>> 3)
//  - Force result to 0 when shift is too big

public class TrieForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E> {
    private static final int MARGIN = 2;
    private static final int SHIFT = 4;
    private static final int SPAN = 1 << SHIFT;
    private static final int MASK = SPAN-1;
    private static final Node INITIAL_TAIL = new Node(new WeakReference<>(null), new Object[SPAN]);
    
    private int size;
    private int tailSize;
    private int rootShift;
    private Node root;
    private Node tail;
    
    public TrieForkJoinList() {
        size = tailSize = rootShift = 0;
        root = null;
        tail = INITIAL_TAIL;
    }
    
    public TrieForkJoinList(Collection<? extends E> c) {
        this();
        addAll(c);
    }
    
    protected TrieForkJoinList(TrieForkJoinList<? extends E> toCopy) {
        size = toCopy.size;
        tailSize = toCopy.tailSize;
        rootShift = toCopy.rootShift;
        root = toCopy.root;
        tail = toCopy.tail;
    }
    
    // add(index, element)
    // addAll(index, collection) - AbstractList, but we can do better
    // remove(index)
    // removeAll(collection)
    // replaceAll(collection)
    // retainAll(collection)
    // removeIf(predicate)
    // toArray() - AbstractCollection, but we can do better(?)
    // toArray(arr) - AbstractCollection, but we can do better(?)
    // toArray(gen) - Collection, but we can do better(?)
    // iterator()
    // listIterator()
    // listIterator(index)
    // subList(from, to)
    // spliterator()
    // join()
    
    // -get(index)
    // -set(index, element)
    // -add(element)
    // -clear()
    // -reversed()
    // -size()
    // -fork()
    // -splice()
    // -splice(replacement)
    
    // -addAll(collection) - AbstractCollection
    // -addFirst() - List
    // -addLast() - List
    // -contains(object) - AbstractCollection
    // -containsAll(collection) - AbstractCollection
    // -equals(object) - AbstractList
    // -getFirst() - List
    // -getLast() - List
    // -hashCode() - AbstractList
    // -indexOf() - AbstractList
    // -isEmpty() - AbstractCollection
    // -lastIndexOf() - AbstractList
    // -remove(object) - AbstractCollection
    // -removeFirst() - List
    // -removeLast() - List
    // -sort(comparator) - List
    // -toString() - AbstractCollection
    // -forEach(action) - Iterable
    // -stream() - Collection
    // -parallelStream() - Collection
    
    @Override
    public E get(int index) {
        Objects.checkIndex(index, size);
        Node node;
        
        if (index >= tailOffset()) {
            node = tail;
        }
        else {
            node = root;
            for (int shift = rootShift; shift > 0; shift -= SHIFT) {
                int childIdx = index >>> shift;
                if (node instanceof SizedNode sn) {
                    int[] sizes = sn.sizeTable.sizes;
                    while (sizes[childIdx] <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes[childIdx-1];
                    }
                }
                node = (Node) node.children[childIdx & MASK];
            }
        }
        
        @SuppressWarnings("unchecked")
        E value = (E) node.children[index & MASK];
        return value;
    }
    
    @Override
    public E set(int index, E element) {
        Objects.checkIndex(index, size);
        modCount++;
        int tailOffset = tailOffset();
        Node node;
        
        if (index >= tailOffset) {
            node = tail = tail.ensureEditable(this);
            index -= tailOffset;
        }
        else {
            node = root;
            Object parent = this;
            int childIdx = -1;
            
            for (int shift = rootShift; ; shift -= SHIFT) {
                node = node.ensureEditable(parent);
                parent = setChild(parent, childIdx, node);
                if (shift == 0) {
                    break;
                }
                childIdx = index >>> shift;
                if (node instanceof SizedNode sn) {
                    int[] sizes = sn.sizeTable.sizes;
                    while (sizes[childIdx] <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes[childIdx-1];
                    }
                }
                node = (Node) node.children[childIdx &= MASK];
            }
        }
        
        @SuppressWarnings("unchecked")
        E old = (E) node.children[index & MASK];
        node.children[index & MASK] = element;
        return old;
    }
    
    @Override
    public boolean add(E e) {
        modCount++;
        addToTail(e);
        return true;
    }
    
    @Override
    public void add(int index, E element) {
        rangeCheckForAdd(index);
        modCount++;
        
        if (index == size-1) {
            addToTail(element);
        }
        else {
            throw new UnsupportedOperationException(); // TODO
        }
    }
    
    @Override
    public void clear() {
        size = tailSize = rootShift = 0;
        root = null;
        tail = INITIAL_TAIL;
    }
    
    @Override
    public E remove(int index) {
        Objects.checkIndex(index, size);
        modCount++;
        
        if (index == size-1) {
            return popFromTail();
        }
        else {
            throw new UnsupportedOperationException(); // TODO
        }
    }
    
    @Override
    public ForkJoinList<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }
    
    private E popFromTail() {
        assert size > 0;
        
        int theSize = size;
        int theTailSize = tailSize;
        Node theTail = tail;
        @SuppressWarnings("unchecked")
        E old = (E) theTail.children[theTailSize-1];
        
        if (theSize == 1 && !theTail.parent.refersTo(this)) {
            tail = INITIAL_TAIL;
            tailSize = size = 0;
            return old;
        }
        
        size--;
        if (theTailSize > 1 || theSize == 1) {
            (tail = theTail.ensureEditableWithLen(this, SPAN)).children[--tailSize] = null;
            return old;
        }
        
        // oldTailSize == 1 && oldSize > 1  -->  promote new tail
        int height = rootShift/SHIFT;
        if (height == 0) {
            tailSize = (tail = root).children.length;
            root = null;
            return old;
        }
        
        Node[] path = new Node[height+1];
        path[0] = root;
        for (int i = 0; i < height; i++) {
            path[i+1] = (Node) path[i].children[path[i].children.length-1];
        }
        (theTail = tail = path[height]).tryTransferOwnership(path[height-1], this);
        theTailSize = tailSize = theTail.children.length;
        path[height] = null;
        
        for (int i = height; i-- > 0; ) {
            Node curr = path[i];
            if (path[i+1] == null && curr.children.length == 1) {
                path[i] = null;
            }
            else if (i == 0 && path[i+1] == null && curr.children.length == 2) {
                path[i] = (Node) curr.children[0];
                rootShift -= SHIFT;
            }
            else {
                Object expectedParent = i == 0 ? this : path[i-1];
                int len = curr.children.length;
                if (path[i+1] == null) {
                    path[i] = curr.ensureEditableWithLen(expectedParent, --len);
                }
                else {
                    (path[i] = curr.ensureEditableWithLen(expectedParent, len)).children[len-1] = path[i+1];
                    path[i+1].tryTransferOwnership(curr, path[i]);
                }
                if (path[i] instanceof SizedNode sn) {
                    sn.sizeTable = sn.sizeTable.ensureEditableWithLen(expectedParent, len);
                    sn.sizeTable.sizes[len-1] -= theTailSize;
                }
            }
        }
        
        root = path[0];
        return old;
    }
    
    private void addToTail(E e) {
        int oldTailSize = tailSize;
        Node oldTail = tail;
        
        if (oldTailSize < SPAN) {
            (tail = oldTail.ensureEditableWithLen(this, SPAN)).children[tailSize++] = e;
            size++;
            return;
        }
        
        (tail = new Node(new WeakReference<>(this), new Object[SPAN])).children[0] = e;
        tailSize = 1;
        size++;
        
        Node oldRoot = root;
        if (oldRoot == null) {
            root = oldTail;
            return;
        }
        
        int nodesToMutate = 0;
        int nodesVisited = 0;
        int shift = rootShift;
        
        // TODO: Simplify whatever is going on here
        countNodesToMutate: {
            Node curr = oldRoot;
            for (int index = size-2; shift > SHIFT; shift -= SHIFT) {
                int childIdx;
                if (curr instanceof SizedNode sn) {
                    childIdx = sn.children.length-1;
                    if (childIdx > 0) {
                        index -= sn.sizeTable.sizes[childIdx-1];
                    }
                }
                else {
                    int prevShift = shift + SHIFT;
                    if ((index >>> prevShift) > 0) {
                        nodesVisited++;
                        break countNodesToMutate;
                    }
                    childIdx = (index >>> shift) & MASK;
                    index &= ~(MASK << shift);
                }
                
                nodesVisited++;
                if (childIdx < MASK) {
                    nodesToMutate = nodesVisited;
                }
                
                if (childIdx == curr.children.length) {
                    nodesToMutate = nodesVisited;
                    break countNodesToMutate;
                }
                
                curr = (Node) curr.children[childIdx];
            }
            
            if (shift > 0) {
                nodesVisited++;
                if (curr.children.length < SPAN) {
                    nodesToMutate = nodesVisited;
                }
            }
        }
        
        for (; shift > SHIFT; shift -= SHIFT) {
            nodesVisited++;
        }
        
        if (nodesToMutate == 0) {
            Node newRoot;
            if (oldRoot instanceof SizedNode) {
                SizeTable tab = new SizeTable(new WeakReference<>(this), new int[]{ size-SPAN-1, size-1 });
                newRoot = new SizedNode(new WeakReference<>(this), new Object[2], tab);
            }
            else {
                newRoot = new Node(new WeakReference<>(this), new Object[2]);
            }
            newRoot.children[0] = oldRoot;
            pushDownTailThroughNewPath(newRoot, 1, oldTail, nodesVisited);
            oldRoot.tryTransferOwnership(this, newRoot);
            root = newRoot;
            rootShift += SHIFT;
        }
        else {
            pushDownTailThroughExistingPath(oldTail, nodesToMutate, nodesVisited - nodesToMutate);
        }
    }
    
    private void pushDownTailThroughExistingPath(Node oldTail, int prefixHeight, int suffixHeight) {
        Node node = root;
        Object parent = this;
        int childIdx = -1;
        int index = size-2;
        
        for (int i = 1, shift = rootShift; i <= prefixHeight; i++, shift -= SHIFT) {
            node = node.ensureEditableWithLen(parent, node.children.length + (i == prefixHeight ? 1 : 0));
            parent = setChild(parent, childIdx, node);
            if (shift == 0) {
                break;
            }
            if (node instanceof SizedNode sn) {
                SizeTable tab = sn.sizeTable = sn.sizeTable.ensureEditableWithLen(parent, node.children.length);
                int[] sizes = tab.sizes;
                int offset = i == prefixHeight ? 2 : 1;
                sizes[sizes.length-1] = sizes[sizes.length - offset] + SPAN;
                childIdx = sizes.length-1;
                if (childIdx > 0) {
                    index -= sizes[childIdx-1];
                }
            }
            else {
                childIdx = (index >>> shift) & MASK;
            }
            node = (Node) node.children[childIdx];
        }
        
        pushDownTailThroughNewPath(parent, childIdx, oldTail, suffixHeight);
    }
    
    private void pushDownTailThroughNewPath(Object parent, int childIdx, Node oldTail, int height) {
        for (int i = 0; i < height; i++) {
            Node curr = new Node(new WeakReference<>(parent), new Object[1]);
            parent = setChild(parent, childIdx, curr);
            childIdx = 0;
        }
        setChild(parent, childIdx, oldTail);
        oldTail.tryTransferOwnership(this, parent);
    }
    
    private Node setChild(Object parent, int childIdx, Node child) {
        if (parent instanceof Node p) {
            p.children[childIdx] = child;
        }
        else {
            root = child;
        }
        return child;
    }
    
    @Override
    public int size() {
        return size;
    }
    
    private void transferOwnership(Object to) {
        tail.tryTransferOwnership(this, to);
        root.tryTransferOwnership(this, to);
    }
    
    @Override
    public ForkJoinList<E> fork() {
        // Disown any owned top-level objects, to force path-copying upon future mutations
        transferOwnership(null);
        return new TrieForkJoinList<>(this);
    }
    
    @Override
    public void join(Collection<? extends E> other) {
        // TODO
        throw new UnsupportedOperationException();
    }
    
    @Override
    public ForkJoinList<E> splice() {
        TrieForkJoinList<E> copy = new TrieForkJoinList<>(this);
        transferOwnership(copy);
        size = tailSize = rootShift = 0;
        root = null;
        tail = INITIAL_TAIL;
        return copy;
    }
    
    @Override
    public ForkJoinList<E> splice(Collection<? extends E> replacement) {
        ForkJoinList<E> copy = splice();
        join(replacement);
        return copy;
    }
    
    private int tailOffset() {
        return size - tailSize;
    }
    
    private void rangeCheckForAdd(int index) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }
    
    private String outOfBoundsMsg(int index) {
        return "Index: "+index+", Size: "+size;
    }
    
    
    // Extra overhead so far:
    //  - Node has extra 16-byte pointer to children array
    //  - SizeTable has extra 16-byte pointer to sizes array
    //  - sizes array (and children array) do not use minimum-width types
    //  - sizes array redundantly tracks 4-byte length (nothing we can do)
    //  - WeakReference is an extra 16-byte pointer to a pointer, for Node and SizeTable (nothing we can do)
    
    private static class Node {
        WeakReference<Object> parent;
        Object[] children;
        
        Node(WeakReference<Object> parent, Object[] children) {
            this.parent = parent;
            this.children = children;
        }
        
        void tryTransferOwnership(Object from, Object to) {
            if (parent.refersTo(from) && from != to) {
                parent = new WeakReference<>(to);
            }
        }
        
        Node ensureEditable(Object expectedParent) {
            return ensureEditableWithLen(expectedParent, children.length);
        }
        
        Node ensureEditableWithLen(Object expectedParent, int len) {
            if (parent.refersTo(expectedParent)) {
                if (children.length != len) {
                    children = Arrays.copyOf(children, len);
                }
                return this;
            }
            return new Node(new WeakReference<>(expectedParent), Arrays.copyOf(children, len));
        }
    }
    
    private static class SizedNode extends Node {
        SizeTable sizeTable;
        
        SizedNode(WeakReference<Object> parent, Object[] children, SizeTable sizeTable) {
            super(parent, children);
            this.sizeTable = sizeTable;
        }
        
        // TODO: tryTransferOwnership also deals in sizeTable, but ensureEditable does not?
        
        @Override
        void tryTransferOwnership(Object from, Object to) {
            super.tryTransferOwnership(from, to);
            sizeTable.tryTransferOwnership(from, to);
        }
        
        @Override
        SizedNode ensureEditable(Object expectedParent) {
            return ensureEditableWithLen(expectedParent, children.length);
        }
        
        @Override
        SizedNode ensureEditableWithLen(Object expectedParent, int len) {
            if (parent.refersTo(expectedParent)) {
                if (children.length != len) {
                    children = Arrays.copyOf(children, len);
                }
                return this;
            }
            return new SizedNode(new WeakReference<>(expectedParent), Arrays.copyOf(children, len), sizeTable);
        }
    }
    
    private static class SizeTable {
        WeakReference<Object> parent;
        int[] sizes;
        
        SizeTable(WeakReference<Object> parent, int[] sizes) {
            this.parent = parent;
            this.sizes = sizes;
        }
        
        void tryTransferOwnership(Object from, Object to) {
            if (parent.refersTo(from)) {
                parent = new WeakReference<>(to);
            }
        }
        
        SizeTable ensureEditable(Object expectedParent) {
            return ensureEditableWithLen(expectedParent, sizes.length);
        }
        
        SizeTable ensureEditableWithLen(Object expectedParent, int len) {
            if (parent.refersTo(expectedParent)) {
                if (sizes.length != len) {
                    sizes = Arrays.copyOf(sizes, len);
                }
                return this;
            }
            return new SizeTable(new WeakReference<>(expectedParent), Arrays.copyOf(sizes, len));
        }
    }
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // -> If I am not sized, none of my progeny are sized
}
