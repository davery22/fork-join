package io.avery.util;

import java.lang.ref.WeakReference;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class TrieForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E> {
    // TODO: Adjust tail parent (if owned) when lifting tail from root
    // TODO: Adjust tail parent (if owned) when pushing tail into root
    
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
        this.size = this.tailSize = this.rootShift = 0;
        this.root = null;
        this.tail = INITIAL_TAIL;
    }
    
    protected TrieForkJoinList(int size, int tailSize, int rootShift, Node root, Node tail) {
        this.size = size;
        this.tailSize = tailSize;
        this.rootShift = rootShift;
        this.root = root;
        this.tail = tail;
    }
    
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
            index -= tailOffset;
            node = tail = tail.ensureEditable(this);
        }
        else {
            Object parent = this;
            node = root;
            int childIdx = -1;
            
            for (int shift = rootShift; shift > 0; shift -= SHIFT) {
                node = node.ensureEditable(parent);
                if (parent instanceof Node p) {
                    parent = p.children[childIdx] = node;
                }
                else {
                    parent = root = node;
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
            
            node = node.ensureEditable(parent);
            if (parent instanceof Node p) {
                p.children[childIdx] = node;
            }
            else {
                root = node;
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
    
    private void addToTail(E e) {
        int oldTailSize = tailSize;
        Node oldTail = tail;
        
        if (oldTailSize < SPAN) {
            // Old tail is not full - insert element into old tail.
            (tail = oldTail.ensureEditable(this)).children[tailSize++] = e;
            size++;
            return;
        }
        
        Node oldRoot = root;
        Node newTail = tail = new Node(new WeakReference<>(this), new Object[SPAN]);
        newTail.children[0] = e;
        tailSize = 1;
        size++;
        
        if (oldRoot == null) {
            root = oldTail;
            return;
        }
        
        int index = size-2;
        int nodesToMutate = 0;
        int nodesVisited = 0;
        int shift = rootShift;
        Node curr = oldRoot;
        
        countNodesToMutate: {
            for (; shift > SHIFT; shift -= SHIFT) {
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
            newRoot.children[0] = oldRoot.ensureEditable(this);
            newRoot.children[1] = newPathToNode(newRoot, oldTail.ensureEditable(this), nodesVisited);
            oldRoot.parent = new WeakReference<>(newRoot);
            root = newRoot;
            rootShift += SHIFT;
        }
        else {
            appendNodeThroughExistingPath(oldTail.ensureEditable(this), nodesToMutate, nodesVisited - nodesToMutate);
        }
    }
    
    private void appendNodeThroughExistingPath(Node child, int prefixHeight, int suffixHeight) {
        Object parent = this;
        Node node = root;
        int index = size-2;
        int childIdx = -1;
        int i = 1;
        
        for (int shift = rootShift; i <= prefixHeight && shift > 0; i++, shift -= SHIFT) {
            node = node.ensureEditableWithLen(parent, node.children.length + (i == prefixHeight ? 1 : 0));
            if (parent instanceof Node p) {
                parent = p.children[childIdx] = node;
            }
            else {
                parent = root = node;
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
        
        if (i == prefixHeight) {
            node = node.ensureEditableWithLen(parent, node.children.length+1);
            if (parent instanceof Node p) {
                parent = p.children[childIdx] = node;
            }
            else {
                parent = root = node;
            }
        }
        
        // Finally, add the new child
        node = newPathToNode(parent, child, suffixHeight);
        if (parent instanceof Node p) {
            p.children[childIdx] = node;
        }
        else {
            root = node;
        }
    }
    
    private static Node newPathToNode(Object root, Node child, int height) {
        for (int i = 0; i < height; i++) {
            Node parent = new Node(null, new Object[]{ child });
            child.parent = new WeakReference<>(parent);
            child = parent;
        }
        child.parent = new WeakReference<>(root);
        return child;
    }
    
    @Override
    public int size() {
        return size;
    }
    
    private void transferOwnership(Object to) {
        if (tail.parent.refersTo(this)) {
            tail.parent = new WeakReference<>(to);
        }
        if (root.parent.refersTo(this)) {
            root.parent = new WeakReference<>(to);
        }
        if (root instanceof SizedNode sn && sn.sizeTable.parent.refersTo(this)) {
            sn.sizeTable.parent = new WeakReference<>(to);
        }
    }
    
    @Override
    public ForkJoinList<E> fork() {
        // Disown any owned top-level objects, to force path-copying upon future mutations
        transferOwnership(null);
        return new TrieForkJoinList<>(size, tailSize, rootShift, root, tail);
    }
    
    @Override
    public void join(Collection<? extends E> other) {
        // TODO
        throw new UnsupportedOperationException();
    }
    
    @Override
    public ForkJoinList<E> splice() {
        TrieForkJoinList<E> copy = new TrieForkJoinList<>(size, tailSize, rootShift, root, tail);
        transferOwnership(copy);
        this.size = this.tailSize = this.rootShift = 0;
        this.root = null;
        this.tail = INITIAL_TAIL;
        return copy;
    }
    
    @Override
    public ForkJoinList<E> splice(Collection<? extends E> replacement) {
        ForkJoinList<E> copy = splice();
        join(replacement);
        return copy;
    }
    
    @Override
    public ForkJoinList<E> reversed() {
        return null;
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
    
    // TODO: Make protected? Node is exposed to the protected constructor
    private static class Node {
        WeakReference<Object> parent;
        Object[] children;
        
        Node(WeakReference<Object> parent, Object[] children) {
            this.parent = parent;
            this.children = children;
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
    
//    // How many table elements in this node
//    private static int nodeSize(Object[] node) {
//        return node[node.length-1] instanceof Size s ? s.size : SPAN;
//    }
//
//    // How many leaf table elements under this node
//    // (internal nodes only - would be redundant for leaf nodes)
//    private static int trieSize(Object[] node) {
//        return switch (node[SIZE_OFF]) {
//            case Size s -> s.size;
//            case byte[] b -> Byte.toUnsignedInt(b[b.length-1]);
//            case short[] s -> Short.toUnsignedInt(s[s.length-1]);
//            case int[] i -> i[i.length-1];
//            default -> throw new AssertionError();
//        };
//    }
//
//    @SuppressWarnings("unchecked")
//    private static Object nodeParent(Object[] node) {
//        return ((WeakReference<Object>) node[REF_OFF]).get();
//    }
}
