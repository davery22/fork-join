package io.avery.util;

import java.util.*;

// TODO: Implement RandomAccess?
// TODO: can rootShift get too big? (ie shift off the entire index -> 0, making later elements unreachable)
//  - I think the answer is "yes, but we'd still find later elements by linear searching a size table"
// TODO: Shifts (right and left) can wrap! eg: (int >>> 35) == (int >>> 3);  (int << 35) == (int << 3)
//  - Force result to 0 when shift is too big
// TODO: Throw OOME when index size limit exceeded

public class TrieForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E> {
    /* Implements a variant of Relaxed Radix Balanced (RRB) Tree, a data structure proposed by Bagwell & Rompf
     * [https://infoscience.epfl.ch/server/api/core/bitstreams/e5d662ea-1e8d-4dda-b917-8cbb8bb40bf9/content]
     * and further elaborated by L'orange [https://hypirion.com/thesis.pdf].
     *
     * Notable features of this variant:
     *  1. Rather than forcing update operations to return a new instance (copy + mutate, as in "persistent" data
     *     structures), this variant separates a copy operation (fork) from mutative operations, allowing it to
     *     adhere to the java.util.List interface. The copy operation is O(1) (when copying a full list), or O(logN)
     *     (when copying a sublist, aka 'slicing' in the literature).
     *  2. Rather than having each node reference a list id to determine if it is owned or shared (as in L'orange's
     *     transient variant), this variant has each node keep a bitset tracking which of its children are owned or
     *     shared. A node is owned if its parent is owned and its bit is set in its parent's bitset. This finer-grained
     *     ownership tracking allows a list to share nodes with a sublist fork while retaining ownership of nodes
     *     outside the sublist fork. Tracking ownership on the parent instead of on each child reduces data fetches, and
     *     allows for bitwise operations to bulk-update ownership when many children are moved, as during concatenation.
     *     For a branching factor of 32, each bitset takes up the same space as a 4-byte pointer to a list id would.
     *  3. This variant compresses size tables at lower levels in the tree, taking advantage of the lower maximum
     *     cumulative size to use narrower primitive types (e.g. byte or char instead of int).
     *  4. This variant attempts to avoid or even eliminate existing size tables during operations like concatenation.
     *  5. This variant does not use a planning phase during concatenation.
     *  6. This variant typically mutates nodes in-place when they are owned, rather than replacing with new nodes.
     */
    
    // If changed, ParentNode.owns (and operations on it) must be updated to use a type of the appropriate width
    //  - eg: 4 => short, 5 => int, 6 => long
    // SHIFT > 6 is possible, but requires more work to switch to long[] owns, and long[] deathRows in rebalance()
    private static final int SHIFT = 4;
    private static final int SPAN = 1 << SHIFT;
    private static final int MASK = SPAN-1;
    private static final int MARGIN = 2;
    private static final int DO_NOT_REDISTRIBUTE = SPAN - MARGIN/2; // During rebalance(), a node with size >= threshold is not redistributed
    private static final Node INITIAL_TAIL = new Node(new Object[SPAN]);
    
    private Node root;
    private Node tail;
    private int size;
    private int tailSize;
    private int rootShift;
    private byte owns; // only need 2 bits - for root and tail
                       // TODO: Maybe store this in the rootShift? Or make rootShift narrower
    
    private boolean ownsTail() {
        return (owns & 1) != 0;
    }
    
    private boolean ownsRoot() {
        return (owns & 2) != 0;
    }
    
    private boolean claimTail() {
        return owns == (owns |= 1);
    }
    
    private boolean claimRoot() {
        return owns == (owns |= 2);
    }
    
    private void disownTail() {
        owns &= ~1;
    }
    
    private void disownRoot() {
        owns &= ~2;
    }
    
    private void claimOrDisownTail(boolean claim) {
        if (claim) {
            owns |= 1;
        }
        else {
            owns &= ~1;
        }
    }
    
    private void claimOrDisownRoot(boolean claim) {
        if (claim) {
            owns |= 2;
        }
        else {
            owns &= ~2;
        }
    }
    
    private Node getEditableTail() {
        return tail = tail.copy(claimTail(), SPAN);
    }
    
    private Node getEditableRoot() {
        return root = root.copy(claimRoot());
    }
    
    private Node getEditableRoot(int len) {
        return root = root.copy(claimRoot(), len);
    }
    
    private Node getSizedEditableRoot(int shift) {
        return root = root.copySized(claimRoot(), shift);
    }
    
    private Node getSizedEditableRoot(int len, int shift) {
        return root = root.copySized(claimRoot(), len, shift);
    }
    
    public TrieForkJoinList() {
        tail = INITIAL_TAIL;
    }
    
    public TrieForkJoinList(Collection<? extends E> c) {
        this();
        addAll(c); // TODO: Avoid copying - we know we are not aliased
    }
    
    protected TrieForkJoinList(TrieForkJoinList<? extends E> toCopy) {
        size = toCopy.size;
        tailSize = toCopy.tailSize;
        rootShift = toCopy.rootShift;
        root = toCopy.root;
        tail = toCopy.tail;
    }
    
    // add(index, element) - specialized self-concat, unless after tailOffset
    // addAll(collection) - Use collection.toArray()
    // addAll(index, collection) - Use collection.toArray(), or join(index, new [owned] TrieForkJoinList<>(collection)), unless after tailOffset
    // remove(index) - removeRange, unless after tailOffset
    // removeAll(collection) - removeRange
    // retainAll(collection) - removeRange
    // removeIf(predicate) - bitset + listIterator + removeRange
    // toArray() - AbstractCollection, but we can do better(?)
    // toArray(arr) - AbstractCollection, but we can do better(?)
    // toArray(gen) - Collection, but we can do better(?)
    // iterator()
    // listIterator()
    // listIterator(index)
    // subList(from, to)
    // spliterator()
    // join(index, collection)
    
    // TODO: Make sure we handle modCount
    // -get(index)
    // -set(index, element)
    // -add(element)
    // -clear()
    // -reversed()
    // -size()
    // -fork()
    // -join(collection)
    
    // TODO: Some of these might need revisited just to handle modCount - see ArrayList
    //  - not to mention 'range' variants
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
    // -replaceAll(unaryOp) - List
    // -sort(comparator) - List
    // -toString() - AbstractCollection
    // -forEach(action) - Iterable
    // -stream() - Collection
    // -parallelStream() - Collection
    
    
    // slice_left[_rec]
    // slice_right[_rec]
    
    @Override
    public E get(int index) {
        Objects.checkIndex(index, size);
        int tailOffset = tailOffset();
        Node leaf;
        
        if (index >= tailOffset) {
            leaf = tail;
            index -= tailOffset;
        }
        else {
            leaf = root;
            for (int shift = rootShift; shift > 0; shift -= SHIFT) {
                int childIdx = index >>> shift;
                if (leaf instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                leaf = (Node) leaf.children[childIdx & MASK];
            }
            index &= MASK;
        }
        
        @SuppressWarnings("unchecked")
        E value = (E) leaf.children[index];
        return value;
    }
    
    @Override
    public E set(int index, E element) {
        Objects.checkIndex(index, size);
        modCount++;
        int tailOffset = tailOffset();
        Node leaf;
        
        if (index >= tailOffset) {
            leaf = getEditableTail();
            index -= tailOffset;
        }
        else {
            leaf = getEditableRoot();
            for (int shift = rootShift; shift > 0; shift -= SHIFT) {
                int childIdx = index >>> shift;
                if (leaf instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                leaf = leaf.getEditableChild(childIdx & MASK);
            }
            index &= MASK;
        }
        
        @SuppressWarnings("unchecked")
        E old = (E) leaf.children[index];
        leaf.children[index] = element;
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
        int tailOffset = tailOffset();
        
        if (index >= tailOffset) {
            if (index == size) {
                addToTail(element);
                return;
            }
            int tailIdx = index - tailOffset;
            Node oldTail = getEditableTail();
            if (tailSize < SPAN) {
                System.arraycopy(oldTail.children, tailIdx, oldTail.children, tailIdx+1, tailSize-tailIdx);
                oldTail.children[tailIdx] = element;
                tailSize++;
                size++;
            }
            else {
                Object lastElement = oldTail.children[SPAN-1];
                System.arraycopy(oldTail.children, tailIdx, oldTail.children, tailIdx+1, SPAN-tailIdx-1);
                oldTail.children[tailIdx] = element;
                addToTail(lastElement); // TODO: Rechecks tailSize < SPAN
            }
        }
        else {
            // TODO: Add to root
            throw new UnsupportedOperationException();
        }
    }
    
    @Override
    public void clear() {
        size = tailSize = rootShift = owns = 0;
        root = null;
        tail = INITIAL_TAIL;
    }
    
    @Override
    public E remove(int index) {
        Objects.checkIndex(index, size);
        modCount++;
        int tailOffset = tailOffset();
        
        if (index >= tailOffset) {
            if (index == size-1) {
                return popFromTail();
            }
            // We know that we are in the tail, but not in the last position.
            // So tailSize must be >= 2, and we will not need to promote a new tail.
            int tailIdx = index - tailOffset;
            Node oldTail = getEditableTail();
            @SuppressWarnings("unchecked")
            E old = (E) oldTail.children[tailIdx];
            System.arraycopy(oldTail.children, tailIdx+1, oldTail.children, tailIdx, tailSize-tailIdx-1);
            tailSize--;
            size--;
            return old;
        }
        else {
            // TODO: Remove from root
            throw new UnsupportedOperationException();
        }
    }
    
    @Override
    public ForkJoinList<E> subList(int fromIndex, int toIndex) {
        subListRangeCheck(fromIndex, toIndex, size);
        return new SubList<>(this, fromIndex, toIndex);
    }
    
    static void subListRangeCheck(int fromIndex, int toIndex, int size) {
        if (fromIndex < 0)
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        if (toIndex > size)
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        if (fromIndex > toIndex)
            throw new IllegalArgumentException("fromIndex(" + fromIndex +
                                                   ") > toIndex(" + toIndex + ")");
    }
    
    private E popFromTail() {
        assert size > 0;
        
        Node oldTail = tail;
        int oldTailSize = tailSize;
        @SuppressWarnings("unchecked")
        E old = (E) oldTail.children[oldTailSize-1];
        
        if (oldTailSize > 1) {
            getEditableTail().children[--tailSize] = null;
            size--;
        }
        else if (size == 1) {
            if (!ownsTail()) {
                tail = INITIAL_TAIL;
            }
            else {
                tail.children[0] = null;
            }
            tailSize = size = 0;
        }
        else {
            pullUpTail();
            size--;
        }
        
        return old;
    }
    
    // Updates tail, tailSize
    // May update root, rootShift
    private void pullUpTail() {
        int oldRootShift = rootShift;
        if (oldRootShift == 0) {
            claimOrDisownTail(ownsRoot());
            tailSize = (tail = root).children.length;
            root = null;
            return;
        }
        
        // Two top-down passes:
        //  1. Find the deepest ancestor that will be non-empty after tail pull-up (and pull-up the new tail)
        //  2. Truncate that deepest ancestor (which requires claiming it, and thus the path to it)
        
        // First pass
        ParentNode oldRoot = (ParentNode) root, node = oldRoot;
        boolean owned = ownsRoot();
        int deepestNonEmptyAncestorShift = oldRootShift;
        for (int shift = oldRootShift; ; shift -= SHIFT) {
            int lastIdx = node.children.length-1;
            owned = owned && node.owns(lastIdx);
            if (shift < oldRootShift && lastIdx > 0) { // root (shift == oldRootShift) is handled specially later
                deepestNonEmptyAncestorShift = shift;
            }
            if (shift == SHIFT) {
                // Promote last child to tail
                claimOrDisownTail(owned);
                tailSize = (tail = (Node) node.children[lastIdx]).children.length;
                break;
            }
            node = (ParentNode) node.children[lastIdx];
        }
        
        // Second pass
        if (deepestNonEmptyAncestorShift == oldRootShift) {
            int len = oldRoot.children.length;
            if (len > 2) {
                getEditableRoot(len-1);
            }
            else if (len == 2) {
                // Replace root with its first child
                root = (Node) oldRoot.children[0];
                rootShift -= SHIFT;
                if (!oldRoot.owns(0)) {
                    disownRoot();
                }
            }
            else { // len == 1
                root = null;
                rootShift = 0;
            }
        }
        else {
            // Claim nodes up to the deepest non-empty ancestor
            node = (ParentNode) getEditableRoot();
            int lastIdx = node.children.length-1;
            int droppedSize = -tailSize;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedParentNode sn) {
                    sn.sizes().inc(lastIdx, droppedSize);
                }
                int childLastIdx = ((Node) node.children[lastIdx]).children.length-1;
                if (shift == deepestNonEmptyAncestorShift + SHIFT) {
                    node.getEditableChild(lastIdx, childLastIdx);
                    break;
                }
                node = (ParentNode) node.getEditableChild(lastIdx);
                lastIdx = childLastIdx;
            }
        }
    }
    
    private void addToTail(Object e) {
        if (tailSize < SPAN) {
            // Could check for INITIAL_TAIL here to skip copying, but that slows down the common case
            getEditableTail().children[tailSize++] = e;
            size++;
            return;
        }
        
        pushDownTail(false);
        claimTail();
        (tail = new Node(new Object[SPAN])).children[0] = e;
        tailSize = 1;
        size++;
    }
    
    // Reads root, rootShift, size, tailSize, tail
    // May update root, rootShift
    private void pushDownTail(boolean fixupSizes) {
        // For appends, fixupSizes = false, ie we allow nodes with not-full children to stay not-Sized.
        // For joins, fixupSizes = true, ie when we push down the left tail we make nodes with not-full children Sized.
        
        int oldTailSize = tailSize;
        boolean nonFullTail = oldTailSize < SPAN;
        
        if (root == null) {
            if (nonFullTail) {
                Sizes sizes = Sizes.of(rootShift = SHIFT, 1);
                sizes.set(0, oldTailSize);
                claimRoot();
                ParentNode newRoot = new SizedParentNode(new Object[]{ tail }, sizes);
                newRoot.claimOrDisown(0, ownsTail());
                root = newRoot;
            }
            else {
                claimOrDisownRoot(ownsTail());
                root = tail;
            }
            return;
        }
        
        // Two top-down passes:
        //  1. Find the deepest ancestor that will be non-full after tail push-down
        //  2. Extend that deepest ancestor with a path to the tail (which requires claiming it, and thus the path to it)
        
        // First pass
        Node oldRoot = root, node = oldRoot;
        int oldRootShift = rootShift;
        int deepestNonFullAncestorShift = oldRootShift + SHIFT;
        for (int shift = oldRootShift; shift > 0; shift -= SHIFT) {
            int len = node.children.length;
            if (len < SPAN) {
                deepestNonFullAncestorShift = shift;
            }
            node = (Node) node.children[len-1];
        }
        
        int deepestNonFullShift = !fixupSizes ? Integer.MAX_VALUE : nonFullTail ? 0 : deepestNonFullAncestorShift;
        
        // Second pass
        if (deepestNonFullAncestorShift > oldRootShift) {
            // No existing non-full ancestors - need to make a new root above old root
            ParentNode newRoot;
            Object[] children = new Object[]{ oldRoot, null };
            if (deepestNonFullShift < deepestNonFullAncestorShift || oldRoot instanceof SizedParentNode || size - oldTailSize < SPAN) {
                Sizes sizes = Sizes.of(deepestNonFullAncestorShift, 2);
                sizes.set(0, size - oldTailSize);
                sizes.set(1, size);
                newRoot = new SizedParentNode(children, sizes);
            }
            else {
                newRoot = new ParentNode(children);
            }
            if (claimRoot()) {
                newRoot.claim(0);
            }
            root = node = newRoot;
            rootShift = deepestNonFullAncestorShift;
        }
        else if (deepestNonFullAncestorShift == oldRootShift) {
            // Claim root - it is the deepest non-full ancestor
            int len = oldRoot.children.length;
            node = getSizedEditableRoot(len+1, deepestNonFullShift < oldRootShift ? oldRootShift : 0);
            if (node instanceof SizedParentNode sn) {
                Sizes sizes = sn.sizes();
                sizes.set(len, sizes.get(len-1) + oldTailSize);
            }
        }
        else {
            // Claim nodes up to the deepest non-full ancestor
            node = getSizedEditableRoot(deepestNonFullShift < oldRootShift ? oldRootShift : 0);
            int lastIdx = node.children.length-1;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedParentNode sn) {
                    sn.sizes().inc(lastIdx, oldTailSize);
                }
                int childLen = ((Node) node.children[lastIdx]).children.length;
                int sizedShift = deepestNonFullShift < shift-SHIFT ? shift-SHIFT : 0;
                if (shift == deepestNonFullAncestorShift + SHIFT) {
                    node = node.getSizedEditableChild(lastIdx, childLen+1, sizedShift);
                    if (node instanceof SizedParentNode sn) {
                        Sizes sizes = sn.sizes();
                        sizes.set(childLen, sizes.get(childLen-1) + oldTailSize);
                    }
                    break;
                }
                node = node.getSizedEditableChild(lastIdx, sizedShift);
                lastIdx = childLen-1;
            }
        }
        
        // Finish second pass: Extend a path (of the appropriate height) from the deepest non-full ancestor to tail
        ParentNode parent = (ParentNode) node;
        int lastIdx = parent.children.length-1;
        for (int shift = deepestNonFullAncestorShift; shift > SHIFT; shift -= SHIFT) {
            parent.claim(lastIdx);
            if (deepestNonFullShift < shift-SHIFT) {
                Sizes sizes = Sizes.of(shift-SHIFT, 1);
                sizes.set(0, oldTailSize);
                parent.children[lastIdx] = parent = new SizedParentNode(new Object[1], sizes);
            }
            else {
                parent.children[lastIdx] = parent = new ParentNode(new Object[1]);
            }
            lastIdx = 0;
        }
        parent.claimOrDisown(lastIdx, ownsTail());
        parent.children[lastIdx] = tail;
    }
    
    @Override
    public int size() {
        return size;
    }
    
    @Override
    public ForkJoinList<E> fork() {
        // Disown root/tail, to force path-copying upon future mutations
        owns = 0;
        return new TrieForkJoinList<>(this);
    }
    
    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        // In-place dual of forkRange()
        
        // TODO: Handle non-full leaf root before returning?
        
        if (fromIndex == 0) {
            removePrefix(toIndex);
            return;
        }
        if (toIndex == size) {
            removeSuffix(fromIndex);
            return;
        }
        
        int tailOffset = tailOffset(), rangeSize = toIndex - fromIndex;
        size -= rangeSize;
        if (toIndex >= tailOffset) {
            if (fromIndex >= tailOffset) {
                // Starts and ends in the tail
                Node newTail = getEditableTail();
                int oldTailSize = tailSize, tailIdx = toIndex - tailOffset;
                int newTailSize = tailSize = oldTailSize - rangeSize;
                System.arraycopy(newTail.children, tailIdx, newTail.children, fromIndex - tailOffset, oldTailSize - tailIdx);
                Arrays.fill(newTail.children, newTailSize, oldTailSize, null);
                return;
            }
            
            root = forkPrefixRec(fromIndex, this, getEditableRoot(), rootShift, true, true);
            if (toIndex != tailOffset) {
                // Starts in the root, ends in the tail
                Node newTail = getEditableTail();
                int oldTailSize = tailSize, tailIdx = toIndex - tailOffset;
                int newTailSize = tailSize = oldTailSize - tailIdx;
                System.arraycopy(newTail.children, tailIdx, newTail.children, 0, newTailSize);
                Arrays.fill(newTail.children, newTailSize, oldTailSize, null);
            }
            return;
        }
        
        // Starts and ends in the root
        root = removeRangeRec(fromIndex, toIndex-1, getEditableRoot(), true, rootShift);
    }
    
    private void removeSuffix(int fromIndex) {
        // In-place version of forkPrefix()
        
        if (fromIndex == 0) {
            clear();
            return;
        }
        if (fromIndex == size) {
            return;
        }
        
        int tailOffset = tailOffset();
        size = fromIndex;
        if (fromIndex >= tailOffset) {
            if (fromIndex == tailOffset) {
                pullUpTail();
                return;
            }
            int oldTailSize = tailSize;
            int newTailSize = tailSize = fromIndex - tailOffset;
            Node newTail = getEditableTail();
            Arrays.fill(newTail.children, newTailSize, oldTailSize, null);
            return;
        }
        
        root = forkPrefixRec(fromIndex-1, this, getEditableRoot(), rootShift, true, true);
        pullUpTail();
    }
    
    private void removePrefix(int toIndex) {
        // In-place version of forkSuffix()
        
        if (toIndex == 0) {
            return;
        }
        if (toIndex == size) {
            clear();
            return;
        }
        
        int newSize = size -= toIndex;
        if (newSize <= tailSize) {
            int oldTailSize = tailSize;
            tailSize = newSize;
            Node newTail = getEditableTail();
            System.arraycopy(newTail.children, oldTailSize - newSize, newTail.children, 0, newSize);
            Arrays.fill(newTail.children, newSize, oldTailSize, null);
            return;
        }
        
        root = forkSuffixRec(toIndex, this, getEditableRoot(), rootShift, true, true);
    }
    
    private Node removeRangeRec(int fromIndex, int toIndex, Node node, boolean isRightmost, int shift) {
        int fromChildIdx = (fromIndex >>> shift) & MASK;
        int toChildIdx = (toIndex >>> shift) & MASK;
        if (shift == 0) {
            return node.removeRange(true, fromChildIdx, toChildIdx+1);
        }
        if (node instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(fromChildIdx) <= fromIndex) {
                fromChildIdx++;
            }
            if (fromChildIdx > toChildIdx) {
                toChildIdx = fromChildIdx;
            }
            while (oldSizes.get(toChildIdx) <= toIndex) {
                toChildIdx++;
            }
            if (fromChildIdx != 0) {
                fromIndex -= oldSizes.get(fromChildIdx-1);
                toIndex -= oldSizes.get(toChildIdx-1);
            }
            else if (toChildIdx != 0) {
                toIndex -= oldSizes.get(toChildIdx-1);
            }
        }
        ParentNode parent = (ParentNode) node;
        int childShift = shift - SHIFT, len = node.children.length;
        boolean isChildRightmost = isRightmost && toChildIdx == len-1;
        if (fromChildIdx == toChildIdx) {
            Node child = node.getEditableChild(fromChildIdx);
            int oldChildLen = child.children.length;
            parent.children[fromChildIdx] = child = removeRangeRec(fromIndex, toIndex, child, isChildRightmost, childShift);
            if (child == EMPTY_NODE) {
                if (len == 1) {
                    // Only-child - Parent becomes empty
                    return EMPTY_NODE;
                }
                // Shift to remove, refresh sizes
                // Removing a child cannot increase parent's excess, so no rebalancing needed
                // TODO: Implement a faster shift + refresh for the special case of one child removed
                parent.shiftChildren(1L << fromChildIdx, 0, 0, null, 0);
                return parent.refreshSizes(isRightmost, shift);
            }
            if (child.children.length != oldChildLen) {
                // Grandchildren count changed - May need to rebalance
                return rebalance1(parent, shift, isRightmost);
            }
            return parent;
        }
        Node[] children = new Node[]{ node.getEditableChild(fromChildIdx), node.getEditableChild(toChildIdx) };
        removeRangeRec2(fromIndex, toIndex, children, isChildRightmost, childShift);
        
        node.children[fromChildIdx] = children[0];
        node.children[toChildIdx] = children[1];
        return rebalance12(parent, fromChildIdx, toChildIdx, shift, isRightmost);
    }
    
    private void removeRangeRec2(int fromIndex, int toIndex, Node[] children, boolean isRightmost, int shift) {
        Node left = children[0], right = children[1];
        int fromChildIdx = (fromIndex >>> shift) & MASK;
        int toChildIdx = (toIndex >>> shift) & MASK;
        if (shift == 0) {
            int remainingRight = right.children.length - toChildIdx - 1;
            int totalSize = fromChildIdx + remainingRight;
            if (totalSize <= SPAN) {
                // Empty right into left if possible
                if (totalSize == 0) {
                    children[0] = EMPTY_NODE;
                }
                else {
                    // TODO: Actually doing extra work to shift left's children right, just so we can reuse an existing copy method.
                    int skip = left.children.length - fromChildIdx;
                    System.arraycopy(left.children, 0, left.children, skip, fromChildIdx);
                    left.copy(true, skip, fromChildIdx, remainingRight, right, true, true, 0);
                }
                children[1] = EMPTY_NODE;
            }
            else { // totalSize > SPAN. Since both parts are < SPAN, both must be non-zero, so no empty nodes
                left.copy(true, fromChildIdx);
                right.copySuffix(true, toChildIdx + 1);
            }
            return;
        }
        if (left instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(fromChildIdx) <= fromIndex) {
                fromChildIdx++;
            }
            if (fromChildIdx != 0) {
                fromIndex -= oldSizes.get(fromChildIdx-1);
            }
        }
        if (right instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(toChildIdx) <= toChildIdx) {
                toChildIdx++;
            }
            if (toChildIdx != 0) {
                toIndex -= oldSizes.get(toChildIdx-1);
            }
        }
        boolean isChildRightmost = isRightmost && toChildIdx == right.children.length-1;
        int childShift = shift - SHIFT;
        children[0] = left.getEditableChild(fromChildIdx);
        children[1] = right.getEditableChild(toChildIdx);
        removeRangeRec2(fromIndex, toIndex, children, isChildRightmost, childShift);
        
        left.children[fromChildIdx] = children[0];
        right.children[toChildIdx] = children[1];
        children[0] = left;
        children[1] = right;
        rebalance22(children, fromChildIdx, toChildIdx, shift, isRightmost);
        // TODO: Children may be empty (which will at least force a shift, or may empty us)
        //  Or maybe their children counts changed, prompting us to rebalance
        // TODO: Remove suffix/prefix, refresh rightmost/leftmost child, empty right into left if possible
        // TODO: Restore initial left/right
    }
    
    private TrieForkJoinList<E> forkRange(int fromIndex, int toIndex) {
        
        // TODO: Handle non-full leaf root before returning?
        
        if (fromIndex == 0) {
            return forkPrefix(toIndex);
        }
        if (toIndex == size) {
            return forkSuffix(fromIndex);
        }
        
        TrieForkJoinList<E> newList = new TrieForkJoinList<>();
        int newSize = newList.size = toIndex - fromIndex;
        int tailOffset = tailOffset();
        if (toIndex >= tailOffset) {
            if (fromIndex >= tailOffset) {
                // Starts and ends in the tail
                newList.claimTail();
                Object[] newTailChildren = new Object[SPAN];
                System.arraycopy(tail.children, fromIndex - tailOffset, newTailChildren, 0, newSize);
                newList.tail = new Node(newTailChildren);
                newList.tailSize = newSize;
                return newList;
            }
            
            newList.root = forkSuffixRec(fromIndex, newList, root, rootShift, true, false);
            if (toIndex == tailOffset) {
                // Starts in the root, ends at the tail
                newList.pullUpTail();
            }
            else {
                // Starts in the root, ends in the tail
                int newTailSize = newList.tailSize = toIndex - tailOffset;
                newList.claimTail();
                Object[] newTailChildren = new Object[SPAN];
                System.arraycopy(tail.children, 0, newTailChildren, 0, newTailSize);
                newList.tail = new Node(newTailChildren);
            }
            return newList;
        }
        
        // Starts and ends in the root
        newList.root = forkRangeRec(fromIndex, toIndex-1, newList, root, rootShift);
        newList.pullUpTail();
        return newList;
    }
    
    private TrieForkJoinList<E> forkPrefix(int toIndex) {
        if (toIndex == 0) {
            return new TrieForkJoinList<>();
        }
        if (toIndex == size) {
            owns = 0;
            return new TrieForkJoinList<>(this);
        }
        
        TrieForkJoinList<E> newList = new TrieForkJoinList<>();
        int tailOffset = tailOffset();
        newList.size = toIndex;
        if (toIndex >= tailOffset) {
            disownRoot();
            newList.rootShift = rootShift;
            newList.root = root;
            if (toIndex == tailOffset) {
                newList.pullUpTail();
                return newList;
            }
            int newTailSize = newList.tailSize = toIndex - tailOffset;
            newList.claimTail();
            Object[] newTailChildren = new Object[SPAN];
            System.arraycopy(tail.children, 0, newTailChildren, 0, newTailSize);
            newList.tail = new Node(newTailChildren);
            return newList;
        }
        
        newList.root = forkPrefixRec(toIndex-1, newList, root, rootShift, true, false);
        newList.pullUpTail();
        return newList;
    }
    
    private TrieForkJoinList<E> forkSuffix(int fromIndex) {
        if (fromIndex == 0) {
            owns = 0;
            return new TrieForkJoinList<>(this);
        }
        if (fromIndex == size) {
            return new TrieForkJoinList<>();
        }
        
        TrieForkJoinList<E> newList = new TrieForkJoinList<>();
        int newSize = newList.size = size - fromIndex;
        if (newSize <= tailSize) {
            newList.tailSize = newSize;
            newList.claimTail();
            Object[] newTailChildren = new Object[SPAN];
            System.arraycopy(tail.children, tailSize - newSize, newTailChildren, 0, newSize);
            newList.tail = new Node(newTailChildren);
            return newList;
        }
        
        disownTail();
        newList.tailSize = tailSize;
        newList.tail = tail;
        newList.root = forkSuffixRec(fromIndex, newList, root, rootShift, true, false);
        return newList;
    }
    
    private static <E> Node forkPrefixRec(int toIndex, TrieForkJoinList<E> newList, Node node, int shift, boolean isLeftmost, boolean isOwned) {
        int childIdx = (toIndex >>> shift) & MASK;
        if (shift == 0) {
            newList.rootShift = shift;
            return node.copy(isOwned, childIdx+1);
        }
        if (node instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(childIdx) <= toIndex) {
                childIdx++;
            }
            if (childIdx != 0) {
                toIndex -= oldSizes.get(childIdx-1);
            }
        }
        boolean isChildLeftmost = isLeftmost && childIdx == 0;
        Node rightNode = isOwned ? node.getEditableChild(childIdx) : (Node) node.children[childIdx];
        rightNode = forkPrefixRec(toIndex, newList, rightNode, shift - SHIFT, isChildLeftmost, isOwned);
        if (isChildLeftmost) {
            return rightNode;
        }
        newList.rootShift = shift;
        return ((ParentNode) node).copyPrefix(isOwned, childIdx, rightNode, shift);
    }
    
    private static <E> Node forkSuffixRec(int fromIndex, TrieForkJoinList<E> newList, Node node, int shift, boolean isRightmost, boolean isOwned) {
        int childIdx = (fromIndex >>> shift) & MASK;
        if (shift == 0) {
            newList.rootShift = shift;
            return node.copySuffix(isOwned, childIdx);
        }
        if (node instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(childIdx) <= fromIndex) {
                childIdx++;
            }
            if (childIdx != 0) {
                fromIndex -= oldSizes.get(childIdx-1);
            }
        }
        boolean isChildRightmost = isRightmost && childIdx == node.children.length-1;
        Node leftNode = isOwned ? node.getEditableChild(childIdx) : (Node) node.children[childIdx];
        leftNode = forkSuffixRec(fromIndex, newList, leftNode, shift - SHIFT, isChildRightmost, isOwned);
        if (isChildRightmost) {
            return leftNode;
        }
        newList.rootShift = shift;
        return ((ParentNode) node).copySuffix(isOwned, childIdx, leftNode, isRightmost, shift);
    }
    
    private static <E> Node forkRangeRec(int fromIndex, int toIndex, TrieForkJoinList<E> newList, Node node, int shift) {
        int fromChildIdx = (fromIndex >>> shift) & MASK;
        int toChildIdx = (toIndex >>> shift) & MASK;
        if (shift == 0) {
            newList.rootShift = shift;
            return node.copyRange(false, fromChildIdx, toChildIdx+1);
        }
        if (node instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(fromChildIdx) <= fromIndex) {
                fromChildIdx++;
            }
            if (fromChildIdx > toChildIdx) {
                toChildIdx = fromChildIdx;
            }
            while (oldSizes.get(toChildIdx) <= toIndex) {
                toChildIdx++;
            }
            if (fromChildIdx != 0) {
                fromIndex -= oldSizes.get(fromChildIdx-1);
                toIndex -= oldSizes.get(toChildIdx-1);
            }
            else if (toChildIdx != 0) {
                toIndex -= oldSizes.get(toChildIdx-1);
            }
        }
        int childShift = shift - SHIFT;
        if (fromChildIdx == toChildIdx) {
            return forkRangeRec(fromIndex, toIndex, newList, (Node) node.children[fromChildIdx], childShift);
        }
        Node leftNode = forkSuffixRec(fromIndex, newList, (Node) node.children[fromChildIdx], childShift, false, false);
        Node rightNode = forkPrefixRec(toIndex, newList, (Node) node.children[toChildIdx], childShift, false, false);
        newList.rootShift = shift;
        return ((ParentNode) node).copyRange(false, fromChildIdx, leftNode, toChildIdx, rightNode, shift);
    }
    
    // Pros with a draining join:
    //   a.join(b) can transfer ownership of b - no fork() / lazy copying
    //
    // Cons with a draining join:
    //   a.join(a.subList(..)) is fine = "move subList to end"
    //   a.join(a) is a little confusing = "clear and re-add everything (ie no-op)"
    //    - here, the argument is not cleared
    //   a.subList(..).join(a) doesn't make sense = "clear and re-add everything after (non-existent) subList?"
    //    - here, the argument is not cleared, and the receiver is?!
    //   risk of wasted effort if argument is immutable, ie clear() throws
    //    - and clear() itself is risky if argument is a concurrent collection
    //
    // Pros with a copying join:
    //   joining subLists, or subLists joining parents, is intuitive
    //   no need to clear() argument just to be consistent with other cases
    //    - so joining immutable or concurrent collections is also a non-issue
    //   can always fall back to addAll()
    //
    // Cons with a copying join:
    //   a.join(b) has to conservatively fork b, so future updates will lazy-copy
    //
    // TODO: Does parent.fork() count as co-modification?
    //  - Not to subLists, and not necessarily to iterators
    //    - But iterators must ensureEditable() on the whole stack each time...
    //      - Because any intermediate parent may have been disowned by a subList.fork()...
    //  - Perhaps forking a subList counts as a mod, but not forking the root?
    //    - Iterators must check that the expected root hasn't changed, but not every intermediate node
    //      - ...because they will throw if an intermediate node was shared
    //    - This will be surprising when we join() a subList and implicitly fork() it
    //      - Invalidates progeny subLists and ALL iterators (because their cursor might be in the subList)
    //    - What about when root is re-owned?
    //      - Can still detect this - root will not match the first node in iterator stack
    //        - (Which implies iterator can prevent gc... probably ok)
    //        - In that case, traverse the stack and ensureEditable for each node
    //      - This can only happen due to a co-mod anyway
    //  - IDEA: Use a separate checksum to tell iterators to re-sync their stack on checksum mismatch
    //    - Only need to increment checksum if we changed some ownership during set() (unowned -> owned)
    //      or fork() (owned -> unowned, maybe only subList.fork() since list.fork() is a faster root-sync)
    //    - Unfortunately, unlike modCount which makes co-mods typically fail, this would make co-mods typically
    //      succeed, which would create a false sense of security.
    
    // if other is a known implementation: fork() it to ensure no unsafe sharing or mutation
    //  - this also protects the list when joining itself, or a sublist
    // else: addAll()
    
    // NOTE: Update if we add ForkJoinCollection superinterface
    //   - Having a new superinterface force a change feels bad
    //   - Plus, we are forking inputs unnecessarily just to see if it will yield the right type
    //   - This buys convenience at the cost of (documented) surprise (ie fork() can have co-mod effects)
    //     - Still less surprising than us mutating/destroying the argument...
    //  We could instead make forking the caller's responsibility, and check type + unowned?
    //   - Nope; we cannot assume caller is ok if we destroy something they gave us - still have to fork internally
    //   - Plus, requiring arg.fork() before calling to get optimization is unintuitive and forgettable
    
    // TODO: Nodes with free space?
    
    
    
    @Override
    public boolean join(Collection<? extends E> other) {
        // TODO: Update if we add ForkJoinCollection superinterface
        if (!(other instanceof ForkJoinList<? extends E> fjl)) {
            return addAll(other);
        }
        if (!((other = fjl.fork()) instanceof TrieForkJoinList<? extends E> right)) {
            return addAll(other); // TODO: Skip copying
        }
        if (right.isEmpty()) {
            return false;
        }
        if (isEmpty()) {
            size = right.size;
            tailSize = right.tailSize;
            rootShift = right.rootShift;
            root = right.root;
            tail = right.tail;
            owns = 0;
            return true;
        }

        if (right.root == null) {
            int leftTailSize = tailSize, rightTailSize = right.tailSize;
            Node rightTail = right.tail;

            if (leftTailSize == SPAN) {
                // Push down tail and adopt right tail
                pushDownTail(false);
                tail = rightTail;
                tailSize = rightTailSize;
                size += right.size;
                return true;
            }

            int newTailSize = leftTailSize + rightTailSize;
            Node leftTail = getEditableTail();

            if (newTailSize <= SPAN) {
                // Merge tails
                System.arraycopy(rightTail.children, 0, leftTail.children, leftTailSize, rightTailSize);
                tailSize = newTailSize;
                size += right.size;
                return true;
            }

            // Fill left tail from right tail; Push down; Adopt remaining right tail
            int suffixSize = SPAN - leftTailSize;
            System.arraycopy(rightTail.children,  0, leftTail.children, leftTailSize, suffixSize);
            pushDownTail(false);
            // TODO? Makes a new array - assumes right tail would need copied anyway
            Node newTail = tail = new Node(new Object[SPAN]);
            System.arraycopy(rightTail.children, suffixSize, newTail.children, 0, tailSize = rightTailSize - suffixSize);
            size += right.size;
            return true;
        }

        // Push down left tail
        if (tail.children.length != tailSize) {
            tail = tail.copy(claimTail(), tailSize);
        }
        pushDownTail(true);
        
        // Concatenate trees
        ConcatState state = new ConcatState();
        // TODO: Avoid editable if no rebalancing needed?
        state.left = getEditableRoot();
        state.right = right.getEditableRoot();
        concatSubTree(state, rootShift, right.rootShift, true);
        
        // Assign root
        size += right.size;
        Node leftNode = state.left, rightNode = state.right;
        int newRootShift = Math.max(rootShift, right.rootShift);
        if (rightNode == EMPTY_NODE) {
            root = leftNode;
            rootShift = newRootShift;
        }
        else {
            Object[] children = new Object[]{ leftNode, rightNode };
            ParentNode newRoot;
            int childSize;
            if ((childSize = getSizeIfNeedsSizedParent(leftNode, false, newRootShift)) != -1) {
                Sizes sizes = Sizes.of(rootShift = newRootShift + SHIFT, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, childSize);
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else if ((childSize = getSizeIfNeedsSizedParent(rightNode, true, newRootShift)) != -1) {
                Sizes sizes = Sizes.of(rootShift = newRootShift + SHIFT, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, lastSize - childSize);
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else {
                root = newRoot = new ParentNode(children);
                rootShift = newRootShift + SHIFT;
            }
            newRoot.claim(0);
            newRoot.claim(1);
        }
        
        // Adopt right tail, update size
        tail = right.tail;
        tailSize = right.tailSize;
        claimOrDisownTail(right.ownsTail());
        return true;
    }
    
    // This method is used during join() to determine if the node's parent needs to be Sized. If the parent needs to be
    // Sized, this method will return the node's size, to be accumulated in the Sized parent's sizes; else this method
    // will return -1. This method will return -1 if the node is not Sized, AND (contains the maximum number of children
    // OR is rightmost and not a leaf).
    static int getSizeIfNeedsSizedParent(Node node, boolean isRightmost, int shift) {
        if (node instanceof SizedParentNode sn) {
            return sn.sizes().get(sn.children.length-1);
        }
        if (isRightmost && shift != 0) {
            return -1;
        }
        int len = node.children.length;
        if (len == SPAN) {
            return -1;
        }
        return (len << shift);
    }
    
    // Single input+output object, reused in recursive calls to concatSubTree() and rebalance()
    //  - On the way down:
    //    - concatSubTree() extracts initial left/right, sets state.left=left.lastChild and state.right=right.firstChild, then recurses
    //  - On the way up:
    //    - concatSubTree() sets initial left.lastChild=state.left, right.firstChild=state.right, then restores initial left/right
    //    - rebalance() possibly updates left/right (eg to Sized/not-Sized/EMPTY) as a result of rebalancing their children
    private static class ConcatState {
        Node left, right;
    }
    
    private static final ParentNode EMPTY_NODE = new ParentNode(new Object[0]);
    
    private static void concatSubTree(ConcatState state, int leftShift, int rightShift, boolean isRightmost) {
        // TODO: Avoid editable if no rebalancing needed?
        Node left = state.left, right = state.right;
        if (leftShift > rightShift) {
            int childShift = leftShift - SHIFT;
            state.left = left.getEditableChild(left.children.length-1);
            concatSubTree(state, childShift, rightShift, true);
            
            // Right child may be empty after recursive call - due to rebalancing - but if not we introduce a parent
            ParentNode newRight = EMPTY_NODE;
            if (state.right != EMPTY_NODE) {
                int childSize = getSizeIfNeedsSizedParent(state.right, true, childShift);
                if (childSize != -1) {
                    Sizes sizes = Sizes.of(leftShift, 1);
                    sizes.set(0, childSize);
                    (newRight = new SizedParentNode(new Object[]{ state.right }, sizes)).claim(0);
                }
                else {
                    (newRight = new ParentNode(new Object[]{ state.right })).claim(0);
                }
            }
            right = newRight; // This might be EMPTY_NODE
            left.children[left.children.length-1] = state.left;
        }
        else if (leftShift < rightShift) {
            int childShift = rightShift - SHIFT;
            boolean isChildRightmost = isRightmost && right.children.length == 1;
            state.right = right.getEditableChild(0);
            concatSubTree(state, leftShift, childShift, isChildRightmost);
            
            // Left child is never empty after recursive call, because we introduce a parent for a non-empty child,
            // and we always rebalance by shifting left, so as we come up from recursion the left remains non-empty
            ParentNode newLeft;
            int childSize = getSizeIfNeedsSizedParent(state.left, isChildRightmost && state.right == EMPTY_NODE, childShift);
            if (childSize != -1) {
                Sizes sizes = Sizes.of(rightShift, 1);
                sizes.set(0, childSize);
                (newLeft = new SizedParentNode(new Object[]{ state.left }, sizes)).claim(0);
            }
            else {
                (newLeft = new ParentNode(new Object[]{ state.left })).claim(0);
            }
            left = newLeft;
            right.children[0] = state.right; // This might be EMPTY_NODE
            leftShift = rightShift; // Adjust so we can consistently use leftShift below
        }
        else if (leftShift > 0) { // leftShift == rightShift
            int childShift = leftShift - SHIFT;
            boolean isChildRightmost = isRightmost && right.children.length == 1;
            state.left = left.getEditableChild(left.children.length-1);
            state.right = right.getEditableChild(0);
            concatSubTree(state, childShift, childShift, isChildRightmost);
            
            left.children[left.children.length-1] = state.left;
            right.children[0] = state.right; // This might be EMPTY_NODE
        }
        else { // leftShift == rightShift == 0 (leaf-level)
            int leftLen = left.children.length, rightLen = right.children.length;
            if (leftLen + rightLen <= SPAN) {
                // Right fits in left - copy it over
                Object[] newChildren = Arrays.copyOf(left.children, leftLen + rightLen);
                System.arraycopy(right.children, 0, newChildren, leftLen, rightLen);
                state.left = new Node(newChildren);
                state.right = EMPTY_NODE;
            }
            return;
        }
        
        boolean isRightFirstChildEmpty = state.right == EMPTY_NODE;
        state.left = left;
        state.right = right;
        rebalance(state, leftShift, isRightmost, isRightFirstChildEmpty);
    }
    
    // Should we combine left/right Nodes at the end of rebalance?
    //  - This will happen anyway at the next level up if needed - except for at the roots
    //  - This isn't necessary to maintain the invariant, but is done by both Bagwell/Rompf and L'orange
    
    // The margin seems badly-behaved near the top of the tree...
    // Example: We may find that at the roots, minLength = 1, and we have 2, so we don't rebalance,
    //          But then we need to add a new root at the top of the tree, increasing the height.
    
    // We can constrain the total number of extra steps (not just per level) by starting with margin = MARGIN at the
    // bottom, and deducting any overage from margin, thus further constraining higher levels
    
    // The main disadvantage with the search-step invariant (as opposed to the min-span invariant) is that it is easier
    // for the tree to increase in height, because the search-step invariant is more lenient on how wasted space can be
    // distributed, which makes it easier to accumulate more of it.
    
    private static ParentNode rebalance1(ParentNode node, int shift, boolean isRightmost) {
        // Variant of rebalance used by removeRange when one child changes
        
        Object[] children = node.children;
        int totalNodes = 0;
        for (Object child : children) {
            totalNodes += ((Node) child).children.length;
        }
        
        int minLength = ((totalNodes-1) / SPAN) + 1;
        int maxLength = minLength + MARGIN;
        int curLength = children.length;
        if (curLength <= maxLength) {
            // Yay, no rebalancing needed
            // TODO: Implement a faster refresh for the special case of one child changed
            return node.refreshSizes(isRightmost, shift);
        }
        
        Node lastNode = isRightmost ? (Node) children[curLength-1] : null;
        int i = 0, step = 1, childShift = shift - SHIFT;
        long deathRow = 0;
        
        do {
            int curLen;
            while ((curLen = ((Node) children[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                i += step;
                step = 1;
            }
            int skip = 0;
            for (;;) {
                int j = i + step;
                step = 1;
                
                Node rChildNode = (Node) children[j];
                int slots = SPAN - curLen;
                int items = rChildNode.children.length;
                
                if (slots < items) {
                    // Fill the rest of the left child with the right child
                    node.editChild(i, skip, curLen, slots, rChildNode, node.owns(j), false, childShift);
                    i = j;
                    skip = slots;
                    curLen = items - slots;
                }
                else {
                    // Empty the rest of the right child into the left child
                    // TODO: If the outer loop doesn't break here, we may end up resizing this child again.
                    node.editChild(i, skip, curLen, items, rChildNode, node.owns(j), rChildNode == lastNode, childShift);
                    deathRow |= (1L << j);
                    step = 2; // Ensure we step over the gap we just created
                    break;
                }
            }
        } while (--curLength > maxLength);
        
        node.shiftChildren(deathRow, 0, 0, null, 0);
        return node.refreshSizes(isRightmost, shift);
    }
    
    private static ParentNode rebalance12(ParentNode parent, int fromChildIdx, int toChildIdx, int shift, boolean isRightmost) {
        // Variant of rebalance used by removeRange when two children of one parent change (with range-to-remove between them)
        
        Object[] children = parent.children;
        Node leftNode = (Node) children[fromChildIdx], rightNode = (Node) children[toChildIdx];
        
        if (leftNode == EMPTY_NODE) {
            if (rightNode == EMPTY_NODE) {
                if (toChildIdx - fromChildIdx + 1 == parent.children.length) {
                    // Nothing left - Parent becomes empty
                    return EMPTY_NODE;
                }
                // Shift to remove, refresh sizes
                // Removing children cannot increase parent's excess, so no rebalancing needed
                // TODO: Implement a faster range-removal + refresh for the special case of contiguous range
                long deathRow = (fromChildIdx == 0 ? -1L : (-2L << (fromChildIdx-1))) & ~(-2L << (toChildIdx+1));
                parent.shiftChildren(deathRow, 0, 0, null, 0);
                return parent.refreshSizes(isRightmost, shift);
            }
            fromChildIdx--;
        }
        else if (rightNode == EMPTY_NODE) {
            toChildIdx++;
        }
        
        int len = parent.children.length, totalNodes = 0;
        for (int i = 0; i <= fromChildIdx; i++) {
            totalNodes += ((Node) children[i]).children.length;
        }
        for (int i = toChildIdx; i < len; i++) {
            totalNodes += ((Node) children[i]).children.length;
        }
        
        long deathRow = (fromChildIdx < 0 ? -1L : (-2L << fromChildIdx)) & ~(-2L << toChildIdx);
        int minLength = ((totalNodes-1) / SPAN) + 1;
        int maxLength = minLength + MARGIN;
        int curLength = len - toChildIdx + fromChildIdx + 1;
        if (curLength <= maxLength) {
            // Yay, no rebalancing needed
            // TODO: Implement a faster range-removal + refresh for the special case of contiguous range
            parent.shiftChildren(deathRow, 0, 0, null, 0);
            return parent.refreshSizes(isRightmost, shift);
        }
        
        Node lastNode = isRightmost ? (Node) children[len-1] : null;
        int i, llen, rlen, step = 1, childShift = shift - SHIFT;
        if (fromChildIdx != -1) {
            i = 0;
            rlen = llen = fromChildIdx+1;
        }
        else {
            i = toChildIdx;
            rlen = llen = len;
        }
        
        do {
            int curLen;
            while ((curLen = ((Node) children[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    i = toChildIdx;
                    llen = len;
                }
                step = 1;
            }
            int skip = 0;
            for (;;) {
                int j = i + step;
                if (j >= llen) {
                    // We may hit this case multiple times, if we empty the first child
                    // on the right and the last child on the left is still below threshold
                    j = toChildIdx;
                    rlen = len;
                }
                step = 1;
                
                Node rChildNode = (Node) children[j];
                int slots = SPAN - curLen;
                int items = rChildNode.children.length;
                
                if (slots < items) {
                    // Fill the rest of the left child with the right child
                    parent.editChild(i, skip, curLen, slots, rChildNode, parent.owns(j), false, childShift);
                    i = j;
                    llen = rlen;
                    skip = slots;
                    curLen = items - slots;
                }
                else {
                    // Empty the rest of the right child into the left child
                    // TODO: If the outer loop doesn't break here, we may end up resizing this child again.
                    parent.editChild(i, skip, curLen, items, rChildNode, parent.owns(j), rChildNode == lastNode, childShift);
                    deathRow |= (1L << j);
                    step = 2; // Ensure we step over the gap we just created
                    break;
                }
            }
        } while (--curLength > maxLength);
        
        parent.shiftChildren(deathRow, 0, 0, null, 0);
        return parent.refreshSizes(isRightmost, shift);
    }
    
    private static void rebalance22(Node[] parents, int fromChildIdx, int toChildIdx, int shift, boolean isRightmost) {
        // Variant of rebalance used by removeRange when two children of two parents change (with range-to-remove between them)
        
        ParentNode left = (ParentNode) parents[0], right = (ParentNode) parents[1];
        Object[] leftChildren = left.children, rightChildren = right.children;
        Node leftNode = (Node) leftChildren[fromChildIdx], rightNode = (Node) rightChildren[toChildIdx];
        
        if (leftNode == EMPTY_NODE) {
            if (rightNode == EMPTY_NODE) {
                // Remove range (which will return EMPTY_NODE if range is [0,len))
                // TODO: Override removeRange for [Sized]ParentNode
                // TODO: Try empty right into left
                parents[0] = left.removeRange(true, fromChildIdx, leftChildren.length);
                parents[1] = right.removeRange(true, 0, toChildIdx+1);
                return;
            }
            fromChildIdx--;
        }
        else if (rightNode == EMPTY_NODE) {
            toChildIdx++;
        }
        
        int leftLen = leftChildren.length, rightLen = rightChildren.length, totalNodes = 0;
        for (int i = 0; i <= fromChildIdx; i++) {
            totalNodes += ((Node) leftChildren[i]).children.length;
        }
        for (int i = toChildIdx; i < rightLen; i++) {
            totalNodes += ((Node) rightChildren[i]).children.length;
        }
        
        int minLength = ((totalNodes-1) / SPAN) + 1;
        int maxLength = minLength + MARGIN;
        int curLength = rightLen - toChildIdx + fromChildIdx + 1;
        if (curLength <= maxLength) {
            // Yay, no rebalancing needed
            // TODO: Override removeRange for [Sized]ParentNode
            // TODO: Try empty right into left
            parents[0] = left.removeRange(true, Math.max(fromChildIdx, 0), leftLen);
            parents[1] = right.removeRange(true, 0, Math.min(toChildIdx+1, rightLen));
            return;
        }
        
        ParentNode lNode = left, rNode = left;
        Object[] lchildren = leftChildren, rchildren = leftChildren;
        Node lastNode = isRightmost ? (Node) rightChildren[rightLen-1] : null;
        //
        int i = 0, step = 1, llen = leftLen, rlen = llen, childShift = shift - SHIFT;
        long leftDeathRow = , rightDeathRow = 0;
        boolean updatedLeft = false;
        
        do {
            int curLen;
            while ((curLen = ((Node) lchildren[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    i -= llen + toChildIdx;
                    llen = rightChildren.length;
                    lNode = right;
                    lchildren = rightChildren;
                }
                step = 1;
            }
            updatedLeft = updatedLeft || lNode == left;
            int skip = 0;
            for (;;) {
                int j = i + step;
                if (j >= llen) {
                    // We may hit this case multiple times, if we empty the first child
                    // on the right and the last child on the left is still below threshold
                    j -= llen + toChildIdx;
                    rlen = rightChildren.length;
                    rNode = right;
                    rchildren = rightChildren;
                }
                step = 1;
                
                Node rChildNode = (Node) rchildren[j];
                int slots = SPAN - curLen;
                int items = rChildNode.children.length;
                
                if (slots < items) {
                    // Fill the rest of the left child with the right child
                    lNode.editChild(i, skip, curLen, slots, rChildNode, rNode.owns(j), false, childShift);
                    i = j;
                    llen = rlen;
                    lNode = rNode;
                    lchildren = rchildren;
                    skip = slots;
                    curLen = items - slots;
                }
                else {
                    // Empty the rest of the right child into the left child
                    // TODO: If the outer loop doesn't break here, we may end up resizing this child again.
                    lNode.editChild(i, skip, curLen, items, rChildNode, rNode.owns(j), rChildNode == lastNode, childShift);
                    
                    if (rNode == left) {
                        leftDeathRow |= (1L << j);
                    }
                    else {
                        rightDeathRow |= (1L << j);
                    }
                    
                    step = 2; // Ensure we step over the gap we just created
                    break;
                }
            }
        } while (--curLength > maxLength);
    }
    
    private static void rebalance(ConcatState state, int shift, boolean isRightmost, boolean isRightFirstChildEmpty) {
        // Assume left and right are editable
        
        ParentNode left = (ParentNode) state.left, right = (ParentNode) state.right;
        
        if (right == EMPTY_NODE || (isRightFirstChildEmpty && right.children.length == 1)) {
            // If right is now empty, then we may have increased the grandchildren under left's rightmost child.
            // Since the trees each start out balanced, and this change would only make left more dense,
            // there is no rebalancing to do.
            state.left = left.refreshSizesIfRightmostChildChanged(isRightmost, shift);
            state.right = EMPTY_NODE;
            return;
        }
        
        Object[] leftChildren = left.children;
        Object[] rightChildren = right.children;
        int totalNodes = 0;
        for (Object child : leftChildren) {
            totalNodes += ((Node) child).children.length;
        }
        for (Object child : rightChildren) {
            totalNodes += ((Node) child).children.length;
        }
        
        int minLength = ((totalNodes-1) / SPAN) + 1;
        int maxLength = minLength + MARGIN;
        int curLength = leftChildren.length + rightChildren.length;
        if (curLength <= maxLength) {
            // Yay, no rebalancing needed
            // But we do still have some work to refresh left/right if their rightmost/leftmost child changed
            if (isRightFirstChildEmpty) {
                if (leftChildren.length + rightChildren.length-1 <= SPAN) {
                    // Removing right's first child makes it fit in left
                    left.shiftChildren(0, 0, rightChildren.length-1, right, 1);
                    state.left = left.refreshSizes(isRightmost, shift);
                    state.right = EMPTY_NODE;
                }
                else {
                    // Removing right's first child does not make it fit in left
                    right.shiftChildren(1, 0, 0, null, 0);
                    state.right = right.refreshSizes(isRightmost, shift);
                    state.left = left.refreshSizesIfRightmostChildChanged(false, shift);
                }
            }
            else if (leftChildren.length + rightChildren.length <= SPAN) {
                // Right fits in left
                left.shiftChildren(0, 0, rightChildren.length, right, 0);
                state.left = left.refreshSizes(isRightmost, shift);
                state.right = EMPTY_NODE;
            }
            else {
                // Right does not fit in left
                state.left = left.refreshSizesIfRightmostChildChanged(false, shift);
                state.right = right.refreshSizesIfLeftmostChildChanged(isRightmost, shift);
            }
            return;
        }
        
        // Rebalance to eliminate excess children
        
        RebalanceResult result = doRebalance(left, right, maxLength - curLength, shift - SHIFT, isRightmost, isRightFirstChildEmpty);
        
        // Fixup left/right - remove emptied children, empty right into left if possible, and refresh sizes
        
        if (result.rightDeathRow == 0) { // Did not update right - must have updated left only (and leftDeathRow != 0)
            int remainingLeft = leftChildren.length - Long.bitCount(result.leftDeathRow);
            if (remainingLeft + rightChildren.length <= SPAN) {
                // Removing some from left makes right fit in
                left.shiftChildren(result.leftDeathRow, 0, rightChildren.length, right, 0);
                state.left = left.refreshSizes(isRightmost, shift);
                state.right = EMPTY_NODE;
            }
            else {
                // Removing some from left does not make right fit in
                left.shiftChildren(result.leftDeathRow, 0, 0, null, 0);
                state.left = left.refreshSizes(false, shift);
                state.right = right.refreshSizesIfLeftmostChildChanged(isRightmost, shift);
            }
        }
        else if (!result.updatedLeft) { // Updated right only
            int remainingRight = rightChildren.length - Long.bitCount(result.rightDeathRow);
            if (leftChildren.length + remainingRight <= SPAN) {
                // Removing some from right makes it fit in left
                left.shiftChildren(0, 0, remainingRight, right, result.rightDeathRow);
                state.left = left.refreshSizes(isRightmost, shift);
                state.right = EMPTY_NODE;
            }
            else {
                // Removing some from right does not make it fit in left
                right.shiftChildren(result.rightDeathRow, 0, 0, null, 0);
                state.right = right.refreshSizes(isRightmost, shift);
                state.left = left.refreshSizesIfRightmostChildChanged(false, shift);
            }
        }
        else { // Updated both
            // Since we're touching both anyway, we transfer as much as
            // we can from right to left, even if it doesn't empty right
            int remainingLeft = leftChildren.length - Long.bitCount(result.leftDeathRow);
            int remainingRight = rightChildren.length - Long.bitCount(result.rightDeathRow);
            int xfer = Math.min(SPAN - remainingLeft, remainingRight); // Possibly both 0
            left.shiftChildren(result.leftDeathRow, 0, xfer, right, result.rightDeathRow); // Possibly a no-op, if leftDeathRow == xfer == 0
            if (xfer == remainingRight) {
                state.left = left.refreshSizes(isRightmost, shift);
                state.right = EMPTY_NODE;
            }
            else {
                right.shiftChildren(result.rightDeathRow, xfer, 0, null, 0);
                state.right = right.refreshSizes(isRightmost, shift);
                state.left = left.refreshSizes(false, shift);
            }
        }
    }
    
    record RebalanceResult(long leftDeathRow, long rightDeathRow, boolean updatedLeft) { }
    
    private static RebalanceResult doRebalance(ParentNode left, ParentNode right,
                                               int excess, int childShift, boolean isRightmost, boolean isRightFirstChildEmpty) {
        ParentNode lNode = left, rNode = left;
        Object[] leftChildren = left.children, rightChildren = right.children, lchildren = leftChildren, rchildren = leftChildren;
        Node lastNode = isRightmost ? (Node) rightChildren[rightChildren.length-1] : null;
        int i = 0, step = 1, llen = leftChildren.length, rlen = llen;
        long leftDeathRow = 0, rightDeathRow = isRightFirstChildEmpty ? 1 : 0; // long assumes SPAN <= 64
        boolean updatedLeft = false;
        
        do {
            int curLen;
            while ((curLen = ((Node) lchildren[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    if ((i -= llen) == 0 && isRightFirstChildEmpty) {
                        i = 1;
                    }
                    llen = rightChildren.length;
                    lNode = right;
                    lchildren = rightChildren;
                }
                step = 1;
            }
            updatedLeft = updatedLeft || lNode == left;
            int skip = 0;
            for (;;) {
                int j = i + step;
                if (j >= llen) {
                    // We may hit this case multiple times, if we empty the first child
                    // on the right and the last child on the left is still below threshold
                    if ((j -= llen) == 0 && isRightFirstChildEmpty) {
                        j = 1;
                    }
                    rlen = rightChildren.length;
                    rNode = right;
                    rchildren = rightChildren;
                }
                step = 1;
                
                Node rChildNode = (Node) rchildren[j];
                int slots = SPAN - curLen;
                int items = rChildNode.children.length;
                
                if (slots < items) {
                    // Fill the rest of the left child with the right child
                    lNode.editChild(i, skip, curLen, slots, rChildNode, rNode.owns(j), false, childShift);
                    i = j;
                    llen = rlen;
                    lNode = rNode;
                    lchildren = rchildren;
                    skip = slots;
                    curLen = items - slots;
                }
                else {
                    // Empty the rest of the right child into the left child
                    // TODO: If the outer loop doesn't break here, we may end up resizing this child again.
                    lNode.editChild(i, skip, curLen, items, rChildNode, rNode.owns(j), rChildNode == lastNode, childShift);
                    
                    if (rNode == left) {
                        leftDeathRow |= (1L << j);
                    }
                    else {
                        rightDeathRow |= (1L << j);
                    }
                    
                    step = 2; // Ensure we step over the gap we just created
                    break;
                }
            }
        } while (--excess > 0);
        
        return new RebalanceResult(leftDeathRow, rightDeathRow, updatedLeft);
    }
    
    // Fixing-up left/right at the end of rebalance:
    //
    // If we need to visit both sides anyway, shift all left - it doesn't add work and MIGHT save revisiting left in higher rebalancing
    // Else if we can empty right into left, do - it doesn't add work (still only "visiting" one side b.c. right = EMPTY after) and MIGHT prevent higher rebalancing
    // Else, only visit the one side - we MIGHT need to revisit in higher rebalancing, but we still would if we shifted all left
    //
    // "shift all left" is only suboptimal if we only changed one side AND DID NOT need higher rebalancing
    // "shift to empty" is only suboptimal if we changed both sides AND could not empty AND DID need higher rebalancing
    
    private static int sizeSubTree(Node node, int shift) {
        int size = 0;
        for (; shift > 0; shift -= SHIFT) {
            int last = node.children.length-1;
            size += (last << shift);
            node = (Node) node.children[last];
        }
        return size + node.children.length;
    }
    
    @Override
    public boolean join(int index, Collection<? extends E> other) {
        // TODO: Optimize - do join(index) directly and in-place
        TrieForkJoinList<E> left = forkPrefix(index);
        TrieForkJoinList<E> right = forkSuffix(index);
        boolean added = left.join(other);
        left.join(right);
        size = left.size;
        tailSize = left.tailSize;
        rootShift = left.rootShift;
        root = left.root;
        tail = left.tail;
        owns = left.owns;
        return added;
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
    
    // TODO: Implement RandomAccess?
    private static class SubList<E> extends AbstractList<E> implements ForkJoinList<E> {
        private final TrieForkJoinList<E> root;
        private final SubList<E> parent;
        private final int offset;
        private int size;
        
        SubList(TrieForkJoinList<E> root, int fromIndex, int toIndex) {
            this.root = root;
            this.parent = null;
            this.offset = fromIndex;
            this.size = toIndex - fromIndex;
            this.modCount = root.modCount;
        }
        
        SubList(SubList<E> parent, int fromIndex, int toIndex) {
            this.root = parent.root;
            this.parent = parent;
            this.offset = parent.offset + fromIndex;
            this.size = toIndex - fromIndex;
            this.modCount = parent.modCount;
        }
        
        @Override
        public boolean join(Collection<? extends E> other) {
            return root.join(offset, other);
        }
        
        @Override
        public boolean join(int index, Collection<? extends E> other) {
            return root.join(offset + index, other);
        }
        
        @Override
        public ForkJoinList<E> fork() {
            return root.forkRange(offset, offset + size);
        }
        
        @Override
        public E get(int index) {
            return root.get(offset + index);
        }
        
        @Override
        public int size() {
            return size;
        }
        
        @Override
        public ForkJoinList<E> subList(int fromIndex, int toIndex) {
            subListRangeCheck(fromIndex, toIndex, size);
            return new SubList<>(this, fromIndex, toIndex);
        }
    }
    
    private static class Node {
        Object[] children;
        
        Node(Object[] children) {
            this.children = children;
        }
        
        Node copy(boolean isOwned) {
            return isOwned ? this : new Node(children.clone());
        }
        
        Node copy(boolean isOwned, int len) {
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                if (isOwned) {
                    children = newChildren;
                    return this;
                }
                return new Node(newChildren);
            }
            return copy(isOwned);
        }
        
        Node copySuffix(boolean isOwned, int from) {
            if (from != 0) {
                Object[] newChildren = Arrays.copyOfRange(children, from, children.length);
                if (isOwned) {
                    children = newChildren;
                    return this;
                }
                return new Node(newChildren);
            }
            return copy(isOwned);
        }
        
        Node copyRange(boolean isOwned, int from, int to) {
            if (from != 0 || to != children.length) {
                Object[] newChildren = Arrays.copyOfRange(children, from, to);
                if (isOwned) {
                    children = newChildren;
                    return this;
                }
                return new Node(newChildren);
            }
            return copy(isOwned);
        }
        
        Node removeRange(boolean isOwned, int from, int to) {
            int len = children.length;
            if (from == 0 && to == len) {
                return EMPTY_NODE;
            }
            int newSize = len - (to-from);
            Object[] newChildren = new Object[newSize];
            System.arraycopy(children, 0, newChildren, 0, from);
            System.arraycopy(children, to, newChildren, from, len-to);
            if (isOwned) {
                children = newChildren;
                return this;
            }
            return new Node(newChildren);
        }
        
        Node copySized(boolean isOwned, int shift) {
            return copy(isOwned);
        }
        
        Node copySized(boolean isOwned, int len, int shift) {
            return copy(isOwned, len);
        }
        
        Node copy(boolean isOwned, int skip, int keep, int take, Node from,
                  boolean isFromOwned, boolean isRightmost, int shift) {
            assert take > 0;
            int newLen = keep + take;
            
            // Handle children
            Object[] newChildren;
            if (skip != take) { // length is not preserved, so must copy
                newChildren = Arrays.copyOfRange(children, skip, skip + newLen);
            }
            else { // skip == take, so length is preserved, and must skip (because take > 0)
                newChildren = isOwned ? children : new Object[newLen];
                System.arraycopy(children, skip, newChildren, 0, keep);
            }
            System.arraycopy(from.children, 0, newChildren, keep, take);
            
            if (isOwned) {
                children = newChildren;
                return this;
            }
            return new Node(newChildren);
        }
        
        Node getEditableChild(int i) {
            throw new AssertionError();
        }
        
        Node getEditableChild(int i, int len) {
            throw new AssertionError();
        }
        
        Node getSizedEditableChild(int i, int shift) {
            throw new AssertionError();
        }
        
        Node getSizedEditableChild(int i, int len, int shift) {
            throw new AssertionError();
        }
    }
    
    private static class ParentNode extends Node {
        // NOTE: Type of owns (and related operations) need to be co-updated with SPAN:
        // SPAN=16 -> short
        // SPAN=32 -> int
        // SPAN=64 -> long
        // SPAN>64 -> long[]
        short owns;
        
        ParentNode(short owns, Object[] children) {
            super(children);
            this.owns = owns;
        }
        
        ParentNode(Object[] children) {
            super(children);
            this.owns = 0;
        }
        
        boolean owns(int i) {
            return (owns & (1 << i)) != 0;
        }
        
        boolean claim(int i) {
            return owns == (owns |= (1 << i));
        }
        
        void claimOrDisown(int i, boolean claim) {
            if (claim) {
                owns |= (1 << i);
            }
            else {
                owns &= ~(1 << i);
            }
        }
        
        void disownPrefix(int to) {
            owns &= ~mask(to);
        }
        
        void disownSuffix(int from) {
            owns &= mask(from);
        }
        
        void disownRange(int from, int to) {
            owns &= mask(from) | ~mask(to);
        }
        
        static short skipOwnership(short owns, int skip, int keep) {
            return (short) ((owns >>> skip) & mask(keep));
        }
        
        static short takeOwnership(int keep, int take, short owns) {
            return (short) ((owns & mask(take)) << keep);
        }
        
        static short removeFromOwnership(short owns, int remove) {
            return (short) ((owns & mask(remove)) | ((owns >>> 1) & ~mask(remove)));
        }
        
        static int mask(int shift) {
            // TODO: Alternatively: ~(-2 << shift), or just (-2 << shift) for negated mask
            return (1 << shift) - 1;
        }
        
        @Override
        ParentNode copy(boolean isOwned) {
            return isOwned ? this : new ParentNode(children.clone());
        }
        
        @Override
        ParentNode copy(boolean isOwned, int len) {
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                if (isOwned) {
                    children = newChildren;
                    return this;
                }
                return new ParentNode(newChildren);
            }
            return copy(isOwned);
        }
        
        @Override
        ParentNode copySuffix(boolean isOwned, int from) {
            // TODO: Integrate into full method
            if (from != 0) {
                Object[] newChildren = Arrays.copyOfRange(children, from, children.length);
                if (isOwned) {
                    owns >>>= from;
                    children = newChildren;
                    return this;
                }
                return new ParentNode(newChildren);
            }
            return copy(isOwned);
        }
        
        @Override
        ParentNode copyRange(boolean isOwned, int from, int to) {
            // TODO: Integrate into full method
            if (from != 0 || to != children.length) {
                Object[] newChildren = Arrays.copyOfRange(children, from, to);
                if (isOwned) {
                    owns >>>= from; // shift off prefix; suffix bits are simply ignored
                    children = newChildren;
                    return this;
                }
                return new ParentNode(newChildren);
            }
            return copy(isOwned);
        }
        
        @Override
        ParentNode copySized(boolean isOwned, int shift) {
            if (shift == 0) {
                return copy(isOwned);
            }
            int oldLen = children.length;
            Sizes sizes = Sizes.of(shift, oldLen);
            sizes.fill(0, oldLen, shift);
            if (isOwned) {
                return new SizedParentNode(owns, children, sizes);
            }
            return new SizedParentNode(children.clone(), sizes);
        }
        
        @Override
        ParentNode copySized(boolean isOwned, int len, int shift) {
            if (shift == 0) {
                return copy(isOwned, len);
            }
            if (children.length != len) {
                int oldLen = children.length;
                Sizes sizes = Sizes.of(shift, len);
                sizes.fill(0, oldLen, shift);
                Object[] newChildren = Arrays.copyOf(children, len);
                if (isOwned) {
                    return new SizedParentNode(owns, newChildren, sizes);
                }
                return new SizedParentNode(newChildren, sizes);
            }
            return copySized(isOwned, shift);
        }
        
        ParentNode copyPrefix(boolean isOwned, int toIndex, Node newLastChild, int shift) {
            disownPrefix(toIndex); // TODO: avoid if isOwned
            ParentNode newNode = copy(isOwned, toIndex+1);
            newNode.claim(toIndex);
            newNode.children[toIndex] = newLastChild;
            return newNode.refreshSizesIfRightmostChildChanged(true, shift); // TODO: Skip straight to Sized if needed (skip intermediate copy)
        }
        
        ParentNode copySuffix(boolean isOwned, int fromIndex, Node newFirstChild, boolean isRightmost, int shift) {
            disownSuffix(fromIndex+1); // TODO: avoid if isOwned
            ParentNode newNode = copySuffix(isOwned, fromIndex);
            newNode.claim(0);
            newNode.children[0] = newFirstChild;
            return newNode.refreshSizesIfLeftmostChildChanged(isRightmost, shift); // TODO: Skip straight to Sized if needed (skip intermediate copy)
        }
        
        ParentNode copyRange(boolean isOwned, int fromIndex, Node newFirstChild, int toIndex, Node newLastChild, int shift) {
            int lastIdx = toIndex-fromIndex;
            disownRange(fromIndex+1, toIndex); // TODO: avoid if isOwned
            ParentNode newNode = copyRange(isOwned, fromIndex, toIndex+1);
            newNode.claim(0);
            newNode.claim(lastIdx);
            newNode.children[0] = newFirstChild;
            newNode.children[lastIdx] = newLastChild;
            return newNode.refreshSizes(true, shift); // TODO: Skip straight to Sized if needed (skip intermediate copy)
        }
        
        @Override
        ParentNode copy(boolean isOwned, int skip, int keep, int take, Node from,
                        boolean isFromOwned, boolean isRightmost, int shift) {
            assert take > 0;
            int newLen = keep + take;
            
            // Handle sizes
            Sizes newSizes = null;
            if (from instanceof SizedParentNode sn) {
                Sizes fromSizes = sn.sizes();
                // if taken children are not full, we must be sized
                if (isRightmost || fromSizes.get(take-1) != (take << shift)) {
                    newSizes = Sizes.of(shift, newLen);
                    int lastSize = newSizes.fill(0, keep, shift);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize + fromSizes.get(i));
                    }
                }
            }
            // else we are taking from a not-sized node, so it either has full children or is rightmost,
            // so we will retain full children or become rightmost, so we can stay not-sized.
            
            // Handle children
            Object[] newChildren;
            if (skip != take) { // length is not preserved, so must copy
                newChildren = Arrays.copyOfRange(children, skip, skip + newLen);
            }
            else { // skip == take, so length is preserved, and must skip (because take > 0)
                newChildren = isOwned ? children : new Object[newLen];
                System.arraycopy(children, skip, newChildren, 0, keep);
            }
            System.arraycopy(from.children, 0, newChildren, keep, take);
            
            // Handle ownership
            short newOwns = (short) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, ((ParentNode) from).owns) : 0));
            if (newSizes != null) {
                return new SizedParentNode(newOwns, newChildren, newSizes);
            }
            if (isOwned) {
                owns = newOwns;
                children = newChildren;
                return this;
            }
            return new ParentNode(newOwns, newChildren);
        }
        
        void shiftChildren(long deathRow, int skip, int take, ParentNode from, long fromDeathRow) {
            // Assume this and from are owned
            // At most one of skip/take will be non-zero
            // If take == 0, from/fromDeathRow are unused
            
            // Remove deleted children
            int keep, newLen;
            short newOwns = owns;
            Object[] newChildren;
            if (deathRow != 0) {
                Object[] oldChildren = children;
                int len = oldChildren.length;
                int toRemove = Long.bitCount(deathRow);
                newLen = len - skip - toRemove + take;
                newChildren = newLen == len ? oldChildren : new Object[newLen];
                keep = 0;
                for (int i = 0; i < len; i++) {
                    if ((deathRow & (1L << i)) != 0 || skip-- > 0) {
                        newOwns = removeFromOwnership(newOwns, i);
                    }
                    else {
                        newChildren[keep++] = oldChildren[i];
                    }
                }
            }
            else if (skip != take) {
                newLen = (keep = children.length - skip) + take;
                newChildren = Arrays.copyOfRange(children, skip, newLen);
                newOwns = skipOwnership(newOwns, skip, keep);
            }
            else { // deathRow == skip == take == 0; nothing to do
                return;
            }
            
            if (take != 0) {
                // Add adopted children
                short fromOwns = from.owns;
                Object[] fromChildren = from.children;
                if (fromDeathRow != 0) {
                    for (int i = 0; keep < newLen; i++) {
                        if ((fromDeathRow & (1L << i)) == 0) {
                            newOwns |= ((1 << i) & fromOwns) != 0 ? (1 << keep) : 0;
                            newChildren[keep++] = fromChildren[i];
                        }
                    }
                }
                else {
                    System.arraycopy(fromChildren, 0, newChildren, keep, take);
                    newOwns |= takeOwnership(keep, take, fromOwns);
                }
            }
            
            owns = newOwns;
            children = newChildren;
        }
        
        static Sizes computeSizes(Object[] children, boolean isRightmost, int shift) {
            // Scan children to determine if we are now sized + compute sizes if so
            int len = children.length;
            Sizes sizes = null;
            if (shift == SHIFT) {
                // We are sized if any children are not full
                for (int i = 0; i < len; i++) {
                    Node child = (Node) children[i];
                    if (child.children.length < SPAN) {
                        sizes = Sizes.of(shift, len);
                        int lastSize = sizes.fill(0, i, shift);
                        sizes.set(i, lastSize += child.children.length);
                        while (++i < len) {
                            int currSize = ((Node) children[i]).children.length;
                            sizes.set(i, lastSize += currSize);
                        }
                        break;
                    }
                }
            }
            else {
                // We are sized if any children need us to be
                int childShift = shift - SHIFT;
                for (int i = 0; i < len; i++) {
                    int childSize = getSizeIfNeedsSizedParent((Node) children[i], isRightmost && i == len-1, childShift);
                    if (childSize != -1) {
                        sizes = Sizes.of(shift, len);
                        int lastSize = sizes.fill(0, i, shift);
                        sizes.set(i, lastSize += childSize);
                        while (++i < len) {
                            Node child = (Node) children[i];
                            int currSize = child instanceof SizedParentNode sn ? sn.sizes().get(sn.children.length-1)
                                : (isRightmost && i == len-1) ? sizeSubTree(child, childShift)
                                : (child.children.length << childShift);
                            sizes.set(i, lastSize += currSize);
                        }
                        break;
                    }
                }
            }
            return sizes;
        }
        
        ParentNode refreshSizes(boolean isRightmost, int shift) {
            Sizes newSizes = computeSizes(children, isRightmost, shift);
            if (newSizes != null) {
                return new SizedParentNode(owns, children, newSizes);
            }
            return this;
        }
        
        ParentNode refreshSizesIfRightmostChildChanged(boolean isRightmost, int shift) {
            Object[] oldChildren = children;
            int len = oldChildren.length;
            int childSize = getSizeIfNeedsSizedParent((Node) oldChildren[len-1], isRightmost, shift - SHIFT);
            if (childSize != -1) {
                Sizes newSizes = Sizes.of(shift, len);
                int lastSize = newSizes.fill(0, len-1, shift);
                newSizes.set(len-1, lastSize + childSize);
                return new SizedParentNode(owns, oldChildren, newSizes);
            }
            return this;
        }
        
        ParentNode refreshSizesIfLeftmostChildChanged(boolean isRightmost, int shift) {
            Object[] oldChildren = children;
            int len = oldChildren.length;
            int childSize = getSizeIfNeedsSizedParent((Node) oldChildren[0], isRightmost && len == 1, shift - SHIFT);
            if (childSize != -1) {
                Sizes newSizes = Sizes.of(shift, len);
                newSizes.set(0, childSize);
                if (isRightmost && len > 1) {
                    int lastSize = newSizes.fill(1, len-1, shift);
                    newSizes.set(len-1, lastSize + sizeSubTree((Node) oldChildren[len-1], shift - SHIFT));
                }
                else {
                    newSizes.fill(1, len, shift);
                }
                return new SizedParentNode(owns, oldChildren, newSizes);
            }
            return this;
        }
        
        // TODO: Methods for 'decrementAndGetChild(i)' 'incrementAndGetChild(i, addedSize)' would probably be handy
        
        @Override
        Node getEditableChild(int i) {
            Node tmp = ((Node) children[i]).copy(claim(i));
            children[i] = tmp;
            return tmp;
        }
        
        @Override
        Node getEditableChild(int i, int len) {
            Node tmp = ((Node) children[i]).copy(claim(i), len);
            children[i] = tmp;
            return tmp;
        }
        
        @Override
        Node getSizedEditableChild(int i, int shift) {
            Node tmp = ((Node) children[i]).copySized(claim(i), shift);
            children[i] = tmp;
            return tmp;
        }
        
        @Override
        Node getSizedEditableChild(int i, int len, int shift) {
            Node tmp = ((Node) children[i]).copySized(claim(i), len, shift);
            children[i] = tmp;
            return tmp;
        }
        
        // Special-purpose method used during rebalance
        void editChild(int i, int skip, int keep, int take, Node from,
                       boolean isFromOwned, boolean isRightmost, int shift) {
            children[i] = ((Node) children[i]).copy(claim(i), skip, keep, take, from, isFromOwned, isRightmost, shift);
        }
    }
    
    private static class SizedParentNode extends ParentNode {
        // TODO: Weigh the cost of storing Sizes directly, instead of allocating every time we touch sizes.
        Object sizes;
        
        SizedParentNode(short owns, Object[] children, Sizes sizes) {
            super(owns, children);
            this.sizes = sizes.unwrap();
        }
        
        SizedParentNode(Object[] children, Sizes sizes) {
            super(children);
            this.sizes = sizes.unwrap();
        }
        
        Sizes sizes() {
            return Sizes.wrap(sizes);
        }
        
        @Override
        SizedParentNode copy(boolean isOwned) {
            return isOwned ? this : new SizedParentNode(children.clone(), sizes().copy());
        }
        
        @Override
        SizedParentNode copy(boolean isOwned, int len) {
            // TODO: If truncating (len < curLen), could become not-Sized
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                Sizes newSizes = sizes().copy(len);
                if (isOwned) {
                    children = newChildren;
                    sizes = newSizes.unwrap();
                    return this;
                }
                return new SizedParentNode(newChildren, newSizes);
            }
            return copy(isOwned);
        }
        
        @Override
        ParentNode copySuffix(boolean isOwned, int from) {
            // TODO: Integrate into full method
            // TODO: could become not-Sized
            if (from != 0) {
                int len = children.length;
                Object[] newChildren = Arrays.copyOfRange(children, from, len);
                Sizes newSizes = sizes().copyOfRange(from, len);
                if (isOwned) {
                    owns >>>= from;
                    children = newChildren;
                    sizes = newSizes.unwrap();
                    return this;
                }
                return new SizedParentNode(newChildren, newSizes);
            }
            return copy(isOwned);
        }
        
        @Override
        ParentNode copyRange(boolean isOwned, int from, int to) {
            // TODO: Integrate into full method
            // TODO: could become not-Sized
            if (from != 0 || to != children.length) {
                Object[] newChildren = Arrays.copyOfRange(children, from, to);
                Sizes newSizes = sizes().copyOfRange(from, to);
                if (isOwned) {
                    owns >>>= from; // shift off prefix; suffix bits are simply ignored
                    children = newChildren;
                    sizes = newSizes.unwrap();
                    return this;
                }
                return new SizedParentNode(newChildren, newSizes);
            }
            return copy(isOwned);
        }
        
        @Override
        SizedParentNode copySized(boolean isOwned, int shift) {
            return copy(isOwned);
        }
        
        @Override
        SizedParentNode copySized(boolean isOwned, int len, int shift) {
            return copy(isOwned, len);
        }
        
        @Override
        ParentNode copyPrefix(boolean isOwned, int toIndex, Node newLastChild, int shift) {
            disownPrefix(toIndex); // TODO: avoid if isOwned
            ParentNode newNode = copy(isOwned, toIndex+1); // TODO: Truncating-copy could make us not-Sized
            newNode.claim(toIndex);
            newNode.children[toIndex] = newLastChild;
            return newNode.refreshSizesIfRightmostChildChanged(true, shift); // TODO: Could become Sized or not-Sized
        }
        
        @Override
        ParentNode copySuffix(boolean isOwned, int fromIndex, Node newFirstChild, boolean isRightmost, int shift) {
            disownSuffix(fromIndex+1); // TODO: avoid if isOwned
            ParentNode newNode = copySuffix(isOwned, fromIndex); // TODO: Truncating-copy could make us not-Sized
            newNode.claim(0);
            newNode.children[0] = newFirstChild;
            return newNode.refreshSizesIfLeftmostChildChanged(isRightmost, shift); // TODO: Could become Sized or not-Sized
        }
        
        @Override
        ParentNode copyRange(boolean isOwned, int fromIndex, Node newFirstChild, int toIndex, Node newLastChild, int shift) {
            disownRange(fromIndex+1, toIndex); // TODO: avoid if isOwned (currently, isOwned = false always)
            ParentNode newNode = copyRange(isOwned, fromIndex, toIndex+1); // TODO: Truncating-copy could make us not-Sized
            newNode.claim(0);
            newNode.claim(toIndex);
            newNode.children[0] = newFirstChild;
            newNode.children[toIndex] = newLastChild;
            return newNode.refreshSizes(true, shift); // TODO: Could become Sized or not-Sized
        }
        
        @Override
        ParentNode copy(boolean isOwned, int skip, int keep, int take, Node from,
                        boolean isFromOwned, boolean isRightmost, int shift) {
            assert take > 0;
            
            int oldLen = skip + keep;
            int newLen = keep + take;
            Sizes oldSizes = sizes();
            
            // Handle sizes
            int lastSize = oldSizes.get(oldLen-1);
            Sizes newSizes = (skip == 0 ? lastSize : (lastSize -= oldSizes.get(skip-1))) != (keep << shift)
                ? skip != take
                    ? oldSizes.copyOfRange(skip, skip + newLen)
                    : (isOwned ? oldSizes : Sizes.of(shift, newLen)).arrayCopy(oldSizes, skip, keep)
                : null;
            
            if (from instanceof SizedParentNode sn) {
                Sizes fromSizes = sn.sizes();
                if (newSizes != null) {
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize + fromSizes.get(i));
                    }
                }
                else if (isRightmost || fromSizes.get(take-1) != (take << shift)) {
                    // taken children are not full - we need to be sized
                    newSizes = Sizes.of(shift, newLen);
                    lastSize = newSizes.fill(0, keep, shift);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize + fromSizes.get(i));
                    }
                }
            }
            else if (newSizes != null) {
                if (isRightmost) {
                    lastSize = newSizes.fill(keep, newLen-1, shift);
                    newSizes.set(newLen-1, lastSize + sizeSubTree((Node) from.children[take-1], shift - SHIFT));
                }
                else {
                    newSizes.fill(keep, newLen, shift);
                }
            }
            
            // Handle children
            Object[] newChildren;
            if (skip != take) { // length is not preserved, so must copy
                newChildren = Arrays.copyOfRange(children, skip, skip + newLen);
            }
            else { // skip == take, so length is preserved, and must skip (because take > 0)
                newChildren = isOwned ? children : new Object[newLen];
                System.arraycopy(children, skip, newChildren, 0, keep);
            }
            System.arraycopy(from.children, 0, newChildren, keep, take);
            
            // Handle ownership
            short newOwns = (short) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, ((ParentNode) from).owns) : 0));
            if (newSizes == null) {
                return new ParentNode(newOwns, newChildren);
            }
            if (!isOwned) {
                return new SizedParentNode(newOwns, newChildren, newSizes);
            }
            owns = newOwns;
            children = newChildren;
            sizes = newSizes.unwrap();
            return this;
        }
        
        @Override
        ParentNode refreshSizes(boolean isRightmost, int shift) {
            Sizes newSizes = computeSizes(children, isRightmost, shift);
            if (newSizes == null) {
                return new ParentNode(owns, children);
            }
            sizes = newSizes.unwrap();
            return this;
        }
        
        @Override
        ParentNode refreshSizesIfRightmostChildChanged(boolean isRightmost, int shift) {
            Object[] oldChildren = children;
            int len = oldChildren.length;
            Node child = (Node) oldChildren[len-1];
            Sizes oldSizes = sizes();
            int lastSize = oldSizes.get(len-1);
            int prevSize = (len == 1 ? 0 : oldSizes.get(len-2));
            int currSize = lastSize - prevSize;
            int newSize;
            if (child instanceof SizedParentNode sn) {
                newSize = sn.sizes().get(sn.children.length-1);
            }
            else if (isRightmost && shift != SHIFT) {
                if (prevSize == ((len-1) << shift)) {
                    // All children before last are full
                    return new ParentNode(owns, oldChildren);
                }
                newSize = sizeSubTree(child, shift - SHIFT);
            }
            else {
                newSize = (child.children.length << (shift - SHIFT));
                if (lastSize - currSize + newSize == (len << shift)) {
                    // All children are full
                    return new ParentNode(owns, oldChildren);
                }
            }
            if (currSize != newSize) {
                // Size changed
                oldSizes.set(len-1, prevSize + newSize);
            }
            return this;
        }
        
        @Override
        ParentNode refreshSizesIfLeftmostChildChanged(boolean isRightmost, int shift) {
            Object[] oldChildren = children;
            int len = oldChildren.length;
            Node child = (Node) oldChildren[0];
            Sizes oldSizes = sizes();
            int currSize = oldSizes.get(0);
            int newSize;
            if (child instanceof SizedParentNode sn) {
                newSize = sn.sizes().get(sn.children.length-1);
            }
            else if (isRightmost && shift != SHIFT) {
                if (len == 1) {
                    // All (0) children before last are full
                    return new ParentNode(owns, oldChildren);
                }
                // len > 1, so child is not rightmost, so must have full children
                newSize = (child.children.length << (shift - SHIFT));
                if (!(oldChildren[len-1] instanceof SizedParentNode) && // last child has no gaps
                    oldSizes.get(len-2) - currSize + newSize == ((len-1) << shift)) { // children before last are full
                    // All children before last are full
                    return new ParentNode(owns, oldChildren);
                }
            }
            else {
                newSize = (child.children.length << (shift - SHIFT));
                if (oldSizes.get(len-1) - currSize + newSize == (len << shift)) {
                    // All children are full
                    return new ParentNode(owns, oldChildren);
                }
            }
            if (currSize != newSize) {
                // Size changed
                oldSizes.set(0, newSize);
                int delta = newSize - currSize;
                for (int i = 1; i < len; i++) {
                    oldSizes.inc(i, delta);
                }
            }
            return this;
        }
    }
    
    private static abstract class Sizes {
        abstract int get(int i);
        abstract void set(int i, int size);
        abstract Object unwrap();
        abstract Sizes copy();
        abstract Sizes copy(int len);
        abstract Sizes copyOfRange(int from, int to);
        abstract Sizes arrayCopy(Sizes src, int srcPos, int len);
        abstract int fill(int from, int to, int shift);
        
        void inc(int i, int size) {
            set(i, get(i) + size);
        }
        
        // To stay in the range of narrower primitive types for as long as possible,
        // we take advantage of the fact that we never need to store a size of 0,
        // by subtracting 1 before storing the size.
        //
        // This can be very important for space utilization. For example:
        // A SHIFT of 4 gives a SPAN of 16, so level 1 parent nodes (shift=4)
        // can have a maximum cumulative size of 16^2 = 256 elements, which we can
        // just barely fit in a byte after subtracting 1. Likewise for SHIFT 4 at
        // level 3 (shift=12) => 16^4 = 65536 elements, which can just barely fit
        // in a short after subtracting 1.
        
        static Sizes wrap(Object o) {
            return switch (o) {
                case byte[] arr -> new Sizes.OfByte(arr);
                case char[] arr -> new Sizes.OfShort(arr);
                case int[] arr -> new Sizes.OfInt(arr);
                default -> throw new AssertionError();
            };
        }
        
        static Sizes of(int shift, int len) {
            if ((SPAN<<shift) <= 256) {
                return new Sizes.OfByte(new byte[len]);
            }
            if ((SPAN<<shift) <= 65536) {
                return new Sizes.OfShort(new char[len]);
            }
            return new Sizes.OfInt(new int[len]);
        }
        
        static class OfByte extends Sizes {
            byte[] sizes;
            OfByte(byte[] sizes) { this.sizes = sizes; }
            int get(int i) { return Byte.toUnsignedInt(sizes[i])+1; }
            void set(int i, int size) { sizes[i] = (byte) (size-1); }
            byte[] unwrap() { return sizes; }
            OfByte copy() { sizes = sizes.clone(); return this; }
            OfByte copy(int len) { sizes = Arrays.copyOf(sizes, len); return this; }
            
            OfByte copyOfRange(int from, int to) {
                if (from == 0) {
                    sizes = Arrays.copyOf(sizes, to);
                }
                else {
                    int prefixLen = Math.min(sizes.length, to) - from;
                    int initialSize = sizes[from-1]+1;
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < prefixLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfByte arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                byte[] other = (byte[]) src.unwrap();
                int initialSize = other[srcPos-1]+1;
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to, int shift) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (byte) (lastSize += (1 << shift));
                }
                return lastSize + 1;
            }
        }
        
        static class OfShort extends Sizes {
            char[] sizes;
            OfShort(char[] sizes) { this.sizes = sizes; }
            int get(int i) { return sizes[i]+1; }
            void set(int i, int size) { sizes[i] = (char) (size-1); }
            char[] unwrap() { return sizes; }
            OfShort copy() { sizes = sizes.clone(); return this; }
            OfShort copy(int len) { sizes = Arrays.copyOf(sizes, len); return this; }
            
            OfShort copyOfRange(int from, int to) {
                if (from == 0) {
                    sizes = Arrays.copyOf(sizes, to);
                }
                else {
                    int prefixLen = Math.min(sizes.length, to) - from;
                    int initialSize = sizes[from-1]+1;
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < prefixLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfShort arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                char[] other = (char[]) src.unwrap();
                int initialSize = other[srcPos-1]+1;
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to, int shift) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (char) (lastSize += (1 << shift));
                }
                return lastSize + 1;
            }
        }
        
        static class OfInt extends Sizes {
            int[] sizes;
            OfInt(int[] sizes) { this.sizes = sizes; }
            int get(int i) { return sizes[i]+1; }
            void set(int i, int size) { sizes[i] = size-1; }
            int[] unwrap() { return sizes; }
            OfInt copy() { sizes = sizes.clone(); return this; }
            OfInt copy(int len) { sizes = Arrays.copyOf(sizes, len); return this; }
            
            OfInt copyOfRange(int from, int to) {
                if (from == 0) {
                    sizes = Arrays.copyOf(sizes, to);
                }
                else {
                    int prefixLen = Math.min(sizes.length, to) - from;
                    int initialSize = sizes[from-1]+1;
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < prefixLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfInt arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                int[] other = (int[]) src.unwrap();
                int initialSize = other[srcPos-1]+1;
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to, int shift) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (lastSize += (1 << shift));
                }
                return lastSize + 1;
            }
        }
    }
    
    // children-elements:
    //  - for SPAN=16: 16(byte=1) OR 32(short=2) OR 64(int=4)
    //  - for SPAN=32: 32(byte=1) OR 64(short=2) OR 128(int=4)
    //  - for SPAN=64: 64(byte=1) OR 128(short=2) OR 256(int=4)
    // below, assume SPAN=16
    //
    // Node sizes:
    //  - total/each = 64 + children-elements
    //  - sharing cost = 4
    //    - cost for 1@byte = 80
    //    - cost for 2@byte = 84
    //    - cost for 3@byte = 88
    //    - cost for 1@short = 96
    //    - cost for 2@short = 100
    //    - cost for 3@short = 104
    //    - cost for 1@int = 128
    //    - cost for 2@int = 132
    //    - cost for 3@int = 136
    //  - 4-byte pointer-to-Node
    //    - 16-byte Node-header
    //    - 4-byte pointer-to-parentId
    //      - 16-byte Object-header
    //    - 4-byte pointer-to-children
    //      - 16-byte array-header
    //      - 4-byte length
    //      - children-elements
    // flat sizes with parentId:
    //  - total/each = 44 + children-elements
    //  - sharing cost = 8
    //    - cost for 1@byte = 60
    //    - cost for 2@byte = 68 -- best
    //    - cost for 3@byte = 76 -- best
    //    - cost for 1@short = 76
    //    - cost for 2@short = 84 -- best
    //    - cost for 3@short = 92 -- best
    //    - cost for 1@int = 108
    //    - cost for 2@int = 116 -- best
    //    - cost for 3@int = 124 -- best
    //  - 4-byte pointer-to-parentId
    //    - 16-byte Object-header
    //  - 4-byte pointer-to-children
    //    - 16-byte array-header
    //    - 4-byte length
    //    - children-elements
    // flat sizes without parentId:
    //  - total/each = 24 + children-elements
    //  - no sharing
    //    - cost for 1@byte = 40 -- best
    //    - cost for 2@byte = 80
    //    - cost for 3@byte = 120
    //    - cost for 1@short = 56 -- best
    //    - cost for 2@short = 112
    //    - cost for 3@short = 168
    //    - cost for 1@int = 88 -- best
    //    - cost for 2@int = 176
    //    - cost for 3@int = 264
    //  - 4-byte pointer-to-children
    //    - 16-byte array-header
    //    - 4-byte length
    //    - children-elements
    //
    // Sharing only saves when children are updated (added/removed mutates sizes, so cannot share)
    
    // current structure:
    //  - 16-byte Node header
    //  - 4-byte children pointer
    //    - 16-byte array header
    //    - 4-byte length
    //    - (4-256)-byte children pointers (1-64 4-byte pointers)
    //  - (2-8)-byte children ownership bitset (supports 16-64 children)
    //  - (circumstantial) 4-byte size table pointer
    //    - 16-byte array header
    //    - 4-byte length
    //    - (1-256)-byte sizes (1-64 (1-4)-byte sizes)
    //  - (maybe) (2-8)-byte children size tables ownership bitset (supports 16-64 children)
    //    - can't know whether children even have size tables to own
    
    // An alternative "pure Object[]" structure:
    //  - 16-byte array header
    //  - 4-byte length
    //  - (4-256)-bytes pointers to children (1-64 4-byte pointers)
    //  - (circumstantial) 4-byte pointer to ownership bitset
    //    - 16-byte Object header
    //    - (2-8)-byte value (supports 16-64 children)
    //  - (circumstantial) 4-byte pointer to size table
    //    - 16-byte array header
    //    - 4-byte length
    //    - (1-256)-bytes sizes (1-64 (1-4)-byte sizes)
    //
    // This appears to take up the same amount of space - just swapping whether children or bitset is nested.
    // Since the number of children may vary, it's not clear how we would distinguish the size table pointer from a child.
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // If I am not sized, none of my progeny are sized
}
