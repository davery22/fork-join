package io.avery.util;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class TrieForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E>, RandomAccess {
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
    static final int SHIFT = 5;
    static final int SPAN = 1 << SHIFT;
    static final int MASK = SPAN-1;
    static final int MARGIN = 2;
    static final int DO_NOT_REDISTRIBUTE = SPAN - MARGIN/2; // During rebalance(), a node with size >= threshold is not redistributed
    static final Node INITIAL_TAIL = new Node(new Object[SPAN]);
    static final Object INITIAL_FORK_ID = new Object();
    
    private Node root;
    private Node tail;
    private int size;
    private byte tailSize;
    private byte rootShift;
    private byte owns; // only need 2 bits - for root and tail
    
    // This id is copied by ListIterators and compared during ListIterator.set() to detect if a fork() has happened
    // (including a sublist fork()), which would conservatively invalidate ownership of nodes in the iterator stack.
    private Object forkId = INITIAL_FORK_ID;
    
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
    
    public TrieForkJoinList() {
        tail = INITIAL_TAIL;
    }
    
    public TrieForkJoinList(Collection<? extends E> c) {
        // Basically a tailored version of addAll()
        Object[] arr = c.toArray();
        int numNew = arr.length;
        if (numNew == 0) {
            tail = INITIAL_TAIL;
            return;
        }
        claimTail();
        if (numNew <= SPAN) {
            tail = new Node(arr.clone());
            size = tailSize = (byte) numNew;
            return;
        }
        claimRoot();
        root = new Node(Arrays.copyOf(arr, SPAN));
        int offset = numNew - SPAN > SPAN ? directAppend(arr, SPAN, AppendMode.NEVER_EMPTY_SRC) : SPAN;
        Object[] newTailChildren = new Object[SPAN];
        System.arraycopy(arr, offset, newTailChildren, 0, tailSize = (byte) (numNew - offset));
        tail = new Node(newTailChildren);
        size = numNew;
    }
    
    protected TrieForkJoinList(TrieForkJoinList<? extends E> toCopy) {
        size = toCopy.size;
        tailSize = toCopy.tailSize;
        rootShift = toCopy.rootShift;
        root = toCopy.root;
        tail = toCopy.tail;
    }
    
    
    // TODO: Verify that everything in the reversed() List makes sense / is fairly optimal.
    
    // Pros of custom iterator:
    //  - faster iteration - no need to traverse down on each advance
    //  - faster set - no need to traverse down each time
    // Cons of custom iterator:
    //  - iterator has more state + retains a strong reference to root (even after external co-mod), preventing GC
    
    
    @Override
    public boolean equals(Object o) {
        // Overridden from AbstractList just to add short-circuiting on size mismatch (when class matches exactly).
        // This is to somewhat level the playing field with ArrayList, which also does this optimization (but only
        // for itself - not other impls with known O(1) size()).
        if (o == this) {
            return true;
        }
        if (!(o instanceof List<?> list)) {
            return false;
        }
        if (o.getClass() == TrieForkJoinList.class && list.size() != size) {
            return false;
        }
        ListIterator<E> e1 = listIterator();
        ListIterator<?> e2 = list.listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            E o1 = e1.next();
            Object o2 = e2.next();
            if (!Objects.equals(o1, o2)) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }
    
    @Override
    public int hashCode() {
        var box = new Object(){ int hashCode = 1; };
        forEach(e -> box.hashCode = 31*box.hashCode + (e==null ? 0 : e.hashCode()));
        return box.hashCode;
    }
    
    @Override
    public void forEach(Consumer<? super E> action) {
        spliterator().forEachRemaining(action);
    }
    
    @Override
    public Iterator<E> iterator() {
        return new ListItr(0);
    }

    @Override
    public ListIterator<E> listIterator() {
        return new ListItr(0);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        rangeCheckForAdd(index);
        return new ListItr(index);
    }
    
    static class Frame {
        Node node; int offset;
        Frame(Node node, int offset) { this.node = node; this.offset = offset; }
    }
    
    // Inherited by ListItr and Splitr
    private abstract class ItrBase {
        Frame[] stack;
        Node leafNode;
        byte leafOffset;
        int expectedModCount;
        
        ItrBase(int expectedModCount) {
            this.expectedModCount = expectedModCount;
        }
        
        void init(int index) {
            int tailOffset = tailOffset();
            if (index >= tailOffset) {
                leafNode = tail;
                leafOffset = (byte) (index - tailOffset);
            }
            else if (rootShift == 0) {
                leafNode = root;
                leafOffset = (byte) index;
            }
            else {
                initStack(index);
            }
        }
        
        void initStack(int index) {
            assert rootShift > 0;
            
            Node curr = root;
            int shift = rootShift;
            stack = new Frame[shift / SHIFT];
            
            for (int i = stack.length-1; i >= 0; i--, shift -= SHIFT) {
                int childIdx = (int) (rShiftU(index, shift) & MASK);
                if (curr instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx != 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                stack[i] = new Frame(curr, childIdx);
                curr = (Node) curr.children[childIdx];
            }
            
            leafNode = curr;
            leafOffset = (byte) (index & MASK);
            resetDeepestOwned();
        }
        
        void checkForComodification() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }
        
        // Overridden by ListItr to reset the deepestOwned field (read by ListItr.set())
        void resetDeepestOwned() { }
    }
    
    private class ListItr extends ItrBase implements ListIterator<E> {
        int cursor;
        int lastRet = -1;
        byte deepestOwned = Byte.MAX_VALUE;
        Object expectedForkId = forkId;
        
        ListItr(int index) {
            super(modCount);
            cursor = index;
            init(index);
        }
        
        @Override
        void resetDeepestOwned() {
            deepestOwned = Byte.MAX_VALUE;
        }
        
        Node nextLeaf() {
            assert leafOffset == leafNode.children.length;
            assert cursor < tailOffset();
            
            int i = 0;
            Frame parent = stack[0];
            while (parent.offset == parent.node.children.length-1) {
                parent = stack[++i];
            }
            parent.offset++;
            boolean owned = i >= deepestOwned;
            byte newDeepestOwned = owned ? (byte) i : deepestOwned;
            while (i > 0) {
                if (owned && (owned = ((ParentNode) parent.node).owns(parent.offset))) {
                    newDeepestOwned--;
                }
                Frame child = stack[--i];
                child.node = (Node) parent.node.children[parent.offset];
                child.offset = 0;
                parent = child;
            }
            deepestOwned = (owned && ((ParentNode) parent.node).owns(parent.offset)) ? (byte) (newDeepestOwned-1) : newDeepestOwned;
            leafOffset = 0;
            return leafNode = (Node) parent.node.children[parent.offset];
        }
        
        Node prevLeaf() {
            assert leafOffset == 0;
            assert cursor > 0;
            
            int i = 0;
            Frame parent = stack[0];
            while (parent.offset == 0) {
                parent = stack[++i];
            }
            parent.offset--;
            boolean owned = i >= deepestOwned;
            byte newDeepestOwned = owned ? (byte) i : deepestOwned;
            while (i > 0) {
                if (owned && (owned = ((ParentNode) parent.node).owns(parent.offset))) {
                    newDeepestOwned--;
                }
                Frame child = stack[--i];
                child.node = (Node) parent.node.children[parent.offset];
                child.offset = child.node.children.length-1;
                parent = child;
            }
            deepestOwned = (owned && ((ParentNode) parent.node).owns(parent.offset)) ? (byte) (newDeepestOwned-1) : newDeepestOwned;
            leafNode = (Node) parent.node.children[parent.offset];
            leafOffset = (byte) leafNode.children.length;
            return leafNode;
        }
        
        @Override
        public boolean hasNext() {
            return cursor != size;
        }
        
        @Override
        public boolean hasPrevious() {
            return cursor != 0;
        }
        
        @Override
        public int nextIndex() {
            return cursor;
        }
        
        @Override
        public int previousIndex() {
            return cursor - 1;
        }
        
        @Override
        public E next() {
            checkForComodification();
            int i = cursor;
            if (i >= size) {
                throw new NoSuchElementException();
            }
            if (leafOffset == leafNode.children.length) {
                if (i == tailOffset()) {
                    if (stack != null) {
                        // Fixup in case we iterate backwards later
                        stack[0].offset++;
                    }
                    leafOffset = 0;
                    leafNode = tail;
                }
                else {
                    leafNode = nextLeaf();
                }
            }
            lastRet = i;
            cursor = i + 1;
            @SuppressWarnings("unchecked")
            E value = (E) leafNode.children[leafOffset++];
            return value;
        }
        
        @Override
        public E previous() {
            checkForComodification();
            int i = cursor - 1;
            if (i < 0) {
                throw new NoSuchElementException();
            }
            if (leafOffset == 0) {
                if (rootShift == 0) {
                    leafNode = root;
                    leafOffset = (byte) (i+1);
                }
                else if (stack == null) {
                    initStack(i);
                    leafOffset++;
                }
                else {
                    leafNode = prevLeaf();
                }
            }
            lastRet = i;
            cursor = i;
            @SuppressWarnings("unchecked")
            E value = (E) leafNode.children[--leafOffset];
            return value;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            int size = TrieForkJoinList.this.size;
            int i = cursor;
            if (i >= size) {
                return;
            }
            
            E[] leafChildren = (E[]) leafNode.children;
            int tailOffset = tailOffset(), j = leafOffset, theTailSize = tailSize,
                initialLeafSize = i >= tailOffset ? theTailSize : leafChildren.length;
            i -= j;
            while (j < initialLeafSize && modCount == expectedModCount) {
                action.accept(leafChildren[j++]);
            }
            Frame parent = stack != null ? stack[0] : null;
            while ((i += j) < tailOffset && modCount == expectedModCount) {
                // Stack is not null here - else consuming the first leaf would have moved us past tailOffset
                j = 0;
                while (parent.offset == parent.node.children.length-1) {
                    parent = stack[++j];
                }
                parent.offset++;
                while (j > 0) {
                    Frame child = stack[--j];
                    child.node = (Node) parent.node.children[parent.offset];
                    child.offset = 0;
                    parent = child;
                }
                leafChildren = (E[]) ((Node) (parent.node.children[parent.offset])).children;
                while (j < leafChildren.length && modCount == expectedModCount) {
                    action.accept(leafChildren[j++]);
                }
            }
            if (i < size) {
                if (parent != null) {
                    // Fixup in case we iterate backwards later
                    parent.offset++;
                    resetDeepestOwned();
                }
                j = leafOffset = 0;
                leafChildren = (E[]) (leafNode = tail).children;
                while (j < theTailSize && modCount == expectedModCount) {
                    action.accept(leafChildren[j++]);
                }
                i += j;
            }
            
            // Update once at end to reduce heap write traffic.
            cursor = i;
            lastRet = i - 1;
            checkForComodification();
        }
        
        // Overload used by subList ListIterator - takes an explicit end index
        @SuppressWarnings("unchecked")
        public void forEachRemaining(Consumer<? super E> action, int hi) {
            Objects.requireNonNull(action);
            int i = cursor;
            if (i >= hi) {
                return;
            }
            
            int tailFence = Math.min(tailOffset(), hi), j = leafOffset;
            E[] leafChildren = (E[]) leafNode.children;
            for (; j < leafChildren.length && i < hi && modCount == expectedModCount; i++) {
                action.accept(leafChildren[j++]);
            }
            Frame parent = stack != null ? stack[0] : null;
            while (i < tailFence && modCount == expectedModCount) {
                // Stack is not null here - else consuming the first leaf would have moved us past tailOffset
                j = 0;
                while (parent.offset == parent.node.children.length-1) {
                    parent = stack[++j];
                }
                parent.offset++;
                while (j > 0) {
                    Frame child = stack[--j];
                    child.node = (Node) parent.node.children[parent.offset];
                    child.offset = 0;
                    parent = child;
                }
                leafChildren = (E[]) ((Node) (parent.node.children[parent.offset])).children;
                for (; j < leafChildren.length && i < hi && modCount == expectedModCount; i++) {
                    action.accept(leafChildren[j++]);
                }
            }
            if (i < hi) {
                if (parent != null) {
                    // Fixup in case we iterate backwards later
                    parent.offset++;
                    resetDeepestOwned();
                }
                j = leafOffset = 0;
                leafChildren = (E[]) (leafNode = tail).children;
                for (; j < leafChildren.length && i < hi && modCount == expectedModCount; i++) {
                    action.accept(leafChildren[j++]);
                }
            }
            
            // Update once at end to reduce heap write traffic.
            cursor = i;
            lastRet = i - 1;
            checkForComodification();
        }
        
        @Override
        public void set(E e) {
            if (lastRet < 0) {
                throw new IllegalStateException();
            }
            checkForComodification();
            if (unsafeSet(e)) {
                expectedModCount = ++modCount;
            }
        }
        
        // Used by removeIf() and batchRemove(). This method skips checks and does not update modCount.
        // Returns true if any nodes needed to be copied (ie there was a "structural modification").
        private boolean unsafeSet(E e) {
            assert lastRet >= 0;
            
            int i = lastRet;
            int adj = i < cursor ? -1 : 0; // If lastRet < cursor (ie we were traversing forward), need to sub 1 from leaf offset
            Node oldLeafNode = leafNode;
            if (i >= tailOffset()) {
                leafNode = getEditableTail();
            }
            else if (stack == null) { // implies rootShift == 0
                leafNode = getEditableRoot();
            }
            else stackCopying: {
                int j;
                if (expectedForkId != forkId || (j = deepestOwned) == Byte.MAX_VALUE) {
                    // Either deepestOwned is unset (if this is our first time calling set(), or
                    // add()/remove()/forEachRemaining() invalidated it), or we can't trust it because
                    // part or all of the list was forked.
                    expectedForkId = forkId;
                    stack[j = stack.length-1].node = getEditableRoot();
                }
                else if (j == -1) {
                    break stackCopying;
                }
                for (; j > 0; j--) {
                    stack[j-1].node = stack[j].node.getEditableChild(stack[j].offset);
                }
                leafNode = stack[0].node.getEditableChild(stack[0].offset);
                deepestOwned = -1;
            }
            leafNode.children[leafOffset + adj] = e;
            return oldLeafNode != leafNode;
        }
        
        @Override
        public void add(E e) {
            checkForComodification();
            
            try {
                int i = cursor;
                TrieForkJoinList.this.add(i, e);
                cursor = i + 1;
                lastRet = -1;
                expectedModCount = modCount;
                init(cursor); // Possibly refresh stack
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }
        
        @Override
        public void remove() {
            if (lastRet < 0) {
                throw new IllegalStateException();
            }
            checkForComodification();
            
            try {
                TrieForkJoinList.this.remove(lastRet);
                cursor = lastRet;
                lastRet = -1;
                expectedModCount = modCount;
                init(cursor); // Possibly refresh stack
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }
    }
    
    class Splitr extends ItrBase implements Spliterator<E> {
        int index; // current index, modified on advance/split
        int fence; // -1 until used; then one past last index
        
        Splitr(int origin, int fence, int expectedModCount) {
            super(expectedModCount);
            this.index = origin;
            this.fence = fence;
        }
        
        int getFence() {
            int hi = fence;
            if (hi < 0) {
                expectedModCount = modCount;
                hi = fence = size;
            }
            return hi;
        }
        
        Node nextLeaf() {
            assert leafOffset == leafNode.children.length;
            assert index < tailOffset();
            
            int i = 0;
            Frame parent = stack[0];
            while (parent.offset == parent.node.children.length-1) {
                parent = stack[++i];
            }
            parent.offset++;
            while (i > 0) {
                Frame child = stack[--i];
                child.node = (Node) parent.node.children[parent.offset];
                child.offset = 0;
                parent = child;
            }
            leafOffset = 0;
            return leafNode = (Node) parent.node.children[parent.offset];
        }
        
        @Override
        public boolean tryAdvance(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            int hi = getFence(), i = index;
            if (i >= hi) {
                return false;
            }
            if (leafNode == null) {
                init(i);
            }
            if (leafOffset == leafNode.children.length) {
                if (i == tailOffset()) {
                    leafOffset = 0;
                    leafNode = tail;
                }
                else {
                    leafNode = nextLeaf();
                }
            }
            index = i + 1;
            @SuppressWarnings("unchecked")
            E value = (E) leafNode.children[leafOffset++];
            action.accept(value);
            checkForComodification();
            return true;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            int hi = getFence(), i = index;
            if (i >= hi) {
                return;
            }
            if (leafNode == null) {
                init(i);
            }
            
            int tailFence = Math.min(tailOffset(), hi), j = leafOffset;
            E[] leafChildren = (E[]) leafNode.children;
            for (; j < leafChildren.length && i < hi; i++) {
                action.accept(leafChildren[j++]);
            }
            Frame parent = stack != null ? stack[0] : null;
            while (i < tailFence) {
                // Stack is not null here - else consuming the first leaf would have moved us past tailOffset
                j = 0;
                while (parent.offset == parent.node.children.length-1) {
                    parent = stack[++j];
                }
                parent.offset++;
                while (j > 0) {
                    Frame child = stack[--j];
                    child.node = (Node) parent.node.children[parent.offset];
                    child.offset = 0;
                    parent = child;
                }
                leafChildren = (E[]) ((Node) (parent.node.children[parent.offset])).children;
                for (; j < leafChildren.length && i < hi; i++) {
                    action.accept(leafChildren[j++]);
                }
            }
            if (i < hi) {
                j = 0;
                leafChildren = (E[]) tail.children;
                for (; j < leafChildren.length && i < hi; i++) {
                    action.accept(leafChildren[j++]);
                }
            }
            
            index = hi;
            stack = null;
            leafNode = null;
            checkForComodification();
        }
        
        @Override
        public Spliterator<E> trySplit() {
            int hi = getFence(), lo = index, mid = (lo + hi) >>> 1;
            if (lo >= mid) {
                // Range too small
                return null;
            }
            // Going to split - invalidate current position
            stack = null;
            leafNode = null;
            return new Splitr(lo, index = mid, expectedModCount);
        }
        
        @Override
        public long estimateSize() {
            return getFence() - index;
        }
        
        @Override
        public int characteristics() {
            return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
        }
    }
    
    @Override
    public Spliterator<E> spliterator() {
        return new Splitr(0, -1, 0);
    }
    
    @Override
    public boolean addAll(Collection<? extends E> c) {
        Object[] arr = c.toArray();
        int numNew = arr.length;
        if (numNew == 0) {
            return false;
        }
        checkNewSize(size, numNew);
        modCount++;
        
        int oldTailSize = tailSize, offset = SPAN - oldTailSize;
        if (numNew <= offset) {
            Node oldTail = getEditableTail();
            System.arraycopy(arr, 0, oldTail.children, oldTailSize, numNew);
            tailSize += numNew;
            size += numNew;
            return true;
        }
        if (offset != 0) {
            Node oldTail = getEditableTail();
            System.arraycopy(arr, 0, oldTail.children, oldTailSize, offset);
            tailSize += offset;
            size += offset;
        }
        pushDownTail();
        int remaining = numNew - offset;
        if (remaining > SPAN) {
            offset = directAppend(arr, offset, AppendMode.NEVER_EMPTY_SRC);
        }
        claimTail();
        Object[] newTailChildren = new Object[SPAN];
        System.arraycopy(arr, offset, newTailChildren, 0, tailSize = (byte) (numNew - offset));
        tail = new Node(newTailChildren);
        size += remaining;
        return true;
    }
    
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        if (index == size) {
            return addAll(c);
        }
        rangeCheckForAdd(index);
        Object[] arr = c.toArray();
        int numNew = arr.length;
        if (numNew == 0) {
            return false;
        }
        checkNewSize(size, numNew);
        modCount++;
        
        int tailOffset = tailOffset();
        if (index >= tailOffset) {
            int tailIdx = index - tailOffset, newTailSize = tailSize + numNew;
            Node leftTail = getEditableTail();
            if (newTailSize <= SPAN) {
                // New elements fit in left tail
                System.arraycopy(leftTail.children, tailIdx, leftTail.children, tailIdx + numNew, tailSize - tailIdx);
                System.arraycopy(arr, 0, leftTail.children, tailIdx, numNew);
                size += numNew;
                tailSize = (byte) newTailSize;
                return true;
            }
            
            // Fixup left tail and push down, append elements, make a new tail from trailing elements and old tail suffix.
            int oldSize = size;
            int spaceLeft = SPAN - tailIdx;
            int takeRight = Math.min(numNew, spaceLeft);
            int takeLeft = spaceLeft - takeRight;
            int remainingRight = numNew - takeRight;
            int remainingLeft = tailSize - tailIdx - takeLeft;
            Object[] newChildren = new Object[SPAN];
            System.arraycopy(leftTail.children, tailIdx + takeLeft, newChildren, 0, remainingLeft); // Re-home non-retained left suffix
            System.arraycopy(leftTail.children, tailIdx, leftTail.children, tailIdx + takeRight, takeLeft); // Shift over retained left suffix (if any)
            System.arraycopy(arr, 0, leftTail.children, tailIdx, takeRight); // Insert right prefix
            size += SPAN - tailSize;
            tailSize = SPAN;
            pushDownTail();
            
            if (remainingRight > SPAN) {
                takeRight = directAppend(arr, takeRight, AppendMode.EMPTY_SRC_TO_FILL);
                remainingRight = numNew - takeRight;
            }
            
            newTailSize = remainingRight + remainingLeft;
            if (newTailSize <= SPAN) {
                System.arraycopy(newChildren, 0, newChildren, remainingRight, remainingLeft);
                System.arraycopy(arr, takeRight, newChildren, 0, remainingRight);
                tail = new Node(newChildren);
                size = oldSize + numNew;
                tailSize = (byte) newTailSize;
                return true;
            }
            
            Object[] newChildren2 = new Object[SPAN];
            takeLeft = SPAN - remainingRight;
            remainingLeft -= takeLeft;
            System.arraycopy(newChildren, takeLeft, newChildren2, 0, remainingLeft); // Re-home non-retained left suffix (again)
            System.arraycopy(newChildren, 0, newChildren, remainingRight, takeLeft); // Shift over retained left suffix (if any)
            System.arraycopy(arr, takeRight, newChildren, 0, remainingRight); // Insert right prefix
            tail = new Node(newChildren);
            size = oldSize + numNew - remainingLeft;
            tailSize = SPAN;
            pushDownTail();
            tail = new Node(newChildren2);
            size += remainingLeft;
            tailSize = (byte) remainingLeft;
            return true;
        }
        
        if (index == 0) {
            // Prepending
            Node rightRoot;
            int rightRootShift;
            if (numNew <= SPAN) {
                rightRoot = new Node(arr.clone());
                rightRootShift = 0;
            }
            else {
                TrieForkJoinList<E> right = new TrieForkJoinList<>();
                right.root = new Node(Arrays.copyOf(arr, SPAN));
                right.directAppend(arr, SPAN, AppendMode.ALWAYS_EMPTY_SRC);
                rightRoot = right.root;
                rightRootShift = right.rootShift;
            }
            size += numNew;
            Node[] nodes = { rightRoot, getEditableRoot() };
            concatSubTree(nodes, rightRootShift, rootShift);
            return true;
        }
        
        // Split root into left and right
        Node[] splitRoots = { getEditableRoot(), getEditableRoot() };
        int[] rootShifts = new int[2];
        splitRec(index-1, index, splitRoots, rootShifts, true, true, rootShift);
        Node rightRoot = splitRoots[1];
        
        // Append elements onto left (using an auxiliary list around the left split node)
        TrieForkJoinList<E> left = new TrieForkJoinList<>();
        left.claimRoot();
        left.rootShift = (byte) rootShifts[0];
        left.root = splitRoots[0];
        left.directAppend(arr, 0, AppendMode.ALWAYS_EMPTY_SRC);
        
        // Concat right onto left
        size += numNew;
        int newRootShift = left.rootShift;
        splitRoots[0] = left.root;
        splitRoots[1] = rightRoot;
        concatSubTree(splitRoots, newRootShift, rootShifts[1]);
        return true;
    }
    
    @Override
    public Object[] toArray() {
        return toArray(new Object[size], 0, size);
    }
    
    @Override
    public <T> T[] toArray(T[] a) {
        return toArray(a, 0, size);
    }
    
    @SuppressWarnings("unchecked")
    private <T> T[] toArray(T[] a, int fromIndex, int toIndex) {
        int range = toIndex - fromIndex;
        if (a.length < range) {
            a = (T[]) Array.newInstance(a.getClass().componentType(), range);
        }
        
        int tailOffset = tailOffset();
        if (fromIndex >= tailOffset) {
            // Starts and ends in the tail
            System.arraycopy(tail.children, fromIndex - tailOffset, a, 0, range);
        }
        else if (rootShift == 0) {
            if (toIndex <= tailOffset) {
                // Starts and ends in the root
                System.arraycopy(root.children, fromIndex, a, 0, range);
            }
            else {
                // Starts in the root, ends in the tail
                int len = root.children.length;
                System.arraycopy(root.children, fromIndex, a, 0, len);
                System.arraycopy(tail.children, 0, a, len, range - len);
            }
        }
        else {
            // Starts in the root...
            Node curr = root;
            int shift = rootShift, index = fromIndex;
            Frame[] stack = new Frame[shift / SHIFT];
            
            for (int i = stack.length-1; i >= 0; i--, shift -= SHIFT) {
                int childIdx = (int) (rShiftU(index, shift) & MASK);
                if (curr instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                stack[i] = new Frame(curr, childIdx);
                curr = (Node) curr.children[childIdx];
            }
            
            index &= MASK;
            int i = 0, offset = 0, copyLen;
            Frame parent = stack[0];
            
            if (toIndex < tailOffset) {
                // ...Ends in the root
                System.arraycopy(curr.children, index, a, offset, copyLen = Math.min(range - offset, curr.children.length - index));
                while ((offset += copyLen) != range) {
                    while (++parent.offset == parent.node.children.length) {
                        parent = stack[++i];
                    }
                    while (i > 0) {
                        Frame child = stack[--i];
                        child.node = (Node) parent.node.children[parent.offset];
                        child.offset = 0;
                        parent = child;
                    }
                    curr = (Node) parent.node.children[parent.offset];
                    System.arraycopy(curr.children, 0, a, offset, copyLen = Math.min(range - offset, curr.children.length));
                }
            }
            else {
                // ...Ends at or in the tail
                int fence = range - (toIndex - tailOffset);
                System.arraycopy(curr.children, index, a, offset, copyLen = curr.children.length - index);
                while ((offset += copyLen) != fence) {
                    while (++parent.offset == parent.node.children.length) {
                        parent = stack[++i];
                    }
                    while (i > 0) {
                        Frame child = stack[--i];
                        child.node = (Node) parent.node.children[parent.offset];
                        child.offset = 0;
                        parent = child;
                    }
                    curr = (Node) parent.node.children[parent.offset];
                    System.arraycopy(curr.children, 0, a, offset, copyLen = curr.children.length);
                }
                System.arraycopy(tail.children, 0, a, offset, range - fence);
            }
        }
        
        if (a.length > range) {
            a[range] = null;
        }
        return a;
    }
    
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
                int childIdx = (int) (rShiftU(index, shift) & MASK);
                if (leaf instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                leaf = (Node) leaf.children[childIdx];
            }
            index &= MASK;
        }
        
        @SuppressWarnings("unchecked")
        E value = (E) leaf.children[index];
        return value;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public E getFirst() {
        int ts = tailSize;
        if (ts == 0) {
            throw new NoSuchElementException();
        }
        if (ts == size) {
            return (E) tail.children[0];
        }
        Node leaf = root;
        for (int shift = rootShift; shift > 0; shift -= SHIFT) {
            leaf = (Node) leaf.children[0];
        }
        return (E) leaf.children[0];
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public E getLast() {
        int ts = tailSize;
        if (ts == 0) {
            throw new NoSuchElementException();
        }
        return (E) tail.children[ts-1];
    }
    
    @Override
    public E set(int index, E element) {
        Objects.checkIndex(index, size);
        int tailOffset = tailOffset();
        Object oldLeaf;
        Node leaf;
        
        if (index >= tailOffset) {
            oldLeaf = tail;
            leaf = getEditableTail();
            index -= tailOffset;
        }
        else {
            oldLeaf = root;
            leaf = getEditableRoot();
            for (int shift = rootShift; shift > 0; shift -= SHIFT) {
                int childIdx = (int) (rShiftU(index, shift) & MASK);
                if (leaf instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                oldLeaf = leaf.children[childIdx];
                leaf = leaf.getEditableChild(childIdx);
            }
            index &= MASK;
        }
        
        if (oldLeaf != leaf) {
            modCount++;
        }
        
        @SuppressWarnings("unchecked")
        E old = (E) leaf.children[index];
        leaf.children[index] = element;
        return old;
    }
    
    @Override
    public boolean add(E e) {
        checkNewSize(size, 1);
        modCount++;
        addToTail(e);
        return true;
    }
    
    @Override
    public void add(int index, E element) {
        rangeCheckForAdd(index);
        checkNewSize(size, 1);
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
                return;
            }
            
            Object lastElement = oldTail.children[SPAN-1];
            System.arraycopy(oldTail.children, tailIdx, oldTail.children, tailIdx+1, SPAN-tailIdx-1);
            oldTail.children[tailIdx] = element;
            pushDownTail();
            (tail = new Node(new Object[SPAN])).children[0] = lastElement;
            tailSize = 1;
            size++;
            return;
        }
        
        if (index == 0) {
            // Concat root onto element
            size++;
            Node[] nodes = { new Node(new Object[]{ element }), getEditableRoot() };
            concatSubTree(nodes, 0, rootShift);
            return;
        }
        
        // Split root into left and right
        Node[] splitRoots = { getEditableRoot(), getEditableRoot() };
        int[] rootShifts = new int[2];
        splitRec(index-1, index, splitRoots, rootShifts, true, true, rootShift);
        Node rightRoot = splitRoots[1];
        
        // Append element onto left (using an auxiliary list around the left split node)
        TrieForkJoinList<E> left = new TrieForkJoinList<>();
        left.claimRoot();
        left.rootShift = (byte) rootShifts[0];
        left.root = splitRoots[0];
        left.directAppend(new Object[]{ element }, 0, AppendMode.ALWAYS_EMPTY_SRC);
        
        // Concat element onto left
//        splitRoots[1] = new Node(new Object[]{ element });
//        concatSubTree(splitRoots, rootShifts[0], 0);
        
        // Concat right onto left
        size++;
//        int newRootShift = rootShift;
//        splitRoots[0] = root;
        int newRootShift = left.rootShift;
        splitRoots[0] = left.root;
        splitRoots[1] = rightRoot;
        concatSubTree(splitRoots, newRootShift, rootShifts[1]);
    }
    
    @Override
    public void clear() {
        modCount++;
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
        
        E old = get(index);
        size--;
        if (tailOffset == 1) {
            root = null;
            rootShift = 0;
        }
        else if (index == tailOffset-1) {
            root = forkPrefixRec(index-1, this, getEditableRoot(), rootShift, true, true);
        }
        else if (index == 0) {
            root = forkSuffixRec(index+1, this, getEditableRoot(), rootShift, true, true);
        }
        else {
            Node[] splitRoots = { getEditableRoot(), getEditableRoot() };
            int[] rootShifts = new int[2];
            splitRec(index-1, index+1, splitRoots, rootShifts, true, true, rootShift);
            concatSubTree(splitRoots, rootShifts[0], rootShifts[1]);
        }
        return old;
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
            size = tailSize = 0;
        }
        else {
            size--;
            pullUpTail();
        }
        
        return old;
    }
    
    // Read size, tailSize
    // Updates tail, tailSize
    // May update root, rootShift
    private void pullUpTail() {
        int oldRootShift = rootShift;
        if (oldRootShift == 0) {
            claimOrDisownTail(ownsRoot());
            tailSize = (byte) (tail = root).children.length;
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
                tailSize = (byte) (tail = (Node) node.children[lastIdx]).children.length;
                break;
            }
            node = (ParentNode) node.children[lastIdx];
        }
        
        // Second pass
        if (deepestNonEmptyAncestorShift == oldRootShift) {
            int len = oldRoot.children.length;
            if (len == 2) {
                // Removing second child - Replace root with its first child.
                // If that child has one child (due to forkRange() or removeRange() leaving a thin path),
                // then repeat until there is more than one child, or we hit leaf-level.
                Node newRoot = oldRoot;
                boolean owns = true;
                do {
                    owns = owns && ((ParentNode) newRoot).owns(0);
                    newRoot = (Node) newRoot.children[0];
                } while ((oldRootShift -= SHIFT) > 0 && newRoot.children.length == 1);
                root = newRoot;
                rootShift = (byte) oldRootShift;
                if (!owns) {
                    disownRoot();
                }
                preventNonFullLeafRoot();
            }
            else {
                // len == 1 is impossible, because it would imply that we had a single leaf node under root,
                // and no operation leaves the tree in that state (the leaf node would be the root, not under it).
                assert len > 2;
                getEditableRoot(len-1);
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
        
        pushDownTail();
        claimTail();
        (tail = new Node(new Object[SPAN])).children[0] = e;
        tailSize = 1;
        size++;
    }
    
    // Reads root, rootShift, size, tailSize, tail
    // May update root, rootShift
    private void pushDownTail() {
        if (root == null) {
            claimOrDisownRoot(ownsTail());
            root = tail;
            return;
        }
        
        // Two top-down passes:
        //  1. Find the deepest ancestor that will be non-full after tail push-down
        //  2. Extend that deepest ancestor with a path to the tail (which requires claiming it, and thus the path to it)
        
        // First pass
        Node oldRoot = root, node = oldRoot;
        int oldRootShift = rootShift, deepestNonFullAncestorShift = oldRootShift + SHIFT;
        for (int shift = oldRootShift; shift > 0; shift -= SHIFT) {
            int len = node.children.length;
            if (len < SPAN) {
                deepestNonFullAncestorShift = shift;
            }
            node = (Node) node.children[len-1];
        }
        
        // Second pass
        int oldTailSize = tailSize;
        if (deepestNonFullAncestorShift > oldRootShift) {
            // No existing non-full ancestors - need to make a new root above old root
            ParentNode newRoot;
            Object[] children = { oldRoot, null };
            if (oldRoot instanceof SizedParentNode || size - oldTailSize < SPAN) {
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
            rootShift = (byte) deepestNonFullAncestorShift;
        }
        else if (deepestNonFullAncestorShift == oldRootShift) {
            // Claim root - it is the deepest non-full ancestor
            int len = oldRoot.children.length;
            node = getEditableRoot(len+1);
            if (node instanceof SizedParentNode sn) {
                Sizes sizes = sn.sizes();
                sizes.set(len, sizes.get(len-1) + oldTailSize);
            }
        }
        else {
            // Claim nodes up to the deepest non-full ancestor
            node = getEditableRoot();
            int lastIdx = node.children.length-1;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedParentNode sn) {
                    sn.sizes().inc(lastIdx, oldTailSize);
                }
                int childLen = ((Node) node.children[lastIdx]).children.length;
                if (shift == deepestNonFullAncestorShift + SHIFT) {
                    node = node.getEditableChild(lastIdx, childLen+1);
                    if (node instanceof SizedParentNode sn) {
                        Sizes sizes = sn.sizes();
                        sizes.set(childLen, sizes.get(childLen-1) + oldTailSize);
                    }
                    break;
                }
                node = node.getEditableChild(lastIdx);
                lastIdx = childLen-1;
            }
        }
        
        // Finish second pass: Extend a path (of the appropriate height) from the deepest non-full ancestor to tail
        ParentNode parent = (ParentNode) node;
        int lastIdx = parent.children.length-1;
        for (int shift = deepestNonFullAncestorShift; shift > SHIFT; shift -= SHIFT) {
            parent.claim(lastIdx);
            parent.children[lastIdx] = parent = new ParentNode(new Object[1]);
            lastIdx = 0;
        }
        parent.claimOrDisown(lastIdx, ownsTail());
        parent.children[lastIdx] = tail;
    }
    
    enum AppendMode {
        // Used when inserting into the root
        // Need not fill rightmost leaf (subsequent concatenation will fixup sizes)
        ALWAYS_EMPTY_SRC,
        
        // Used when inserting into the tail, but not at the end
        // Must fill rightmost leaf, but need not preserve any elements for tail
        EMPTY_SRC_TO_FILL,
        
        // Used when inserting at the end
        // Must fill rightmost leaf and preserve at least one element for tail
        NEVER_EMPTY_SRC
    }
    
    // Services add(i,e), addAll(i,es), addAll(es)
    // Reads root, rootShift
    // May update root, rootShift
    private int directAppend(Object[] src, int offset, AppendMode mode) {
        // If root is null, we should fill up and push down a tail before calling this method.
        assert root != null;
        
        // Find the deepest non-full node - we will acquire ownership down to that point.
        // Also keep track of remaining space, needed later to pre-compute node lengths, size entries, and tree height.
        Node oldRoot = root, node = oldRoot;
        int oldRootShift = rootShift, shift = oldRootShift, deepestNonFullShift = oldRootShift + SHIFT;
        long remainingSpace = 0;
        for (; shift > 0; shift -= SHIFT) {
            int len = node.children.length;
            if (len != SPAN) {
                deepestNonFullShift = shift;
                remainingSpace += lShift(SPAN - len, shift);
            }
            node = (Node) node.children[len-1];
        }
        int firstLeafSpace = SPAN - node.children.length;
        if (firstLeafSpace != 0) {
            deepestNonFullShift = 0;
            remainingSpace += firstLeafSpace;
        }
        
        // Figure out how many elements we are actually adding.
        // Depends on the mode, and how many elements are left after filling the first rightmost leaf.
        int numElements = src.length - offset, firstFill = Math.min(numElements, firstLeafSpace);
        numElements -= firstFill;
        int toIndex = switch (mode) {
            case ALWAYS_EMPTY_SRC -> src.length;
            case EMPTY_SRC_TO_FILL -> src.length - (numElements & MASK);
            case NEVER_EMPTY_SRC -> src.length - 1 - ((numElements-1) & MASK);
        };
        numElements = toIndex - offset;
        
        // Initialize stack of nodes, which will be used at the end to
        // add elements from the bottom of the tree, in node-sized chunks.
        // We increment the height above the current root until the tree has capacity for all elements.
        shift = oldRootShift;
        int initialHeight = oldRootShift / SHIFT, height = initialHeight;
        for (long remSpace = remainingSpace; remSpace < numElements; ) {
            height++;
            shift += SHIFT;
            remSpace += lShift(SPAN-1, shift);
        }
        Frame[] stack = new Frame[height];
        
        // Populate the stack by copying the path to the current rightmost leaf node.
        // Along the way, we take ownership of nodes (down to the deepest non-full node),
        // increase their capacity to fit remaining elements, and pre-populate sizes as needed.
        node = oldRoot;
        shift = oldRootShift;
        int j = initialHeight-1, oldLen = oldRoot.children.length;
        if (shift >= deepestNonFullShift) {
            long remainingSpaceUnder = remainingSpace - lShift(SPAN - oldLen, shift);
            long remainingElements = Math.max(0, numElements - remainingSpaceUnder);
            int newLen = Math.min(SPAN, oldLen + 1 + (int) rShift(remainingElements-1, shift)); // Sign-extending shift, so -1 stays -1
            node = oldRoot = root = getEditableRoot(newLen);
            
            while (shift > 0) {
                stack[j--] = new Frame(node, oldLen-1);
                if (node instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes();
                    int oldLastSize = sizes.get(oldLen-1);
                    sizes.set(oldLen-1, oldLastSize + (int) Math.min(numElements, remainingSpaceUnder));
                    if (oldLen != newLen) {
                        long newLastSize = sizes.fill(oldLen, newLen-1, shift);
                        sizes.set(newLen-1, (int) Math.min(oldLastSize + numElements, newLastSize + (1L << shift)));
                    }
                }
                if ((shift -= SHIFT) < deepestNonFullShift) {
                    oldLen = (node = (Node) node.children[oldLen-1]).children.length;
                    break;
                }
                int oldChildLen = ((Node) node.children[oldLen-1]).children.length;
                remainingSpaceUnder -= lShift(SPAN - oldChildLen, shift);
                remainingElements = Math.max(0, numElements - remainingSpaceUnder);
                int newChildLen = Math.min(SPAN, oldChildLen + 1 + (int) rShift(remainingElements-1, shift)); // Sign-extending shift, so -1 stays -1
                node = node.getEditableChild(oldLen-1, newChildLen);
                oldLen = oldChildLen;
                newLen = newChildLen;
            }
        }
        while (shift > 0) {
            stack[j--] = new Frame(node, oldLen-1);
            shift -= SHIFT;
            oldLen = (node = (Node) node.children[oldLen-1]).children.length;
        }
        
        // Fill the current rightmost leaf node.
        // This may not do anything (if already full), or it may be the only thing we do (if insufficient elements to fill).
        System.arraycopy(src, offset, node.children, oldLen, firstFill);
        if ((offset += firstFill) == toIndex) {
            return toIndex;
        }
        
        // Add new nodes above current root if needed to fit all elements.
        if (initialHeight != height) {
            // Slight optimization of getSizeIfNeedsSizedParent(), since at this point
            // we would have isRightmost == true and (shift != 0 or len == SPAN)
            int childSize = (node = oldRoot) instanceof SizedParentNode sn ? sn.sizes().get(sn.children.length-1) : -1;
            long remainingSpaceUnder = remainingSpace;
            long remainingElements = numElements - remainingSpaceUnder;
            boolean owned = claimRoot();
            shift = oldRootShift;
            
            for (int i = initialHeight; i < height; i++) {
                shift += SHIFT;
                int len = Math.min(SPAN, 2 + (int) rShift(remainingElements-1, shift));
                ParentNode parent;
                if (childSize != -1) {
                    Sizes sizes = Sizes.of(shift, len);
                    sizes.set(0, childSize);
                    if (len != 1) {
                        long newLastSize = sizes.fill(1, len-1, shift);
                        sizes.set(len-1, childSize = (int) Math.min(childSize + remainingElements, newLastSize + (1L << shift)));
                    }
                    parent = new SizedParentNode(new Object[len], sizes, true);
                }
                else {
                    parent = new ParentNode(new Object[len], true);
                }
                parent.children[0] = node;
                parent.claimOrDisown(0, owned);
                owned = true;
                remainingSpaceUnder += lShift(SPAN-1, shift);
                remainingElements = numElements - remainingSpaceUnder;
                stack[i] = new Frame(node = parent, 0);
            }
            
            root = node;
            rootShift = (byte) shift;
        }
        
        // Finally, fill up the tree from the bottom, in node-sized chunks.
        numElements -= firstFill;
        Frame parent = stack[0];
        int i = 0, childShift = 0;
        while (offset != toIndex) {
            for (; ++parent.offset == SPAN; childShift += SHIFT) {
                parent = stack[++i];
            }
            for (; i > 0; childShift -= SHIFT) {
                int newChildSize = Math.min(SPAN, 1 + (int) rShiftU(numElements-1, childShift));
                Frame child = stack[--i];
                parent.node.children[parent.offset] = child.node = new ParentNode(new Object[newChildSize], true);
                child.offset = 0;
                parent = child;
            }
            parent.node.children[parent.offset] = new Node(Arrays.copyOfRange(src, offset, offset += Math.min(SPAN, numElements)));
            numElements -= SPAN;
        }
        
        return toIndex;
    }
    
    @Override
    public int size() {
        return size;
    }
    
    @Override
    public ForkJoinList<E> fork() {
        // Invalidate stack ownership for any existing ListIterators
        forkId = new Object();
        // Disown root/tail, to force path-copying upon future mutations
        owns = 0;
        return new TrieForkJoinList<>(this);
    }
    
    // A tiny bit set implementation
    
    private static long[] nBits(int n) {
        return new long[((n - 1) >> 6) + 1];
    }
    private static void setBit(long[] bits, int i) {
        bits[i >> 6] |= 1L << i;
    }
    private static boolean isClear(long[] bits, int i) {
        return (bits[i >> 6] & (1L << i)) == 0;
    }
    
    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        return removeIf(filter, 0, size);
    }
    
    boolean removeIf(Predicate<? super E> filter, int i, int end) {
        Objects.requireNonNull(filter);
        int expectedModCount = modCount;
        ListItr right = new ListItr(i);
        // Optimize for initial run of survivors
        while (i < end && !filter.test(right.next())) {
            i++;
        }
        // Tolerate predicates that reentrantly access the collection for
        // read (but writers still get CME), so traverse once to find
        // elements to delete, a second pass to physically expunge.
        if (i < end) {
            int start = i;
            long[] deathRow = nBits(end - start);
            deathRow[0] = 1L;
            for (i = start + 1; i < end; i++) {
                if (filter.test(right.next())) {
                    setBit(deathRow, i - start);
                }
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            right = new ListItr(start);
            ListItr left = new ListItr(start);
            int w = start;
            for (i = start; i < end; i++) {
                E e = right.next();
                if (isClear(deathRow, i - start)) {
                    left.next();
                    left.unsafeSet(e);
                    w++;
                }
            }
            removeRange(w, end);
            return true;
        }
        if (modCount != expectedModCount) {
            throw new ConcurrentModificationException();
        }
        return false;
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        return batchRemove(c, false, 0, size);
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        return batchRemove(c, true, 0, size);
    }
    
    boolean batchRemove(Collection<?> c, boolean complement, int from, int end) {
        Objects.requireNonNull(c);
        ListItr right = new ListItr(from);
        int r;
        // Optimize for initial run of survivors
        for (r = from;; r++) {
            if (r == end) {
                return false;
            }
            if (c.contains(right.next()) != complement) {
                break;
            }
        }
        ListItr left = new ListItr(r);
        int w = r++;
        try {
            for (E e; r < end; r++) {
                if (c.contains(e = right.next()) == complement) {
                    left.next();
                    left.unsafeSet(e);
                    w++;
                }
            }
        } finally {
            // Preserve behavioral compatibility with AbstractCollection even if c.contains() throws.
            // Remove the gap and increment modCount by number of removed elements.
            modCount += r - w - 1; // Extra -1 to negate the +1 inside removeRange
            removeRange(w, r);
        }
        return true;
    }
    
    // Called at the end of forkRange(), removeRange(), pullUpTail() to empty or fill a non-full leaf root.
    // This is not necessary for correctness, but prevents needing size tables upon subsequent additions.
    private void preventNonFullLeafRoot() {
        if (rootShift == 0 && root != null) {
            int tailOffset = tailOffset();
            if (size <= SPAN) {
                // Root fits in tail - Empty it into tail
                Node newTail = getEditableTail();
                System.arraycopy(newTail.children, 0, newTail.children, tailOffset, tailSize);
                System.arraycopy(root.children, 0, newTail.children, 0, tailOffset);
                tailSize = (byte) size;
                root = null;
            }
            else if (tailOffset < SPAN) {
                // Root is not full - Fill it from tail
                int rootSpace = SPAN - tailOffset, oldTailSize = tailSize, newTailSize = oldTailSize - rootSpace;
                Node newTail = getEditableTail();
                Node newRoot = getEditableRoot(SPAN);
                System.arraycopy(newTail.children, 0, newRoot.children, tailOffset, rootSpace);
                System.arraycopy(newTail.children, rootSpace, newTail.children, 0, newTailSize);
                Arrays.fill(newTail.children, tailSize = (byte) newTailSize, oldTailSize, null);
            }
        }
    }
    
    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        // In-place dual of forkRange()
        
        if (fromIndex < 0 || fromIndex > toIndex || toIndex > size) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(fromIndex, toIndex));
        }
        if (fromIndex == toIndex) {
            return;
        }
        if (fromIndex == 0 && toIndex == size) {
            clear();
            return;
        }
        modCount++;
        
        int tailOffset = tailOffset(), newSize = size -= (toIndex - fromIndex);
        if (toIndex >= tailOffset) {
            if (fromIndex == 0) {
                // Remove all of root
                rootShift = 0;
                root = null;
                int oldTailSize = tailSize;
                if (newSize != oldTailSize) {
                    // Remove prefix of tail
                    Node newTail = getEditableTail();
                    System.arraycopy(newTail.children, oldTailSize - newSize, newTail.children, 0, newSize);
                    Arrays.fill(newTail.children, tailSize = (byte) newSize, oldTailSize, null);
                }
                return;
            }
            
            int newTailOffset = tailOffset;
            if (fromIndex < tailOffset) {
                // Remove suffix of root
                newTailOffset = fromIndex;
                root = forkPrefixRec(fromIndex - 1, this, getEditableRoot(), rootShift, true, true);
            }

            if (newSize == newTailOffset) {
                // Remove all of tail
                pullUpTail();
            }
            else if (toIndex != tailOffset) {
                // Remove range in tail
                int oldTailSize = tailSize;
                Node newTail = getEditableTail();
                System.arraycopy(newTail.children, toIndex - tailOffset, newTail.children, fromIndex - newTailOffset, newSize - fromIndex);
                Arrays.fill(newTail.children, tailSize = (byte) (newSize - newTailOffset), oldTailSize, null);
            }
        }
        else if (fromIndex == 0) {
            // Remove prefix of root
            root = forkSuffixRec(toIndex, this, getEditableRoot(), rootShift, true, true);
        }
        else {
            // Remove range in root
            
            // NOTE: We may be leaving some performance on the table, in exchange for sanity.
            //  We could implement a direct removeRange() that rebalances across the gap on the way up. I tried and ended up
            //  with three additional variants of rebalance(), along with some new node-copying variants, and plenty of glue
            //  code. Instead, I'm using the fact that removeRange = join(prefix, suffix), and optimizing to retain
            //  ownership of the prefix and suffix instead of forking.
            
            int[] rootShifts = new int[2];
            Node[] splitRoots = { getEditableRoot(), getEditableRoot() };
            splitRec(fromIndex-1, toIndex, splitRoots, rootShifts, true, true, rootShift);
            concatSubTree(splitRoots, rootShifts[0], rootShifts[1]);
        }
        
        preventNonFullLeafRoot();
    }
    
    private TrieForkJoinList<E> forkRange(int fromIndex, int toIndex) {
        // Only called by subList.fork(), whose bounds are already checked - no additional bounds checking needed.
        
        // Invalidate stack ownership for any existing ListIterators
        forkId = new Object();
        
        if (fromIndex == toIndex) {
            return new TrieForkJoinList<>();
        }
        if (fromIndex == 0 && toIndex == size) {
            owns = 0;
            return new TrieForkJoinList<>(this);
        }
        
        TrieForkJoinList<E> newList = new TrieForkJoinList<>();
        int tailOffset = tailOffset(), newSize = newList.size = toIndex - fromIndex;
        if (toIndex >= tailOffset) {
            if (fromIndex >= tailOffset) {
                // Starts and ends in the tail
                newList.claimTail();
                Object[] newTailChildren = new Object[SPAN];
                System.arraycopy(tail.children, fromIndex - tailOffset, newTailChildren, 0, newList.tailSize = (byte) newSize);
                newList.tail = new Node(newTailChildren);
                return newList;
            }
            
            if (fromIndex == 0) {
                // Starts before the root...
                disownRoot();
                newList.rootShift = rootShift;
                newList.root = root;
            }
            else {
                // Starts in the root...
                newList.root = forkSuffixRec(fromIndex, newList, root, rootShift, true, false);
            }
            
            if (toIndex == tailOffset) {
                // ...Ends at the tail
                newList.pullUpTail();
            }
            else {
                // ...Ends in the tail
                newList.claimTail();
                Object[] newTailChildren = new Object[SPAN];
                System.arraycopy(tail.children, 0, newTailChildren, 0, newList.tailSize = (byte) (toIndex - tailOffset));
                newList.tail = new Node(newTailChildren);
            }
        }
        else if (fromIndex == 0) {
            // Starts and ends in the root
            newList.root = forkPrefixRec(toIndex - 1, newList, root, rootShift, true, false);
            newList.pullUpTail();
        }
        else {
            // Starts and ends in the root
            newList.root = forkRangeRec(fromIndex, toIndex-1, newList, root, rootShift);
            newList.pullUpTail();
        }
        
        newList.preventNonFullLeafRoot();
        return newList;
    }
    
    private static <E> Node forkPrefixRec(int toIndex, TrieForkJoinList<E> newList, Node node, int shift, boolean isLeftmost, boolean isOwned) {
        int childIdx = (int) (rShiftU(toIndex, shift) & MASK);
        if (shift == 0) {
            newList.rootShift = (byte) shift;
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
        newList.rootShift = (byte) shift;
        return ((ParentNode) node).copyPrefix(isOwned, childIdx, rightNode, shift);
    }
    
    private static <E> Node forkSuffixRec(int fromIndex, TrieForkJoinList<E> newList, Node node, int shift, boolean isRightmost, boolean isOwned) {
        int childIdx = (int) (rShiftU(fromIndex, shift) & MASK);
        if (shift == 0) {
            newList.rootShift = (byte) shift;
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
        newList.rootShift = (byte) shift;
        return ((ParentNode) node).copySuffix(isOwned, childIdx, leftNode, isRightmost, shift);
    }
    
    private static <E> Node forkRangeRec(int fromIndex, int toIndex, TrieForkJoinList<E> newList, Node node, int shift) {
        int fromChildIdx = (int) (rShiftU(fromIndex, shift) & MASK);
        int toChildIdx = (int) (rShiftU(toIndex, shift) & MASK);
        if (shift == 0) {
            newList.rootShift = (byte) shift;
            return node.copyRange(false, fromChildIdx, toChildIdx+1);
        }
        if (node instanceof SizedParentNode sn) {
            Sizes oldSizes = sn.sizes();
            while (oldSizes.get(fromChildIdx) <= fromIndex) {
                fromChildIdx++;
            }
            while (oldSizes.get(toChildIdx) <= toIndex) {
                toChildIdx++;
            }
            if (fromChildIdx != 0) {
                fromIndex -= oldSizes.get(fromChildIdx-1);
            }
            if (toChildIdx != 0) {
                toIndex -= oldSizes.get(toChildIdx-1);
            }
        }
        int childShift = shift - SHIFT;
        if (fromChildIdx == toChildIdx) {
            return forkRangeRec(fromIndex, toIndex, newList, (Node) node.children[fromChildIdx], childShift);
        }
        Node leftNode = forkSuffixRec(fromIndex, newList, (Node) node.children[fromChildIdx], childShift, false, false);
        Node rightNode = forkPrefixRec(toIndex, newList, (Node) node.children[toChildIdx], childShift, false, false);
        newList.rootShift = (byte) shift;
        return ((ParentNode) node).copyRange(false, fromChildIdx, leftNode, toChildIdx, rightNode, shift);
    }
    
    private static void splitRec(int fromIndex, int toIndex, Node[] nodes, int[] rootShifts, boolean isLeftmost, boolean isRightmost, int shift) {
        int fromChildIdx = (int) (rShiftU(fromIndex, shift) & MASK);
        int toChildIdx = (int) (rShiftU(toIndex, shift) & MASK);
        if (shift == 0) {
            nodes[0] = nodes[0].copy(nodes[0] != nodes[1], fromChildIdx+1);
            nodes[1] = nodes[1].copySuffix(true, toChildIdx);
            rootShifts[0] = rootShifts[1] = shift;
            return;
        }
        
        ParentNode left = (ParentNode) nodes[0], right = (ParentNode) nodes[1];
        if (left == right) {
            if (left instanceof SizedParentNode sn) {
                Sizes oldSizes = sn.sizes();
                while (oldSizes.get(fromChildIdx) <= fromIndex) {
                    fromChildIdx++;
                }
                while (oldSizes.get(toChildIdx) <= toIndex) {
                    toChildIdx++;
                }
                if (fromChildIdx != 0) {
                    fromIndex -= oldSizes.get(fromChildIdx-1);
                }
                if (toChildIdx != 0) {
                    toIndex -= oldSizes.get(toChildIdx-1);
                }
            }
        }
        else {
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
                while (oldSizes.get(toChildIdx) <= toIndex) {
                    toChildIdx++;
                }
                if (toChildIdx != 0) {
                    toIndex -= oldSizes.get(toChildIdx-1);
                }
            }
        }
        
        int childShift = shift - SHIFT;
        boolean isChildLeftmost = isLeftmost && fromChildIdx == 0;
        boolean isChildRightmost = isRightmost && toChildIdx == right.children.length-1;
        nodes[0] = left.getEditableChild(fromChildIdx);
        nodes[1] = right.getEditableChild(toChildIdx);
        splitRec(fromIndex, toIndex, nodes, rootShifts, isChildLeftmost, isChildRightmost, childShift);
        
        if (!isChildLeftmost) {
            // To avoid breaking the suffix copy, we only do the prefix copy in-place if left != right (no aliasing).
            // Unfortunately, if we need out-of-place, the copy method disowns copied children, because it assumes that
            // the existing tree is now sharing children with a new tree. But here we are discarding the existing tree.
            // So we copy-and-restore ownership of children.
            var oldOwns = left.owns;
            // TODO: copyPrefix can introduce sizes - due to non-full leaf - that are then obviated by the operation that called split
            nodes[0] = left = left.copyPrefix(left != right || isChildRightmost, fromChildIdx, nodes[0], shift);
            left.owns = oldOwns; // Don't worry about the removed suffix - trailing ownership is ignored
            rootShifts[0] = shift;
        }
        if (!isChildRightmost) {
            nodes[1] = right.copySuffix(true, toChildIdx, nodes[1], isRightmost, shift);
            rootShifts[1] = shift;
        }
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
    
    
    
    @Override
    public boolean join(Collection<? extends E> other) {
        // TODO: Update if we add ForkJoinCollection superinterface
        if (!(other instanceof ForkJoinList<? extends E> fjl)) {
            return addAll(other);
        }
        if (!((other = fjl.fork()) instanceof TrieForkJoinList<? extends E> right)) {
            return addAll(other);
        }
        if (right.isEmpty()) {
            return false;
        }
        checkNewSize(size, right.size);
        modCount++;
        
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
                pushDownTail();
                tail = rightTail;
                tailSize = (byte) rightTailSize;
                size += right.size;
                return true;
            }

            int newTailSize = leftTailSize + rightTailSize;
            Node leftTail = getEditableTail();

            if (newTailSize <= SPAN) {
                // Merge tails
                System.arraycopy(rightTail.children, 0, leftTail.children, leftTailSize, rightTailSize);
                tailSize = (byte) newTailSize;
                size += right.size;
                return true;
            }

            // Fill left tail from right tail; Push down; Adopt remaining right tail
            int suffixSize = SPAN - leftTailSize;
            System.arraycopy(rightTail.children,  0, leftTail.children, leftTailSize, suffixSize);
            pushDownTail();
            Node newTail = tail = new Node(new Object[SPAN]);
            System.arraycopy(rightTail.children, suffixSize, newTail.children, 0, tailSize = (byte) (rightTailSize - suffixSize));
            size += right.size;
            return true;
        }

        // Push down left tail
        trimTailToSize();
        pushDownTail();
        
        // Adopt right tail
        tail = right.tail;
        tailSize = right.tailSize;
        claimOrDisownTail(right.ownsTail());
        
        // Concatenate trees, assign root
        // TODO: Avoid editable if no rebalancing needed?
        size += right.size;
        Node[] nodes = { getEditableRoot(), right.getEditableRoot() };
        concatSubTree(nodes, rootShift, right.rootShift);
        return true;
    }
    
    // Called before pushing down a possibly non-full tail during join
    private void trimTailToSize() {
        if (tail.children.length != tailSize) {
            tail = tail.copy(claimTail(), tailSize);
        }
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
        return (len << shift); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
    }
    
    private static final ParentNode EMPTY_NODE = new ParentNode(new Object[0]);
    
    // Updates root, rootShift at the end
    // - Important! The total size under both nodes is expected to equal (size - tailSize)
    private void concatSubTree(Node[] nodes, int leftShift, int rightShift) {
        record State(Node left, Node right, boolean isRightmost) {}
        
        State[] stack = new State[Math.max(leftShift, rightShift) / SHIFT];
        Node left = nodes[0], right = nodes[1];
        boolean isRightmost = true;
        
        // TODO: Avoid editable if no rebalancing needed?
        int i = stack.length-1;
        for (; leftShift > rightShift; leftShift -= SHIFT) {
            stack[i--] = new State(left, null, true);
            left = left.getEditableChild(left.children.length-1);
        }
        for (; rightShift > leftShift; rightShift -= SHIFT) {
            stack[i--] = new State(null, right, isRightmost);
            isRightmost = isRightmost && right.children.length == 1;
            right = right.getEditableChild(0);
        }
        for (; leftShift > 0; leftShift -= SHIFT) {
            stack[i--] = new State(left, right, isRightmost);
            isRightmost = isRightmost && right.children.length == 1;
            left = left.getEditableChild(left.children.length-1);
            right = right.getEditableChild(0);
        }
        
        int leftLen = left.children.length, rightLen = right.children.length;
        if (leftLen + rightLen <= SPAN) { // TODO: || isRightmost ?
            // Right fits in left - copy it over
            Object[] newChildren = Arrays.copyOf(left.children, leftLen + rightLen);
            System.arraycopy(right.children, 0, newChildren, leftLen, rightLen);
            left = new Node(newChildren);
            right = EMPTY_NODE;
        }
        nodes[0] = left;
        nodes[1] = right;
        
        // Rebalance up the tree
        for (i = 0; i < stack.length; i++) {
            boolean isRightFirstChildEmpty = right == EMPTY_NODE;
            State state = stack[i];
            
            if (state.left == null) {
                // Left child is never empty after recursive call, because we introduce a parent for a non-empty child,
                // and we always rebalance by shifting left, so as we come up from recursion the left remains non-empty
                ParentNode newLeft;
                int childSize = getSizeIfNeedsSizedParent(left, isRightmost && isRightFirstChildEmpty, leftShift);
                if (childSize != -1) {
                    Sizes sizes = Sizes.of(leftShift + SHIFT, 1);
                    sizes.set(0, childSize);
                    (newLeft = new SizedParentNode(new Object[]{ left }, sizes)).claim(0);
                }
                else {
                    (newLeft = new ParentNode(new Object[]{ left })).claim(0);
                }
                state.right.children[0] = right; // This might be EMPTY_NODE
                nodes[0] = newLeft;
                nodes[1] = state.right;
            }
            else if (state.right == null) {
                ParentNode newRight = EMPTY_NODE;
                if (right != EMPTY_NODE) {
                    int childSize = getSizeIfNeedsSizedParent(right, true, leftShift);
                    if (childSize != -1) {
                        Sizes sizes = Sizes.of(leftShift + SHIFT, 1);
                        sizes.set(0, childSize);
                        (newRight = new SizedParentNode(new Object[]{ right }, sizes)).claim(0);
                    }
                    else {
                        (newRight = new ParentNode(new Object[]{ right })).claim(0);
                    }
                }
                state.left.children[state.left.children.length-1] = left;
                nodes[0] = state.left;
                nodes[1] = newRight; // This might be EMPTY_NODE
            }
            else {
                state.left.children[state.left.children.length-1] = left;
                state.right.children[0] = right; // This might be EMPTY_NODE
                nodes[0] = state.left;
                nodes[1] = state.right;
            }
            
            rebalance(nodes, leftShift += SHIFT, isRightmost = state.isRightmost, isRightFirstChildEmpty);
            left = nodes[0];
            right = nodes[1];
        }
        
        // Assign a new root
        if (right == EMPTY_NODE) {
            root = left;
            rootShift = (byte) leftShift;
        }
        else {
            Object[] children = { left, right };
            ParentNode newRoot;
            int childSize;
            if ((childSize = getSizeIfNeedsSizedParent(left, false, leftShift)) != -1) {
                Sizes sizes = Sizes.of(rootShift = (byte) (leftShift + SHIFT), 2);
                int lastSize = size - tailSize;
                sizes.set(0, childSize);
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else if ((childSize = getSizeIfNeedsSizedParent(right, true, leftShift)) != -1) {
                Sizes sizes = Sizes.of(rootShift = (byte) (leftShift + SHIFT), 2);
                int lastSize = size - tailSize;
                sizes.set(0, lastSize - childSize);
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else {
                root = newRoot = new ParentNode(children);
                rootShift = (byte) (leftShift + SHIFT);
            }
            newRoot.claim(0);
            newRoot.claim(1);
        }
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
    
    private static void rebalance(Node[] nodes, int shift, boolean isRightmost, boolean isRightFirstChildEmpty) {
        // Assume left and right are editable
        
        ParentNode left = (ParentNode) nodes[0], right = (ParentNode) nodes[1];
        
        if (right == EMPTY_NODE || (isRightFirstChildEmpty && right.children.length == 1)) {
            // If right is now empty, then we may have increased the grandchildren under left's rightmost child.
            // Either way, left is no less balanced than it started, so no rebalancing to do.
            nodes[0] = left.refreshSizesIfRightmostChildChanged(isRightmost, shift);
            nodes[1] = EMPTY_NODE;
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
        
        int minLength = 1 + ((totalNodes-1) >>> SHIFT);
        int maxLength = minLength + MARGIN;
        int curLength = leftChildren.length + rightChildren.length;
        if (curLength <= maxLength) {
            // Yay, no rebalancing needed
            // But we do still have some work to refresh left/right if their rightmost/leftmost child changed
            if (isRightFirstChildEmpty) {
                if (leftChildren.length + rightChildren.length-1 <= SPAN) {
                    // Removing right's first child makes it fit in left
                    left.shiftChildren(0, 0, rightChildren.length-1, right, 1);
                    nodes[0] = left.refreshSizes(isRightmost, shift);
                    nodes[1] = EMPTY_NODE;
                }
                else {
                    // Removing right's first child does not make it fit in left
                    right.shiftChildren(1, 0, 0, null, 0);
                    nodes[1] = right.refreshSizes(isRightmost, shift);
                    nodes[0] = left.refreshSizesIfRightmostChildChanged(false, shift);
                }
            }
            else if (leftChildren.length + rightChildren.length <= SPAN) { // TODO: || isRightmost ?
                // Right fits in left
                left.shiftChildren(0, 0, rightChildren.length, right, 0);
                nodes[0] = left.refreshSizes(isRightmost, shift);
                nodes[1] = EMPTY_NODE;
            }
            else {
                // Right does not fit in left
                nodes[0] = left.refreshSizesIfRightmostChildChanged(false, shift);
                nodes[1] = right.refreshSizesIfLeftmostChildChanged(isRightmost, shift);
            }
            return;
        }
        
        // Rebalance to eliminate excess children
        
        ParentNode lNode = left, rNode = left;
        Object[] lchildren = leftChildren, rchildren = leftChildren;
        Node lastNode = isRightmost ? (Node) rightChildren[rightChildren.length-1] : null;
        int i = 0, step = 1, llen = leftChildren.length, rlen = llen, childShift = shift - SHIFT, right0 = isRightFirstChildEmpty ? 1 : 0;
        long leftDeathRow = 0, rightDeathRow = right0; // long assumes SPAN <= 64
        boolean updatedLeft = false;
        
        do {
            int curLen;
            while ((curLen = ((Node) lchildren[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    i += right0 - llen;
                    llen = rlen = rightChildren.length;
                    lNode = rNode = right;
                    lchildren = rchildren = rightChildren;
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
                    j += right0 - llen;
                    rlen = rightChildren.length;
                    rNode = right;
                    rchildren = rightChildren;
                }
                
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
                    step = 1;
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
                    
                    // We emptied a node, so need to step further to get to the next non-empty node
                    step++;
                    break;
                }
            }
        } while (--curLength > maxLength);
        
        // Fixup left/right - remove emptied children, empty right into left if possible, and refresh sizes
        
        if (rightDeathRow == 0) { // Did not update right - must have updated left only (and leftDeathRow != 0)
            int remainingLeft = leftChildren.length - Long.bitCount(leftDeathRow);
            if (remainingLeft + rightChildren.length <= SPAN) { // TODO: || isRightmost ?
                // Removing some from left makes right fit in
                left.shiftChildren(leftDeathRow, 0, rightChildren.length, right, 0);
                nodes[0] = left.refreshSizes(isRightmost, shift);
                nodes[1] = EMPTY_NODE;
            }
            else {
                // Removing some from left does not make right fit in
                left.shiftChildren(leftDeathRow, 0, 0, null, 0);
                nodes[0] = left.refreshSizes(false, shift);
                nodes[1] = right.refreshSizesIfLeftmostChildChanged(isRightmost, shift);
            }
        }
        else if (!updatedLeft) { // Updated right only
            int remainingRight = rightChildren.length - Long.bitCount(rightDeathRow);
            if (leftChildren.length + remainingRight <= SPAN) { // TODO: || isRightmost ?
                // Removing some from right makes it fit in left
                left.shiftChildren(0, 0, remainingRight, right, rightDeathRow);
                nodes[0] = left.refreshSizes(isRightmost, shift);
                nodes[1] = EMPTY_NODE;
            }
            else {
                // Removing some from right does not make it fit in left
                right.shiftChildren(rightDeathRow, 0, 0, null, 0);
                nodes[1] = right.refreshSizes(isRightmost, shift);
                nodes[0] = left.refreshSizesIfRightmostChildChanged(false, shift);
            }
        }
        else { // Updated both
            // Since we're touching both anyway, we transfer as much as
            // we can from right to left, even if it doesn't empty right
            int remainingLeft = leftChildren.length - Long.bitCount(leftDeathRow);
            int remainingRight = rightChildren.length - Long.bitCount(rightDeathRow);
            int xfer = Math.min(SPAN - remainingLeft, remainingRight); // Possibly both 0
            left.shiftChildren(leftDeathRow, 0, xfer, right, rightDeathRow); // Possibly a no-op, if leftDeathRow == xfer == 0
            if (xfer == remainingRight) {
                nodes[0] = left.refreshSizes(isRightmost, shift);
                nodes[1] = EMPTY_NODE;
            }
            else {
                right.shiftChildren(rightDeathRow, xfer, 0, null, 0);
                nodes[1] = right.refreshSizes(isRightmost, shift);
                nodes[0] = left.refreshSizes(false, shift);
            }
        }
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
            size += (last << shift); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
            node = (Node) node.children[last];
        }
        return size + node.children.length;
    }
    
    @Override
    public boolean join(int index, Collection<? extends E> other) {
        if (index == size) {
            // This case is always hit if size == 0 and index is in bounds,
            // thus there is no isEmpty() case below.
            return join(other);
        }
        rangeCheckForAdd(index);
        // TODO: Update if we add ForkJoinCollection superinterface
        if (!(other instanceof ForkJoinList<? extends E> fjl)) {
            return addAll(index, other);
        }
        if (!((other = fjl.fork()) instanceof TrieForkJoinList<? extends E> right)) {
            return addAll(index, other);
        }
        if (right.isEmpty()) {
            return false;
        }
        checkNewSize(size, right.size);
        modCount++;
        
        int tailOffset = tailOffset();
        if (index >= tailOffset) {
            int tailIdx = index - tailOffset;
            if (right.root == null) {
                // Right is just a tail.
                // Either it all fits in left tail, or we will push down and make a new tail.
                Node leftTail = getEditableTail();
                int newTailSize = tailSize + right.tailSize;
                if (newTailSize <= SPAN) {
                    System.arraycopy(leftTail.children, tailIdx, leftTail.children, tailIdx + right.tailSize, tailSize - tailIdx);
                    System.arraycopy(right.tail.children, 0, leftTail.children, tailIdx, right.tailSize);
                    size += right.tailSize;
                    tailSize = (byte) newTailSize;
                    return true;
                }
                
                // Prepare new tail
                int spaceLeft = SPAN - tailIdx;
                int takeRight = Math.min(right.tailSize, spaceLeft);
                int takeLeft = spaceLeft - takeRight;
                int remainingRight = right.tailSize - takeRight;
                int remainingLeft = tailSize - tailIdx - takeLeft;
                Object[] newChildren = new Object[SPAN];
                System.arraycopy(right.tail.children, takeRight, newChildren, 0, remainingRight);
                System.arraycopy(leftTail.children, tailSize - remainingLeft, newChildren, remainingRight, remainingLeft);
                
                // Fixup old tail and push down
                System.arraycopy(leftTail.children, tailIdx, leftTail.children, tailIdx + takeRight, takeLeft);
                System.arraycopy(right.tail.children, 0, leftTail.children, tailIdx, takeRight);
                size += SPAN - tailSize;
                tailSize = SPAN;
                pushDownTail();
                
                tail = new Node(newChildren);
                size += tailSize = (byte) (remainingRight + remainingLeft);
                return true;
            }
            
            Node rightTail = right.getEditableTail();
            int spaceRight = SPAN - right.tailSize;
            int leftSuffixSize = tailSize - tailIdx;
            
            if (leftSuffixSize > spaceRight) {
                // Left tail suffix does not fit in right tail.
                // Copy over what fits, push it down, push down left tail prefix, and set left tail to remaining suffix.
                System.arraycopy(tail.children, tailIdx, rightTail.children, right.tailSize, spaceRight);
                right.tailSize = SPAN;
                right.size += spaceRight;
                right.pushDownTail();
                
                int remainingSuffixSize = leftSuffixSize - spaceRight;
                Object[] newTailChildren = new Object[SPAN];
                System.arraycopy(tail.children, tailIdx + spaceRight, newTailChildren, 0, remainingSuffixSize);
                size -= leftSuffixSize;
                boolean ownsTail = claimTail();
                if (tailIdx != 0) {
                    tail = tail.copy(ownsTail, tailSize = (byte) tailIdx);
                    pushDownTail();
                }
                tail = new Node(newTailChildren);
                tailSize = (byte) remainingSuffixSize;
                size += right.size + remainingSuffixSize;
            }
            else {
                // Left tail suffix fits in right tail.
                // Copy it over, push down left tail prefix, and set left tail to right tail.
                System.arraycopy(tail.children, tailIdx, rightTail.children, right.tailSize, leftSuffixSize);
                size -= leftSuffixSize;
                boolean ownsTail = claimTail();
                if (tailIdx != 0) {
                    tail = tail.copy(ownsTail, tailSize = (byte) tailIdx);
                    pushDownTail();
                }
                tail = right.tail;
                tailSize = (byte) (right.tailSize + leftSuffixSize);
                size += right.size + leftSuffixSize;
            }
            
            Node[] nodes = { getEditableRoot(), right.getEditableRoot() };
            concatSubTree(nodes, rootShift, right.rootShift);
            return true;
        }
        
        if (index == 0) {
            // Prepending
            size += right.size;
            right.trimTailToSize();
            right.pushDownTail();
            Node[] nodes = { right.getEditableRoot(), getEditableRoot() };
            concatSubTree(nodes, right.rootShift, rootShift);
            return true;
        }
        
        // Split root into left and right
        Node[] splitRoots = { getEditableRoot(), getEditableRoot() };
        int[] rootShifts = new int[2];
        splitRec(index-1, index, splitRoots, rootShifts, true, true, rootShift);
        Node rightRoot = splitRoots[1];
        
        // Concat other tree onto left
        size = index + right.size + tailSize; // Adjust so that size-tailSize == total size under both roots
        right.trimTailToSize();
        right.pushDownTail();
        splitRoots[1] = right.root;
        concatSubTree(splitRoots, rootShifts[0], right.rootShift);
        
        // Concat right onto left
        size = tailOffset + right.size + tailSize;
        int newRootShift = rootShift;
        splitRoots[0] = root;
        splitRoots[1] = rightRoot;
        concatSubTree(splitRoots, newRootShift, rootShifts[1]);
        return true;
    }
    
    private int tailOffset() {
        return size - tailSize;
    }
    
    // Method used to prevent integer overflow when increasing size.
    // Unlike ArrayList, we have no problem allocating more space, but we would need to
    // widen some fields and/or revisit operations on them if we wanted to allow bigger.
    private static void checkNewSize(int oldSize, int toAdd) {
        if (oldSize + toAdd <= 0) {
            throw new OutOfMemoryError("Required size " + oldSize + " + " + toAdd + " is too large");
        }
    }
    
    // Methods for each shift operation. These take a long argument (often via implicit widening at the call site),
    // to ensure we use shift % 64 instead of shift % 32. This should be sufficient to avoid incorrectly wrapping the
    // shift - Assuming that even a tree with max size (2^31-1) always has a rootShift < 64. If that assumption does not
    // hold, or if we wish to support larger sizes, we would need to branch on too-large shifts.
    
    private static long lShift(long n, int shift) {
        return n << shift;
    }
    
    private static long rShift(long n, int shift) {
        return n >> shift;
    }
    
    private static long rShiftU(long n, int shift) {
        return n >>> shift;
    }
    
    private void rangeCheckForAdd(int index) {
        if (index > size || index < 0) {
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        }
    }
    
    private String outOfBoundsMsg(int index) {
        return "Index: "+index+", Size: "+size;
    }
    
    private static String outOfBoundsMsg(int fromIndex, int toIndex) {
        return "From Index: " + fromIndex + " > To Index: " + toIndex;
    }
    
    private static class SubList<E> extends AbstractList<E> implements ForkJoinList<E>, RandomAccess {
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
        public ForkJoinList<E> fork() {
            checkForComodification();
            return root.forkRange(offset, offset + size);
        }
        
        @Override
        public E get(int index) {
            Objects.checkIndex(index, size);
            checkForComodification();
            return root.get(offset + index);
        }
        
        @Override
        public E set(int index, E element) {
            Objects.checkIndex(index, size);
            checkForComodification();
            int oldModCount = root.modCount;
            E old = root.set(offset + index, element);
            if (oldModCount != root.modCount) {
                updateSizeAndModCount(0);
            }
            return old;
        }
        
        @Override
        public int size() {
            checkForComodification();
            return size;
        }
        
        @Override
        public void add(int index, E element) {
            rangeCheckForAdd(index);
            checkForComodification();
            root.add(offset + index, element);
            updateSizeAndModCount(1);
        }
        
        @Override
        public E remove(int index) {
            Objects.checkIndex(index, size);
            checkForComodification();
            E result = root.remove(offset + index);
            updateSizeAndModCount(-1);
            return result;
        }
        
        @Override
        protected void removeRange(int fromIndex, int toIndex) {
            checkForComodification();
            root.removeRange(offset + fromIndex, offset + toIndex);
            updateSizeAndModCount(fromIndex - toIndex);
        }
        
        @Override
        public boolean addAll(Collection<? extends E> c) {
            return addAll(size, c);
        }
        
        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            rangeCheckForAdd(index);
            int cSize = c.size();
            if (cSize == 0) {
                return false;
            }
            checkForComodification();
            root.addAll(offset + index, c);
            updateSizeAndModCount(cSize);
            return true;
        }
        
        @Override
        public boolean join(Collection<? extends E> c) {
            return join(size, c);
        }
        
        @Override
        public boolean join(int index, Collection<? extends E> c) {
            rangeCheckForAdd(index);
            int cSize = c.size();
            if (cSize == 0) {
                return false;
            }
            checkForComodification();
            root.join(offset + index, c);
            updateSizeAndModCount(cSize);
            return true;
        }
        
        @Override
        public boolean removeAll(Collection<?> c) {
            return batchRemove(c, false);
        }
        
        @Override
        public boolean retainAll(Collection<?> c) {
            return batchRemove(c, true);
        }
        
        private boolean batchRemove(Collection<?> c, boolean complement) {
            checkForComodification();
            int oldSize = root.size;
            boolean modified = root.batchRemove(c, complement, offset, offset + size);
            if (modified) {
                updateSizeAndModCount(root.size - oldSize);
            }
            return modified;
        }
        
        @Override
        public boolean removeIf(Predicate<? super E> filter) {
            checkForComodification();
            int oldSize = root.size;
            boolean modified = root.removeIf(filter, offset, offset + size);
            if (modified) {
                updateSizeAndModCount(root.size - oldSize);
            }
            return modified;
        }
        
        @Override
        public Object[] toArray() {
            checkForComodification();
            return root.toArray(new Object[size], offset, offset + size);
        }
        
        @Override
        public <T> T[] toArray(T[] a) {
            checkForComodification();
            return root.toArray(a, offset, offset + size);
        }
        
        @Override
        public int hashCode() {
            var box = new Object(){ int hashCode = 1; };
            forEach(e -> box.hashCode = 31*box.hashCode + (e==null ? 0 : e.hashCode()));
            return box.hashCode;
        }
        
        @Override
        public void forEach(Consumer<? super E> action) {
            spliterator().forEachRemaining(action);
        }
        
        @Override
        public Iterator<E> iterator() {
            return listIterator();
        }
        
        @Override
        public ListIterator<E> listIterator(int index) {
            checkForComodification();
            rangeCheckForAdd(index);
            
            return root.new ListItr(offset + index) {
                @Override
                public boolean hasNext() {
                    return cursor < offset + size;
                }
                
                @Override
                public boolean hasPrevious() {
                    return cursor > offset;
                }
                
                @Override
                public int nextIndex() {
                    return cursor - offset;
                }
                
                @Override
                public int previousIndex() {
                    return cursor - offset - 1;
                }
                
                @Override
                public E next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return super.next();
                }
                
                @Override
                public E previous() {
                    if (!hasPrevious()) {
                        throw new NoSuchElementException();
                    }
                    return super.previous();
                }
                
                @Override
                public void forEachRemaining(Consumer<? super E> action) {
                    super.forEachRemaining(action, offset + size);
                }
                
                @Override
                public void set(E e) {
                    int oldModCount = root.modCount;
                    super.set(e);
                    if (oldModCount != root.modCount) {
                        updateSizeAndModCount(0);
                    }
                }
                
                @Override
                public void add(E e) {
                    super.add(e);
                    updateSizeAndModCount(1);
                }
                
                @Override
                public void remove() {
                    super.remove();
                    updateSizeAndModCount(-1);
                }
            };
        }
        
        @Override
        public ForkJoinList<E> subList(int fromIndex, int toIndex) {
            subListRangeCheck(fromIndex, toIndex, size);
            return new SubList<>(this, fromIndex, toIndex);
        }
        
        @Override
        public Spliterator<E> spliterator() {
            checkForComodification();
            
            return root.new Splitr(offset, -1, 0) {
                @Override
                int getFence() {
                    int hi = fence;
                    if (hi < 0) {
                        // Late-initialize expectedModCount
                        expectedModCount = SubList.this.modCount;
                        hi = fence = offset + SubList.this.size;
                    }
                    return hi;
                }
            };
        }
        
        private void rangeCheckForAdd(int index) {
            if (index < 0 || index > size) {
                throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
            }
        }
        
        private String outOfBoundsMsg(int index) {
            return "Index: "+index+", Size: "+size;
        }
        
        private void checkForComodification() {
            if (root.modCount != modCount) {
                throw new ConcurrentModificationException();
            }
        }
        
        private void updateSizeAndModCount(int sizeChange) {
            SubList<E> slist = this;
            do {
                slist.size += sizeChange;
                slist.modCount = root.modCount;
                slist = slist.parent;
            } while (slist != null);
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
    }
    
    private static class ParentNode extends Node {
        // NOTE: Type of owns (and related operations) need to be co-updated with SPAN:
        // SPAN=16 -> short
        // SPAN=32 -> int
        // SPAN=64 -> long
        // SPAN>64 -> long[]
        int owns;
        
        ParentNode(int owns, Object[] children) {
            super(children);
            this.owns = owns;
        }
        
        ParentNode(Object[] children) {
            super(children);
            this.owns = 0;
        }
        
        ParentNode(Object[] children, boolean owned) {
            super(children);
            this.owns = (int) ((1 << children.length) - 1);
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
        
        static int skipOwnership(int owns, int skip, int keep) {
            return (int) ((owns >>> skip) & mask(keep));
        }
        
        static int takeOwnership(int keep, int take, int owns) {
            return (int) ((owns & mask(take)) << keep);
        }
        
        static int removeFromOwnership(int owns, int remove) {
            return (int) ((owns & mask(remove)) | ((owns >>> 1) & ~mask(remove)));
        }
        
        static int mask(int shift) {
            return (1 << shift) - 1;
        }
        
        @Override
        ParentNode copy(boolean isOwned) {
            return isOwned ? this : new ParentNode(children.clone());
        }
        
        @Override
        ParentNode copy(boolean isOwned, int len) {
            int oldLen = children.length;
            if (len != oldLen) {
                Object[] newChildren = Arrays.copyOf(children, len);
                if (isOwned) {
                    children = newChildren;
                    owns |= ~mask(oldLen); // Take ownership of new slots if len > oldLen (trailing ownership is ignored)
                    return this;
                }
                return new ParentNode((short) ~mask(oldLen), newChildren);
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
        
        ParentNode copyPrefix(boolean isOwned, int toIndex, Node newLastChild, int shift) {
            disownPrefix(toIndex); // TODO: avoid if isOwned
            ParentNode newNode = copy(isOwned, toIndex+1);
            newNode.claim(toIndex);
            newNode.children[toIndex] = newLastChild;
            // TODO: Not always rightmost - when called by splitRec, we (may) want to retain sizes for later concat...
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
                if (isRightmost || fromSizes.get(take-1) != lShift(take, shift)) {
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
            var newOwns = (int) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, ((ParentNode) from).owns) : 0));
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
            var newOwns = owns;
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
                var fromOwns = from.owns;
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
                                : (child.children.length << childShift); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
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
        
        // Special-purpose method used during rebalance
        void editChild(int i, int skip, int keep, int take, Node from,
                       boolean isFromOwned, boolean isRightmost, int shift) {
            children[i] = ((Node) children[i]).copy(claim(i), skip, keep, take, from, isFromOwned, isRightmost, shift);
        }
    }
    
    private static class SizedParentNode extends ParentNode {
        Object sizes;
        
        SizedParentNode(int owns, Object[] children, Sizes sizes) {
            super(owns, children);
            this.sizes = sizes.unwrap();
        }
        
        SizedParentNode(Object[] children, Sizes sizes) {
            super(children);
            this.sizes = sizes.unwrap();
        }
        
        SizedParentNode(Object[] children, Sizes sizes, boolean owned) {
            super(children, owned);
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
            int oldLen = children.length;
            if (len != oldLen) {
                Object[] newChildren = Arrays.copyOf(children, len);
                Sizes newSizes = sizes().copy(len);
                if (isOwned) {
                    children = newChildren;
                    owns |= ~mask(oldLen); // Take ownership of new slots if len > oldLen (trailing ownership is ignored)
                    sizes = newSizes.unwrap();
                    return this;
                }
                return new SizedParentNode((short) ~mask(oldLen), newChildren, newSizes);
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
            int lastIdx = toIndex-fromIndex;
            disownRange(fromIndex+1, toIndex); // TODO: avoid if isOwned (currently, isOwned = false always)
            ParentNode newNode = copyRange(isOwned, fromIndex, toIndex+1); // TODO: Truncating-copy could make us not-Sized
            newNode.claim(0);
            newNode.claim(lastIdx);
            newNode.children[0] = newFirstChild;
            newNode.children[lastIdx] = newLastChild;
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
            Sizes newSizes = (skip == 0 ? lastSize : (lastSize -= oldSizes.get(skip-1))) != lShift(keep, shift)
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
                else if (isRightmost || fromSizes.get(take-1) != lShift(take, shift)) {
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
            var newOwns = (int) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, ((ParentNode) from).owns) : 0));
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
                if (prevSize == lShift(len-1, shift)) {
                    // All children before last are full
                    return new ParentNode(owns, oldChildren);
                }
                newSize = sizeSubTree(child, shift - SHIFT);
            }
            else {
                newSize = (child.children.length << (shift - SHIFT)); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
                if (lastSize - currSize + newSize == lShift(len, shift)) {
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
                newSize = (child.children.length << (shift - SHIFT)); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
                if (!(oldChildren[len-1] instanceof SizedParentNode) && // last child has no gaps
                    oldSizes.get(len-2) - currSize + newSize == lShift(len-1, shift)) { // children before last are full
                    // All children before last are full
                    return new ParentNode(owns, oldChildren);
                }
            }
            else {
                newSize = (child.children.length << (shift - SHIFT)); // Actual sizes cannot exceed Integer.MAX_VALUE, so no need to protect shift
                if (oldSizes.get(len-1) - currSize + newSize == lShift(len, shift)) {
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
            long maxSize = lShift(SPAN, shift);
            if (maxSize <= 256) {
                return new Sizes.OfByte(new byte[len]);
            }
            if (maxSize <= 65536) {
                return new Sizes.OfShort(new char[len]);
            }
            return new Sizes.OfInt(new int[len]);
        }
        
        // Only used if SHIFT <= 4 (SPAN <= 16)
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
