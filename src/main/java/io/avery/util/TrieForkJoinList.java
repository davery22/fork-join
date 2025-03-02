package io.avery.util;

import java.util.*;

// TODO: Implement RandomAccess?
// TODO: can rootShift get too big? (ie shift off the entire index -> 0, making later elements unreachable)
//  - I think the answer is "yes, but we'd still find later elements by linear searching a size table"
// TODO: Right shifts can wrap! eg: (int >>> 35) == (int >>> 3)
//  - Force result to 0 when shift is too big

public class TrieForkJoinList<E> extends AbstractList<E> implements ForkJoinList<E> {
    private static final int SHIFT = 4;
    private static final int SPAN = 1 << SHIFT;
    private static final int MASK = SPAN-1;
    private static final int MARGIN = 2;
    private static final int DO_NOT_REDISTRIBUTE = SPAN - MARGIN/2; // During rebalance(), a node with size >= threshold is not redistributed
    private static final Object[] INITIAL_TAIL = new Object[SPAN];
    
    private Node root;
    private Object[] tail;
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
    
    private Object[] getEditableTail() {
        return claimTail() ? tail : (tail = Arrays.copyOf(tail, SPAN));
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
        size = tailSize = rootShift = 0;
        root = null;
        tail = INITIAL_TAIL;
        owns = 0;
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
        owns = 0;
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
    // join(collection)
    // join(index, collection)
    
    // TODO: Make sure we handle modCount
    // -get(index)
    // -set(index, element)
    // -add(element)
    // -clear()
    // -reversed()
    // -size()
    // -fork()
    
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
        Object[] children;
        
        if (index >= tailOffset()) {
            children = tail;
        }
        else if (rootShift == 0) {
            children = root.children;
        }
        else {
            Node node = root;
            for (int shift = rootShift; ; shift -= SHIFT) {
                int childIdx = index >>> shift;
                if (node instanceof SizedNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                if (shift == SHIFT) {
                    children = (Object[]) node.children[childIdx & MASK];
                    break;
                }
                node = (Node) node.children[childIdx & MASK];
            }
        }
        
        @SuppressWarnings("unchecked")
        E value = (E) children[index & MASK];
        return value;
    }
    
    @Override
    public E set(int index, E element) {
        Objects.checkIndex(index, size);
        modCount++;
        int tailOffset = tailOffset();
        Object[] children;
        
        if (index >= tailOffset) {
            children = getEditableTail();
            index -= tailOffset;
        }
        else if (rootShift == 0) {
            children = getEditableRoot().children;
        }
        else {
            Node node = getEditableRoot();
            for (int shift = rootShift; ; shift -= SHIFT) {
                int childIdx = index >>> shift;
                if (node instanceof SizedNode sn) {
                    Sizes sizes = sn.sizes();
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
                    }
                }
                if (shift == SHIFT) {
                    children = (Object[]) node.getEditableChild(childIdx & MASK);
                    break;
                }
                node = (Node) node.getEditableChild(childIdx & MASK);
            }
        }
        
        @SuppressWarnings("unchecked")
        E old = (E) children[index & MASK];
        children[index & MASK] = element;
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
        size = tailSize = rootShift = owns = 0;
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
        
        Object[] oldTail = tail;
        int oldTailSize = tailSize;
        @SuppressWarnings("unchecked")
        E old = (E) oldTail[oldTailSize-1];
        
        if (oldTailSize > 1) {
            getEditableTail()[--tailSize] = null;
            size--;
        }
        else if (size == 1) {
            if (!ownsTail()) {
                tail = INITIAL_TAIL;
            }
            else {
                tail[0] = null;
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
        Node oldRoot = root;
        int oldRootShift = rootShift;
        if (oldRootShift == 0) {
            claimOrDisownTail(ownsRoot());
            tailSize = (tail = oldRoot.children).length;
            root = null;
            return;
        }
        
        // Two top-down passes:
        //  1. Find the deepest ancestor that will be non-empty after tail pull-up (and pull-up the new tail)
        //  2. Truncate that deepest ancestor (which requires claiming it, and thus the path to it)
        
        // First pass
        Node node = oldRoot;
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
                tailSize = (tail = (Object[]) node.children[lastIdx]).length;
                break;
            }
            node = (Node) node.children[lastIdx];
        }
        
        // Second pass
        if (deepestNonEmptyAncestorShift == oldRootShift) {
            int len = oldRoot.children.length;
            if (len > 2) {
                getEditableRoot(len-1);
            }
            else if (len == 2) {
                // Replace root with its first child (unless that child is a non-full leaf)
                // TODO: The literature didn't seem to account that we might be promoting a non-full leaf to root?
                int newRootShift = rootShift - SHIFT;
                Object[] child;
                if (newRootShift > 0) {
                    root = (Node) oldRoot.children[0];
                    rootShift = newRootShift;
                    if (!oldRoot.owns(0)) {
                        disownRoot();
                    }
                }
                else if ((child = (Object[]) oldRoot.children[0]).length == SPAN) {
                    root = new Node(child);
                    rootShift = newRootShift;
                    if (!oldRoot.owns(0)) {
                        disownRoot();
                    }
                }
                else {
                    // Left child length < SPAN - root must already be a SizedNode, we just need to truncate it
                    getEditableRoot(1);
                }
            }
            else { // len == 1
                root = null;
                rootShift = 0;
            }
        }
        else {
            // Claim nodes up to the deepest non-empty ancestor
            node = getEditableRoot();
            int lastIdx = node.children.length-1;
            int droppedSize = -tailSize;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedNode sn) {
                    sn.sizes().inc(lastIdx, droppedSize);
                }
                int childLastIdx = ((Node) node.children[lastIdx]).children.length-1;
                if (shift == deepestNonEmptyAncestorShift + SHIFT) {
                    node.getEditableChild(lastIdx, childLastIdx);
                    break;
                }
                node = (Node) node.getEditableChild(lastIdx);
                lastIdx = childLastIdx;
            }
        }
    }
    
    private void addToTail(E e) {
        if (tailSize < SPAN) {
            // Could check for INITIAL_TAIL here to skip copying, but that slows down the common case
            getEditableTail()[tailSize++] = e;
            size++;
            return;
        }
        
        pushDownTail(-1);
        claimTail();
        (tail = new Object[SPAN])[0] = e;
        tailSize = 1;
        size++;
    }
    
    // May update root, rootShift
    private void pushDownTail(int gapAtAndBelowShift) {
        // gapAtAndBelowShift will be -1 except when called by join() to push down the left tail, in which case
        // gapAtAndBelowShift will be the right rootShift. At and below that shift, there may be a gap between the left
        // and right trees - if there are non-full nodes in that range, they and all their ancestors must be converted
        // to SizedNodes if not already. Also, the left tail may not be full during join(), which will trivially force
        // all its ancestors to be converted to SizedNodes if not already.
        int oldTailSize = tailSize;
        boolean nonFullTail = oldTailSize < SPAN;
        
        Node oldRoot = root;
        if (oldRoot == null) {
            if (nonFullTail) {
                Sizes sizes = Sizes.of(rootShift = SHIFT, 1);
                sizes.set(0, oldTailSize);
                claimRoot();
                (root = new SizedNode(new Object[]{ tail }, sizes)).claimOrDisown(0, ownsTail());
            }
            else {
                claimOrDisownRoot(ownsTail());
                root = new Node(tail);
            }
            return;
        }
        
        // Two top-down passes:
        //  1. Find the deepest ancestor that will be non-full after tail push-down
        //  2. Extend that deepest ancestor with a path to the tail (which requires claiming it, and thus the path to it)
        
        // First pass
        Node node = oldRoot;
        int oldRootShift = rootShift;
        int deepestNonFullAncestorShift = oldRootShift + SHIFT;
        for (int shift = oldRootShift; ; shift -= SHIFT) {
            int len = node.children.length;
            if (len < SPAN) {
                deepestNonFullAncestorShift = shift;
            }
            if (shift <= SHIFT) {
                break;
            }
            node = (Node) node.children[len-1];
        }
        
        // If gapAtAndAboveShift <= gapAtAndBelowShift, then all nodes down to gapAtAndAboveShift must be sized
        int gapAtAndAboveShift = nonFullTail ? 0 : deepestNonFullAncestorShift;
        if (gapAtAndAboveShift > gapAtAndBelowShift) {
            gapAtAndAboveShift = Integer.MAX_VALUE; // Prevent sizing any nodes
        }
        
        // Second pass
        if (deepestNonFullAncestorShift > oldRootShift) {
            // No existing non-full ancestors - need to make a new root above old root
            Object[] children = new Object[]{ oldRootShift == 0 ? oldRoot.children : oldRoot, null };
            if (gapAtAndAboveShift <= deepestNonFullAncestorShift || oldRoot instanceof SizedNode) {
                Sizes sizes = Sizes.of(deepestNonFullAncestorShift, 2);
                sizes.set(0, size - oldTailSize);
                sizes.set(1, size);
                node = new SizedNode(children, sizes);
            }
            else {
                node = new Node(children);
            }
            if (claimRoot()) {
                node.claim(0);
            }
            root = node;
            rootShift = deepestNonFullAncestorShift;
        }
        else if (deepestNonFullAncestorShift == oldRootShift) {
            // Claim root - it is the deepest non-full ancestor
            int len = oldRoot.children.length;
            node = getSizedEditableRoot(len+1, gapAtAndAboveShift <= oldRootShift ? oldRootShift : 0);
            if (node instanceof SizedNode sn) {
                sn.sizes().inc(len, oldTailSize);
            }
        }
        else {
            // Claim nodes up to the deepest non-full ancestor
            node = getSizedEditableRoot(gapAtAndAboveShift <= oldRootShift ? oldRootShift : 0);
            int lastIdx = node.children.length-1;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedNode sn) {
                    sn.sizes().inc(lastIdx, oldTailSize);
                }
                int childLen = ((Node) node.children[lastIdx]).children.length;
                int sizedShift = gapAtAndAboveShift <= shift-SHIFT ? shift-SHIFT : 0;
                if (shift == deepestNonFullAncestorShift + SHIFT) {
                    node = (Node) node.getSizedEditableChild(lastIdx, childLen+1, sizedShift);
                    if (node instanceof SizedNode sn) {
                        sn.sizes().inc(childLen, oldTailSize);
                    }
                    break;
                }
                node = (Node) node.getSizedEditableChild(lastIdx, sizedShift);
                lastIdx = childLen-1;
            }
        }
        
        // Finish second pass: Extend a path (of the appropriate height) from the deepest non-full ancestor to tail
        int lastIdx = node.children.length-1;
        for (int shift = deepestNonFullAncestorShift; shift > SHIFT; shift -= SHIFT) {
            node.claim(lastIdx);
            if (gapAtAndAboveShift <= shift-SHIFT) {
                Sizes sizes = Sizes.of(shift-SHIFT, 1);
                sizes.set(0, oldTailSize);
                node.children[lastIdx] = node = new SizedNode(new Object[1], sizes);
            }
            else {
                node.children[lastIdx] = node = new Node(new Object[1]);
            }
            lastIdx = 0;
        }
        node.claimOrDisown(lastIdx, ownsTail());
        node.children[lastIdx] = tail;
    }
    
    @Override
    public int size() {
        return size;
    }
    
    @Override
    public ForkJoinList<E> fork() {
        // Disown any owned top-level objects, to force path-copying upon future mutations
        owns = 0;
        return new TrieForkJoinList<>(this);
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
            Object[] rightTail = right.tail;

            if (leftTailSize == SPAN) {
                // Push down tail and adopt right tail
                pushDownTail(-1);
                tail = rightTail;
                tailSize = rightTailSize;
                size += right.size;
                return true;
            }

            int newTailSize = leftTailSize + rightTailSize;
            Object[] leftTail = getEditableTail();

            if (newTailSize <= SPAN) {
                // Merge tails
                System.arraycopy(rightTail, 0, leftTail, leftTailSize, rightTailSize);
                tailSize = newTailSize;
                size += right.size;
                return true;
            }

            // Fill left tail from right tail; Push down; Adopt remaining right tail
            int suffixSize = SPAN - leftTailSize;
            System.arraycopy(rightTail,  0, leftTail, leftTailSize, suffixSize);
            pushDownTail(-1);
            // TODO? Makes a new array - assumes right tail would need copied anyway
            Object[] newTail = tail = new Object[SPAN];
            System.arraycopy(rightTail, suffixSize, newTail, 0, tailSize = rightTailSize - suffixSize);
            size += right.size;
            return true;
        }

        // Push down left tail
        if (tail.length != tailSize) {
            claimTail();
            tail = Arrays.copyOf(tail, tailSize);
        }
        pushDownTail(right.rootShift);
        
        // Concatenate trees
        ConcatState state = new ConcatState();
        // TODO: Avoid editable if no rebalancing needed?
        state.left = getEditableRoot();
        state.right = right.getEditableRoot();
        concatSubTree(state, rootShift, right.rootShift, true);
        
        // Assign root
        size += right.size;
        Node leftNode = state.left, rightNode = state.right;
        if (rightNode.children.length == 0) {
            root = leftNode;
        }
        else {
            int newRootShift = rootShift += SHIFT;
            Object[] children = new Object[]{ leftNode, rightNode };
            Node newRoot;
            if (leftNode instanceof SizedNode sn) {
                Sizes sizes = Sizes.of(newRootShift, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, sn.sizes().get(sn.children.length-1));
                sizes.set(1, lastSize);
                newRoot = root = new SizedNode(children, sizes);
            }
            else if (rightNode instanceof SizedNode sn) {
                Sizes sizes = Sizes.of(newRootShift, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, lastSize - sn.sizes().get(sn.children.length-1));
                sizes.set(1, lastSize);
                newRoot = root = new SizedNode(children, sizes);
            }
            else {
                newRoot = root = new Node(children);
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
    
    // Used to communicate between concatSubTree() and rebalance()
    //  - concatSubTree() updates left/right before passing to rebalance(), and restores afterward
    //  - concatSubTree() clears leftDeathRow/rightDeathRow before passing to rebalance(),
    //    which may update these before returning, indicating to concatSubTree() which children to remove from left/right
    private static class ConcatState {
        Node left, right;
        long leftDeathRow, rightDeathRow; // Works for SPAN <= 64
        boolean leftUpdated, rightUpdated;
    }
    
    private static final Node EMPTY_NODE = new Node(new Object[0]);
    
    private static void concatSubTree(ConcatState state, int leftShift, int rightShift, boolean isRightMost) {
        // TODO: Avoid editable if no rebalancing needed?
        Node left = state.left, right = state.right;
        if (leftShift > rightShift) {
            state.left = (Node) left.getEditableChild(left.children.length-1);
            concatSubTree(state, leftShift - SHIFT, rightShift, isRightMost);
            left.children[left.children.length-1] = state.left;
            // Right child may be empty after recursive call - due to rebalancing - but if not we introduce a parent
            if (state.rightDeathRow != 0) {
                right = EMPTY_NODE;
                state.rightDeathRow = 0; // Prevent attempted updates to EMPTY_NODE // TODO: Necessary?
            }
            else if (state.right instanceof SizedNode sn) {
                Sizes sizes = Sizes.of(leftShift, 1);
                sizes.set(0, sn.sizes().get(sn.children.length-1));
                (right = new SizedNode(new Object[]{ state.right }, sizes)).claim(0);
            }
            else {
                (right = new Node(new Object[]{ state.right })).claim(0);
            }
        }
        else if (leftShift < rightShift) {
            state.right = (Node) right.getEditableChild(0);
            concatSubTree(state, leftShift, rightShift - SHIFT, isRightMost && right.children.length == 1);
            // Left child is never empty after recursive call, because we introduce a parent for a non-empty child,
            // and we always rebalance by shifting left, so as we come up from recursion the left remains non-empty
            if (state.left instanceof SizedNode sn) { // TODO: Verify cases (!isRightMost || state.rightDeathRow != 0) are handled by lower level / imply SizedNode
                Sizes sizes = Sizes.of(rightShift, 1);
                sizes.set(0, sn.sizes().get(sn.children.length-1));
                (left = new SizedNode(new Object[]{ state.left }, sizes)).claim(0);
            }
            else {
                (left = new Node(new Object[]{ state.left })).claim(0);
            }
            right.children[0] = state.right; // This might be an empty node
        }
        else if (leftShift > 0) {
            state.left = (Node) left.getEditableChild(left.children.length-1);
            state.right = (Node) right.getEditableChild(0);
            concatSubTree(state, leftShift - SHIFT, rightShift - SHIFT, isRightMost && right.children.length == 1);
            left.children[left.children.length-1] = state.left;
            right.children[0] = state.right; // This might be an empty node
        }
        else {
            // Nothing to do at the bottom level
            return;
        }
        state.left = left;
        state.right = right;
        rebalance(state, leftShift, isRightMost);
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
    
    private static void rebalance(ConcatState state, int shift, boolean isRightMost) {
        // Assume left and right are editable
        // Assume that right's first child has already been removed if lower level rebalancing required it
        //  - This might even mean that right is empty...
        // TODO: Assume shift > SHIFT beyond this point, ie we can cast children[i] to Node
        
        Node left = state.left, right = state.right;
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
            // TODO: May still need to update left/right,
            //  if their rightmost/leftmost children changed in lower-level rebalance()
            return; // Yay, no rebalancing needed
        }
        
        long leftDeathRow = 0, rightDeathRow = state.rightDeathRow;
        boolean skipR0 = (rightDeathRow & 1) != 0;
        int childShift = shift - SHIFT;
        int i = 0, step = 1;
        int llen = leftChildren.length, rlen = leftChildren.length;
        Node lNode = left, rNode = left;
        Node lastNode = !isRightMost ? null : (Node) (rightChildren.length > (skipR0 ? 1 : 0) ? rightChildren[rightChildren.length-1] : leftChildren[leftChildren.length-1]);
        Object[] lchildren = leftChildren, rchildren = leftChildren;
        
        do {
            int curLen;
            while ((curLen = ((Node) lchildren[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    if ((i -= llen) == 0 && skipR0) {
                        i = 1;
                    }
                    llen = rightChildren.length;
                    lNode = right;
                    lchildren = rightChildren;
                }
                step = 1;
            }
            int skip = 0;
            for (;;) {
                int j = i + step;
                if (j >= llen) {
                    // We may hit this case multiple times, if we empty the first child
                    // on the right and the last child on the left is still below threshold
                    if ((j -= llen) == 0 && skipR0) {
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
                    lNode.getEditableChild(i, skip, curLen, slots, rChildNode, rNode.owns(j), false, childShift);
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
                    lNode.getEditableChild(i, skip, curLen, items, rChildNode, rNode.owns(j), rChildNode == lastNode, childShift);
                    
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
        
        // Inform caller of children to delete
        state.leftDeathRow = leftDeathRow;
        state.rightDeathRow = rightDeathRow;
        
        int leftLen = leftChildren.length - Long.bitCount(leftDeathRow);
        int rightLen = rightChildren.length - Long.bitCount(rightDeathRow);
        if (rightLen <= 0) { // TODO: == 0?
            state.right = EMPTY_NODE;
            // TODO: Update left
        }
        else if (leftLen + rightLen > SPAN) {
            // TODO: Update left/right separately
        }
        else {
            // TODO: Empty right into left
        }
        
        // TODO: Update left/right:
        //  - Apply effects of resultant children from lower-level concatSubTree() (may affect parent size / sized-ness)
        //  - Remove deathRow children from left/right, and empty right to left if possible
        //    - If isRightMost, this can make left lose sized-ness
        //  - Mark whether left/right updated, for higher-level (ignore right if empty - will be handled by death row)
        
        state.rightDeathRow = state.right.children.length == 0 ? 1 : 0;
    }
    
    // Reframing:
    // when I encounter a direct-child with < DO_NOT_REDISTRIBUTE grand-children, can I shift left grand-children from
    // subsequent direct-children until at or exceeds threshold?
    //  - This could decrease the length of the rightmost direct-child, which could require making it a SizedNode
    //  - There may not be enough grand-children to the right to reach threshold (if any direct-children to the left were above threshold)
    //    - But that's okay - pigeonhole says we will have saved enough space overall
    //  - The main disadvantage of this approach is that we can't stop early - we visit all direct-children
    //  - We might as well shift-to-fill the left direct-children, which will leave at most 1 partially-filled direct-child (the rightmost)
    //    - We can augment to count when we empty a direct-child while filling another, and stop when we have emptied enough)
    //      - Does this approach ensure that direct-children end up at least as full as before?
    //      - Is this approach equivalent to redistributing?
    //        - Assume we still skip nodes until we find one < DO_NOT_REDISTRIBUTE
    
    // Example:
    // 15 5 14 16 3 3 6
    // margin = 2
    // total = 62
    // min = ((62-1)/16)+1 = 4
    // max = min + margin = 6
    // cur = 7
    
    // redistribute_if < 14
    // redistribute: 15 (5)0 (14+2)16           16     (3+3)6        3 6
    // alt:          15      (5+11)16 (14-11+13)16 (16-13+3)6 (3-3)0 3 6
    
    
    
    
    
    // TODO: avoid creating size tables / sized nodes if not needed
    //  - NOTE: unless isTop, the (direct) Node returned from concatSubTree()/rebalance() is just going to be
    //    destructured anyway, so no point making a SizedNode
    //  - When would concatSubTree not need size tables?
    //    - At level 0, never need size tables
    //    - At level N, no need if wide-node is full (contains exactly M or 2M full children), OR wide-node is leftwise full with no right sibling
    //      - "full children" => all size M (works for all levels), or no sized nodes (works for level != 0)
    //      - "no right sibling" => keep track of this as we go down the tree initially

//    private Node concatSubTree(Object leftParent, Id leftParentId, Node leftNode, int leftShift,
//                               Object rightParent, Id rightParentId, Node rightNode, int rightShift,
//                               boolean isTop, boolean isRightMost) {
//        // TODO: Pipe parents down to ensureEditable; transfer ownership to new parents where applicable
//        // TODO: Can we reuse existing nodes instead of returning new ones here?
//        if (leftShift > rightShift) {
//            Id newLeftParentId = leftParentId != null ? leftNode.id() : null;
//            Node centerNode = concatSubTree(leftNode, newLeftParentId, (Node) leftNode.children[leftNode.children.length-1], leftShift - SHIFT, rightParent, rightParentId, rightNode, rightShift, false, isRightMost);
//            return rebalance(leftNode, centerNode, null, leftShift, isTop, isRightMost);
//        }
//        if (leftShift < rightShift) {
//            Id newRightParentId = rightParentId != null ? rightNode.id() : null;
//            Node centerNode = concatSubTree(leftParent, leftParentId, leftNode, leftShift, rightNode, newRightParentId, (Node) rightNode.children[0], rightShift - SHIFT, false, isRightMost && rightNode.children.length == 1);
//            return rebalance(null, centerNode, rightNode, rightShift, isTop, isRightMost);
//        }
//        if (leftShift > 0) {
//            Id newLeftParentId = leftParentId != null ? leftNode.id() : null;
//            Id newRightParentId = rightParentId != null ? rightNode.id() : null;
//            Node centerNode = concatSubTree(leftNode, newLeftParentId, (Node) leftNode.children[leftNode.children.length-1], leftShift - SHIFT, rightNode, newRightParentId, (Node) rightNode.children[0], rightShift - SHIFT, false, isRightMost && rightNode.children.length == 1);
//            return rebalance(leftNode, centerNode, rightNode, leftShift, isTop, isRightMost);
//        }
//        Node node;
//        if (isTop) {
//            int leftSize = leftNode.children.length, rightSize = rightNode.children.length, mergedSize = leftSize + rightSize;
//            if (mergedSize == SPAN + SPAN) {
//                Id newId = new Id();
//                node = new ParentNode(newId, leftParentId, new Object[]{ leftNode, rightNode });
//                leftNode.tryTransferOwnership(leftParentId, newId);
//                rightNode.tryTransferOwnership(rightParentId, newId);
//                return node;
//            }
//            SizedParentNode sn;
//            if (mergedSize <= SPAN) {
//                // TODO: How would this case happen in practice?
//                //  We know that !this.isEmpty(), so this.tailSize >= 1
//                //  We know that this.rootShift == 0, so root = tail (we already pushed down tail), so leftSize = this.tailSize.
//                //  We know that right.rootShift == 0 and right.root != null, so rightSize >= SPAN
//                //  So mergedSize = leftSize + rightSize >= 1 + SPAN > SPAN ??
//                //  Literature implied that slicing might mess things up ??
//                leftNode = leftNode.ensureEditableWithLen(leftParentId, mergedSize);
//                System.arraycopy(rightNode.children, 0, leftNode.children, leftSize, rightSize);
//                if (mergedSize == SPAN) {
//                    return leftNode;
//                }
//                // Need another level with a size table just to indicate that the tree is not leftwise dense.
//                Id newId = new Id();
//                Sizes sizes = Sizes.of(leftParentId, SHIFT, 1);
//                sn = new SizedParentNode(newId, leftParentId, new Object[]{ leftNode }, sizes);
//                leftNode.tryTransferOwnership(leftParentId, newId);
//            }
//            else {
//                Id newId = new Id();
//                Sizes sizes = Sizes.of(leftParentId, SHIFT, 2);
//                sn = new SizedParentNode(newId, leftParentId, new Object[]{ leftNode, rightNode }, sizes);
//                leftNode.tryTransferOwnership(leftParentId, newId);
//                rightNode.tryTransferOwnership(rightParentId, newId);
//            }
//            setSizes(sn, rootShift = SHIFT);
//            return sn;
//        }
//        return new Node(null, new Object[]{ leftNode, rightNode }); // TODO: set children parents -- this Node will be unwrapped
//    }
//
//    private static class RebalanceState {
//        Object[] in;
//        Object[] out;
//
//        RebalanceState(Node left, Node center, Node right) {
//            int leftLen = left != null ? left.children.length : 0;
//            int rightLen = right != null ? right.children.length : 0;
//            int centerLen = center.children.length;
//            in = new Node[leftLen + centerLen + rightLen];
//            if (left != null) {
//                System.arraycopy(left.children, 0, in, 0, leftLen);
//            }
//            System.arraycopy(center.children, 0, in, leftLen, centerLen);
//            if (right != null) {
//                System.arraycopy(right.children, 0, in, leftLen + centerLen, rightLen);
//            }
//        }
//    }
//
//    private Node rebalance(Node leftNode, Node centerNode, Node rightNode, int shift, boolean isTop, boolean isRightMost) {
//        RebalanceState state = new RebalanceState(leftNode, centerNode, rightNode);
//        int[] counts = createConcatPlan(state);
//        executeConcatPlan(state, counts, shift);
//        Object[] out = state.out;
//        if (out.length <= SPAN) {
//            // TODO: avoid creating size tables / sized nodes if not needed
//            Sizes sizes = Sizes.of(null, shift, out.length);
//            SizedParentNode sn = new SizedParentNode(new WeakReference<>(null), out, sizes); // TODO: set children parent
//            setSizes(sn, shift);
//            if (isTop) {
//                sn.tryTransferOwnership(null, this); // TODO: update
//                rootShift = shift;
//                return sn;
//            }
//            return new Node(new WeakReference<>(null), new Object[]{ sn }); // TODO: set child parent
//        }
//        int leftLen = SPAN;
//        int rightLen = out.length - SPAN;
//        Object[] left = new Object[leftLen];
//        Object[] right = new Object[rightLen];
//        System.arraycopy(out, 0, left, 0, leftLen);
//        System.arraycopy(out, leftLen, right, 0, rightLen);
//        // TODO: avoid creating size tables / sized nodes if not needed
//        Sizes leftSizes = Sizes.of(null, shift, leftLen); // TODO: parent
//        Sizes rightSizes = Sizes.of(null, shift, rightLen); // TODO: parent
//        SizedParentNode leftSn = new SizedParentNode(null, left, leftSizes); // TODO: parent
//        SizedParentNode rightSn = new SizedParentNode(null, right, rightSizes); // TODO: parent
//        setSizes(leftSn, shift);
//        setSizes(rightSn, shift);
//        if (isTop) {
//            rootShift = shift += SHIFT;
//            Sizes sizes = Sizes.of(this, shift, 2);
//            SizedParentNode sn = new SizedParentNode(this, new Object[]{ leftSn, rightSn }, sizes);
//            setSizes(sn, shift);
//            return sn;
//        }
//        return new Node(new WeakReference<>(null), new Object[]{ leftSn, rightSn }); // TODO: [X] parent
//    }
//
//    private int[] createConcatPlan(RebalanceState state) {
//        Object[] in = state.in;
//        int[] counts = new int[in.length];
//        int totalNodes = 0;
//
//        for (int i = 0; i < in.length; i++) {
//            totalNodes += counts[i] = ((Node) in[i]).children.length;
//        }
//
//        // TODO: Somewhere, maybe about here, we need to transfer ownership of grandchildren (g for c : in for g : c.children)
//        //  (if they are nodes, and owned up to this point). Unfortunately this means visiting each grandchild,
//        //  rather than just array-copying pointers to them. The cache misses may be devastating.
//        //  Some options:
//        //   1. We could just not transfer ownership, but that would leave the entire list unowned, since everything after
//        //      the first parentId mismatch is unowned, and the root children would not match expected parentId.
//        //   2. We could back out of using fine-grained ids, and just use the listId (and change it upon forking).
//        //      This would mean subList forking would disown nodes even outside the subList.
//        //   3. We could move the parentIds to the parent itself, ie store in an array alongside children. This seems to
//        //      be exactly what size tables do, rather than store the size on each node. This significantly adds to the
//        //      cost of copying a ParentNode. And what about parentIds of size tables?
//        //      - Could this be a bitset instead of an array? It seems sufficient to know "do I own this child?"
//        //        - On fork(), list disowns root and tail (zeroes-out ownership bits)
//        //        - On node copy, node disowns children (zeroes-out ownership bits)
//
//        int minLength = ((totalNodes-1) / SPAN) + 1;
//        int maxLength = minLength + MARGIN;
//        int finalLength = in.length;
//        for (int i = 0; finalLength > maxLength; ) {
//            while (counts[i] >= DO_NOT_REDISTRIBUTE) {
//                i++;
//            }
//            int toRedistribute = counts[i];
//            do {
//                // Move children to the next node, up to SPAN
//                int oldNext = counts[i+1];
//                int newNext = counts[i] = Math.min(oldNext + toRedistribute, SPAN);
//                toRedistribute -= newNext - oldNext;
//                i++;
//            } while (toRedistribute > 0);
//            System.arraycopy(counts, i+1, counts, i, --finalLength + --i);
//        }
//
//        state.out = new Node[finalLength];
//        return counts;
//    }
//
//    private void executeConcatPlan(RebalanceState state, int[] counts, int shift) {
//        Object[] in = state.in;
//        Object[] out = state.out;
//        int j = 0, offset = 0;
//
//        for (int i = 0; i < out.length; i++) {
//            Node oldNode = (Node) in[j];
//            int newSize = counts[i];
//            int oldSize = oldNode.children.length;
//
//            if (offset == 0 && newSize == oldSize) {
//                j++;
//                out[i] = oldNode;
//            }
//            else {
//                Object[] newNodeChildren = new Object[newSize];
//                for (int curSize = 0; curSize < newSize; ) {
//                    oldNode = (Node) in[j];
//                    int remainingOldSize = oldSize - offset;
//                    int remainingNewSpace = newSize - curSize;
//
//                    // TODO: We have up to 2*SPAN children and 2*SPAN*SPAN grandchildren
//
//                    if (remainingNewSpace >= remainingOldSize) {
//                        // Empty the rest of old node into new node
//                        System.arraycopy(oldNode.children, offset, newNodeChildren, curSize, remainingOldSize);
//                        curSize += remainingOldSize;
//                        offset = 0;
//                        j++;
//                    }
//                    else {
//                        // Fill the rest of new node with old node
//                        System.arraycopy(oldNode.children, offset, newNodeChildren, curSize, remainingNewSpace);
//                        offset += remainingNewSpace;
//                        break;
//                    }
//                }
//                if (shift > 0) {
//                    // TODO: avoid creating size tables / sized nodes if not needed
//                    Sizes sizes = Sizes.of(null, shift, newSize); // TODO: parent
//                    SizedParentNode sn = new SizedParentNode(null, newNodeChildren, sizes); // TODO: parent
//                    setSizes(sn, shift - SHIFT);
//                    out[i] = sn;
//                }
//                else {
//                    out[i] = new Node(new WeakReference<>(null), newNodeChildren); // TODO: parent
//                }
//            }
//        }
//    }
//
//    private static void setSizes(SizedNode node, int shift) {
//        Object[] children = node.children;
//        Sizes sizes = node.sizes(); // TODO: Owned?
//        int childShift = shift - SHIFT;
//        int sum = 0;
//        for (int i = 0; i < children.length; i++) {
//            sizes.set(i, sum += sizeSubTree((Node) children[i], childShift));
//        }
//    }
//
//    private static int sizeSubTree(Node node, int shift) {
//        if (node instanceof SizedNode sn) {
//            return sn.sizes().get(node.children.length-1);
//        }
//        int size = 0;
//        for (; shift > 0; shift -= SHIFT) {
//            int last = node.children.length-1;
//            size += (last << shift);
//            node = (Node) node.children[last];
//        }
//        return size + node.children.length;
//    }
    
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
        // TODO
        throw new UnsupportedOperationException();
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
            return false;
        }
        
        @Override
        public boolean join(int index, Collection<? extends E> other) {
            return false;
        }
        
        @Override
        public ForkJoinList<E> fork() {
            return null;
        }
        
        @Override
        public E get(int index) {
            return null;
        }
        
        @Override
        public int size() {
            return 0;
        }
        
        @Override
        public ForkJoinList<E> subList(int fromIndex, int toIndex) {
            return null;
        }
    }
    
    private static class Node {
        // NOTE: Type of owns (and related operations) need to be co-updated with SPAN:
        // SPAN=16 -> short
        // SPAN=32 -> int
        // SPAN=64 -> long
        // SPAN>64 -> long[]
        short owns; // Not used on root leaf node (ie when rootShift == 0)
        Object[] children;
        
        Node(short owns, Object[] children) {
            this.owns = owns;
            this.children = children;
        }
        
        Node(Object[] children) {
            this.owns = 0;
            this.children = children;
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
        
        static short skipOwnership(short owns, int skip, int keep) {
            return (short) ((owns >>> skip) & mask(keep));
        }
        
        static short takeOwnership(int keep, int take, short owns) {
            return (short) ((owns & mask(take)) << keep);
        }
        
        static int mask(int shift) {
            return (1 << shift) - 1;
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
            if (isOwned) {
                return this;
            }
            return new Node(children.clone());
        }
        
        // When does a direct-child end up a SizedNode?
        //  - For all but the last child:
        //    - It is not full after rebalancing
        //    - It was already a SizedNode AND didn't give away its non-full children, OR
        //    - It picked up N children from a SizedNode AND their cumulative size < expected (N << shift)
        //      - (This also means a SizedNode can become a normal Node, if its non-full children are shifted off)
        //  - For the last child:
        //    - It is not full after rebalancing AND is not the rightmost node at its level in the tree
        //    - It was already a SizedNode AND didn't give away its non-full children, OR
        
        Node copy(boolean isOwned, int skip, int keep, int take, Node from,
                  boolean isFromOwned, boolean isRightMost, int shift) {
            assert take > 0;
            
            int newLen = keep + take;
            
            // Handle sizes
            Sizes newSizes = null;
            if (from instanceof SizedNode sn) {
                Sizes fromSizes = sn.sizes();
                // if isRightMost, we must be sized because we're taking all of from, and from is sized
                // else if newLen < SPAN (and !isRightMost), we must be sized because we're leaving a gap
                // else if taken children are not full (and !isRightMost), we must be sized because we're leaving a gap
                if (isRightMost || newLen < SPAN || fromSizes.get(take-1) != (take << shift)) {
                    newSizes = Sizes.of(shift, newLen);
                    int lastSize = newSizes.fill(0, keep);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize += fromSizes.get(i));
                    }
                }
            }
            else if (!isRightMost && newLen < SPAN) {
                newSizes = Sizes.of(shift, newLen);
                newSizes.fill(0, newLen);
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
            short newOwns = (short) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, from.owns) : 0));
            if (newSizes != null) {
                return new SizedNode(newOwns, newChildren, newSizes);
            }
            if (isOwned) {
                owns = newOwns;
                children = newChildren;
                return this;
            }
            return new Node(newOwns, newChildren);
        }
        
        Node copySized(boolean isOwned, int shift) {
            if (shift == 0) {
                return copy(isOwned);
            }
            int oldLen = children.length;
            Sizes sizes = Sizes.of(shift, oldLen);
            sizes.fill(0, oldLen);
            if (isOwned) {
                return new SizedNode(owns, children, sizes);
            }
            return new SizedNode(children.clone(), sizes);
        }
        
        Node copySized(boolean isOwned, int len, int shift) {
            if (shift == 0) {
                return copy(isOwned, len);
            }
            int oldLen = children.length;
            Sizes sizes = Sizes.of(shift, len);
            sizes.fill(0, oldLen);
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                if (isOwned) {
                    return new SizedNode(owns, newChildren, sizes);
                }
                return new SizedNode(newChildren, sizes);
            }
            if (isOwned) {
                return new SizedNode(owns, children, sizes);
            }
            return new SizedNode(children.clone(), sizes);
        }
        
        // TODO: Methods for 'decrementAndGetChild(i)' 'incrementAndGetChild(i, addedSize)' would probably be handy
        
        Object getEditableChild(int i) {
            Object child = children[i];
            boolean isOwned = claim(i);
            return children[i] = switch (child) {
                case Node n -> n.copy(isOwned);
                case Object[] arr -> isOwned ? arr : arr.clone();
                default -> throw new AssertionError();
            };
        }
        
        Object getEditableChild(int i, int len) {
            Object child = children[i];
            boolean isOwned = claim(i);
            return children[i] = switch (child) {
                case Node n -> n.copy(isOwned, len);
                case Object[] arr -> arr.length != len ? Arrays.copyOf(arr, len) : !isOwned ? arr.clone() : arr;
                default -> throw new AssertionError();
            };
        }
        
        Object getEditableChild(int i, int skip, int keep, int take, Node from,
                                boolean isFromOwned, boolean isRightMost, int shift) {
            // TODO: This method is only called on nodes that are grandparents
            Node child = (Node) children[i];
            boolean isOwned = claim(i);
            return children[i] = child.copy(isOwned, skip, keep, take, from, isFromOwned, isRightMost, shift);
        }
        
        Object getSizedEditableChild(int i, int shift) {
            Object child = children[i];
            boolean isOwned = claim(i);
            return children[i] = switch (child) {
                case Node n -> n.copySized(isOwned, shift);
                case Object[] arr -> isOwned ? arr : arr.clone();
                default -> throw new AssertionError();
            };
        }
        
        Object getSizedEditableChild(int i, int len, int shift) {
            Object child = children[i];
            boolean isOwned = claim(i);
            return children[i] = switch (child) {
                case Node n -> n.copySized(isOwned, len, shift);
                case Object[] arr -> arr.length != len ? Arrays.copyOf(arr, len) : !isOwned ? arr.clone() : arr;
                default -> throw new AssertionError();
            };
        }
    }
    
    private static class SizedNode extends Node {
        // TODO: Weigh the cost of storing Sizes directly, instead of allocating every time we touch sizes.
        Object sizes;
        
        SizedNode(short owns, Object[] children, Sizes sizes) {
            super(owns, children);
            this.sizes = sizes.unwrap();
        }
        
        SizedNode(Object[] children, Sizes sizes) {
            super(children);
            this.sizes = sizes.unwrap();
        }
        
        Sizes sizes() {
            return Sizes.wrap(sizes);
        }
        
        @Override
        SizedNode copy(boolean isOwned) {
            return isOwned ? this : new SizedNode(children.clone(), Sizes.wrap(sizes).copy());
        }
        
        @Override
        SizedNode copy(boolean isOwned, int len) {
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                Sizes newSizes = sizes().copy(len);
                if (isOwned) {
                    children = newChildren;
                    sizes = newSizes.unwrap();
                    return this;
                }
                return new SizedNode(newChildren, newSizes);
            }
            if (isOwned) {
                return this;
            }
            return new SizedNode(children.clone(), sizes().copy());
        }
        
        @Override
        Node copy(boolean isOwned, int skip, int keep, int take, Node from,
                  boolean isFromOwned, boolean isRightMost, int shift) {
            assert take > 0;
            
            int oldLen = skip + keep;
            int newLen = keep + take;
            Sizes oldSizes = sizes();
            
            // Handle sizes
            int lastSize = oldSizes.get(oldLen-1);
            Sizes newSizes = skip == 0 || (lastSize -= oldSizes.get(skip-1)) != (keep << shift)
                ? skip != take
                    ? oldSizes.copyOfRange(skip, skip + newLen)
                    : (isOwned ? oldSizes : Sizes.of(shift, newLen)).arrayCopy(oldSizes, skip, keep)
                : null;
            
            if (from instanceof SizedNode sn) {
                Sizes fromSizes = sn.sizes();
                if (newSizes != null) {
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize += fromSizes.get(i));
                    }
                }
                // if isRightMost, we must be sized because we're taking all of from, and from is sized
                // else if newLen < SPAN (and !isRightMost), we must be sized because we're leaving a gap
                // else if taken children are not full (and !isRightMost), we must be sized because we're leaving a gap
                else if (isRightMost || newLen < SPAN || fromSizes.get(take-1) != (take << shift)) {
                    newSizes = Sizes.of(shift, newLen);
                    lastSize = newSizes.fill(0, keep);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize += fromSizes.get(i));
                    }
                }
            }
            else if (newSizes != null) {
                newSizes.fill(keep, newLen);
            }
            else if (!isRightMost && newLen < SPAN) {
                newSizes = Sizes.of(shift, newLen);
                newSizes.fill(0, newLen);
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
            short newOwns = (short) ((isOwned ? skipOwnership(owns, skip, keep) : 0) | (isFromOwned ? takeOwnership(keep, take, from.owns) : 0));
            if (newSizes == null) {
                return new Node(newOwns, newChildren);
            }
            if (!isOwned) {
                return new SizedNode(newOwns, newChildren, newSizes);
            }
            owns = newOwns;
            children = newChildren;
            sizes = newSizes.unwrap();
            return this;
        }
        
        @Override
        SizedNode copySized(boolean isOwned, int shift) {
            return copy(isOwned);
        }
        
        @Override
        SizedNode copySized(boolean isOwned, int len, int shift) {
            return copy(isOwned, len);
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
        abstract int fill(int from, int to);
        
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
                    sizes = Arrays.copyOfRange(sizes, from, to);
                }
                else {
                    int oldLen = sizes.length;
                    var initialSize = sizes[from-1];
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < oldLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfByte arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                byte[] other = (byte[]) src.unwrap();
                var initialSize = other[srcPos-1];
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (byte) (lastSize += (1 << SPAN));
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
                    sizes = Arrays.copyOfRange(sizes, from, to);
                }
                else {
                    int oldLen = sizes.length;
                    var initialSize = sizes[from-1];
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < oldLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfShort arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                char[] other = (char[]) src.unwrap();
                var initialSize = other[srcPos-1];
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (char) (lastSize += (1 << SPAN));
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
                    sizes = Arrays.copyOfRange(sizes, from, to);
                }
                else {
                    int oldLen = sizes.length;
                    var initialSize = sizes[from-1];
                    sizes = Arrays.copyOfRange(sizes, from, to);
                    for (int i = 0; i < oldLen; i++) {
                        sizes[i] -= initialSize;
                    }
                }
                return this;
            }
            
            OfInt arrayCopy(Sizes src, int srcPos, int len) {
                assert srcPos > 0;
                int[] other = (int[]) src.unwrap();
                var initialSize = other[srcPos-1];
                System.arraycopy(other, srcPos, sizes, 0, len);
                for (int i = 0; i < len; i++) {
                    sizes[i] -= initialSize;
                }
                return this;
            }
            
            int fill(int from, int to) {
                int lastSize = from == 0 ? -1 : sizes[from-1];
                for (int i = from; i < to; i++) {
                    sizes[i] = (lastSize += (1 << SPAN));
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
    
    // If I have to copy a node, that means I didn't own it, which means I didn't own its children either
    // If I own a node, but replace it, I might be able to transfer ownership of its children, either by
    // (a) updating the parentId of the children, OR (b) reissuing the parent.id to the replacement parent.
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // -> If I am not sized, none of my progeny are sized
}
