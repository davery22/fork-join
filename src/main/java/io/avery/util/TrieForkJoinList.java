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
    
//    private void disownTail() {
//        owns &= ~1;
//    }
    
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
                // Replace root with its first child (unless that child is a non-full leaf)
                // TODO: The literature didn't seem to account that we might be promoting a non-full leaf to root?
                int newRootShift = rootShift - SHIFT;
                Node child;
                if (newRootShift > 0) {
                    root = (Node) oldRoot.children[0];
                    rootShift = newRootShift;
                    if (!oldRoot.owns(0)) {
                        disownRoot();
                    }
                }
                else if ((child = (Node) oldRoot.children[0]).children.length == SPAN) {
                    root = child;
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
    
    private void addToTail(E e) {
        if (tailSize < SPAN) {
            // Could check for INITIAL_TAIL here to skip copying, but that slows down the common case
            getEditableTail().children[tailSize++] = e;
            size++;
            return;
        }
        
        pushDownTail(-1);
        claimTail();
        (tail = new Node(new Object[SPAN])).children[0] = e;
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
            ParentNode newRoot;
            Object[] children = new Object[]{ oldRoot, null };
            if (gapAtAndAboveShift <= deepestNonFullAncestorShift || oldRoot instanceof SizedParentNode) {
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
            node = getSizedEditableRoot(len+1, gapAtAndAboveShift <= oldRootShift ? oldRootShift : 0);
            if (node instanceof SizedParentNode sn) {
                Sizes sizes = sn.sizes();
                sizes.set(len, sizes.get(len-1) + oldTailSize);
            }
        }
        else {
            // Claim nodes up to the deepest non-full ancestor
            node = getSizedEditableRoot(gapAtAndAboveShift <= oldRootShift ? oldRootShift : 0);
            int lastIdx = node.children.length-1;
            for (int shift = oldRootShift; ; shift -= SHIFT) {
                if (node instanceof SizedParentNode sn) {
                    sn.sizes().inc(lastIdx, oldTailSize);
                }
                int childLen = ((Node) node.children[lastIdx]).children.length;
                int sizedShift = gapAtAndAboveShift <= shift-SHIFT ? shift-SHIFT : 0;
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
            if (gapAtAndAboveShift <= shift-SHIFT) {
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
            Node rightTail = right.tail;

            if (leftTailSize == SPAN) {
                // Push down tail and adopt right tail
                pushDownTail(-1);
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
            pushDownTail(-1);
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
            ParentNode newRoot;
            if (leftNode instanceof SizedParentNode sn) {
                Sizes sizes = Sizes.of(newRootShift, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, sn.sizes().get(sn.children.length-1));
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else if (rightNode instanceof SizedParentNode sn) {
                Sizes sizes = Sizes.of(newRootShift, 2);
                int lastSize = size - right.tailSize;
                sizes.set(0, lastSize - sn.sizes().get(sn.children.length-1));
                sizes.set(1, lastSize);
                root = newRoot = new SizedParentNode(children, sizes);
            }
            else {
                root = newRoot = new ParentNode(children);
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
        boolean skipRight0;
    }
    
    private static final ParentNode EMPTY_NODE = new ParentNode(new Object[0]);
    
    private static void concatSubTree(ConcatState state, int leftShift, int rightShift, boolean isRightMost) {
        // TODO: Avoid editable if no rebalancing needed?
        Node left = state.left, right = state.right;
        if (leftShift > rightShift) {
            state.left = left.getEditableChild(left.children.length-1);
            concatSubTree(state, leftShift - SHIFT, rightShift, isRightMost);
            left.children[left.children.length-1] = state.left;
            // Right child may be empty after recursive call - due to rebalancing - but if not we introduce a parent
            if (state.right != EMPTY_NODE) {
                ParentNode newRight;
                if (state.right instanceof SizedParentNode sn) {
                    Sizes sizes = Sizes.of(leftShift, 1);
                    sizes.set(0, sn.sizes().get(sn.children.length-1));
                    (newRight = new SizedParentNode(new Object[]{ state.right }, sizes)).claim(0);
                }
                else {
                    (newRight = new ParentNode(new Object[]{ state.right })).claim(0);
                }
                right = newRight;
            }
        }
        else if (leftShift < rightShift) {
            state.right = right.getEditableChild(0);
            concatSubTree(state, leftShift, rightShift - SHIFT, isRightMost && right.children.length == 1);
            // Left child is never empty after recursive call, because we introduce a parent for a non-empty child,
            // and we always rebalance by shifting left, so as we come up from recursion the left remains non-empty
            ParentNode newLeft;
            if (state.left instanceof SizedParentNode sn) {
                Sizes sizes = Sizes.of(rightShift, 1);
                sizes.set(0, sn.sizes().get(sn.children.length-1));
                (newLeft = new SizedParentNode(new Object[]{ state.left }, sizes)).claim(0);
            }
            else {
                (newLeft = new ParentNode(new Object[]{ state.left })).claim(0);
            }
            left = newLeft;
            right.children[0] = state.right; // This might be EMPTY_NODE
        }
        else if (leftShift > 0) {
            state.left = left.getEditableChild(left.children.length-1);
            state.right = right.getEditableChild(0);
            concatSubTree(state, leftShift - SHIFT, rightShift - SHIFT, isRightMost && right.children.length == 1);
            left.children[left.children.length-1] = state.left;
            right.children[0] = state.right; // This might be EMPTY_NODE
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
        
        ParentNode left = (ParentNode) state.left, right = (ParentNode) state.right;
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
            if (right == EMPTY_NODE) {
                // Right is empty
                left = left.refreshSizesIfRightmostChildChanged(isRightMost, shift);
            }
            else if (state.skipRight0) {
                if (rightChildren.length == 1) {
                    // Removing right's first child empties it
                    left = left.refreshSizesIfRightmostChildChanged(isRightMost, shift);
                    right = EMPTY_NODE;
                }
                else if (leftChildren.length + rightChildren.length-1 <= SPAN) {
                    // Removing right's first child makes it fit in left
                    left.shiftChildren(0, 0, rightChildren.length-1, right, 1);
                    left = left.refreshSizes(isRightMost, shift);
                    right = EMPTY_NODE;
                }
                else {
                    // Removing right's first child does not make it fit in left
                    left = left.refreshSizesIfRightmostChildChanged(false, shift);
                    right.shiftChildren(1, 0, 0, null, 0);
                    right = right.refreshSizes(isRightMost, shift);
                }
            }
            else if (leftChildren.length + rightChildren.length <= SPAN) {
                // Right fits in left
                left.shiftChildren(0, 0, rightChildren.length, right, 0);
                left = left.refreshSizes(isRightMost, shift);
                right = EMPTY_NODE;
            }
            else {
                // Right does not fit in left
                left = left.refreshSizesIfRightmostChildChanged(false, shift);
                right = right.refreshSizesIfLeftmostChildChanged(isRightMost, shift);
            }
            
            state.left = left;
            state.skipRight0 = (state.right = right) == EMPTY_NODE;
            return;
        }
        
        boolean skipRight0 = state.skipRight0, updatedLeft = false;
        long leftDeathRow = 0, rightDeathRow = skipRight0 ? 1 : 0;
        int childShift = shift - SHIFT;
        int i = 0, step = 1;
        int llen = leftChildren.length, rlen = leftChildren.length;
        ParentNode lNode = left, rNode = left;
        Node lastNode = !isRightMost ? null : (Node) (rightChildren.length > (skipRight0 ? 1 : 0) ? rightChildren[rightChildren.length-1] : leftChildren[leftChildren.length-1]);
        Object[] lchildren = leftChildren, rchildren = leftChildren;
        
        do {
            int curLen;
            while ((curLen = ((Node) lchildren[i]).children.length) >= DO_NOT_REDISTRIBUTE) {
                if ((i += step) >= llen) {
                    // We will hit this case at most once
                    if ((i -= llen) == 0 && skipRight0) {
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
                    if ((j -= llen) == 0 && skipRight0) {
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
        } while (--curLength > maxLength);
        
        if (right == EMPTY_NODE) { // Did not update right - must have updated left only (and leftDeathRow != 0)
            // Right is empty
            left.shiftChildren(leftDeathRow, 0, 0, null, 0);
            left = left.refreshSizes(isRightMost, shift);
        }
        else if (rightDeathRow == 0) { // Did not update right - must have updated left only (and leftDeathRow != 0)
            int remainingLeft = leftChildren.length - Long.bitCount(leftDeathRow);
            if (remainingLeft + rightChildren.length <= SPAN) {
                // Removing some from left makes right fit in
                left.shiftChildren(leftDeathRow, 0, rightChildren.length, right, 0);
                left = left.refreshSizes(isRightMost, shift);
                right = EMPTY_NODE;
            }
            else {
                // Removing some from left does not make right fit in
                left.shiftChildren(leftDeathRow, 0, 0, null, 0);
                left = left.refreshSizes(false, shift);
                right = right.refreshSizesIfLeftmostChildChanged(isRightMost, shift);
            }
        }
        else if (!updatedLeft) { // Updated right only
            int remainingRight = rightChildren.length - Long.bitCount(rightDeathRow);
            if (leftChildren.length + remainingRight <= SPAN) {
                // Removing some from right makes it fit in left
                left.shiftChildren(0, 0, remainingRight, right, rightDeathRow);
                left = left.refreshSizes(isRightMost, shift);
                right = EMPTY_NODE;
            }
            else {
                // Removing some from right does not make it fit in left
                right.shiftChildren(rightDeathRow, 0, 0, null, 0);
                right = right.refreshSizes(isRightMost, shift);
                left = left.refreshSizesIfRightmostChildChanged(false, shift);
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
                left = left.refreshSizes(isRightMost, shift);
                right = EMPTY_NODE;
            }
            else {
                left = left.refreshSizes(false, shift);
                right.shiftChildren(rightDeathRow, xfer, 0, null, 0);
                right = right.refreshSizes(isRightMost, shift);
            }
        }
        
        state.left = left;
        state.skipRight0 = (state.right = right) == EMPTY_NODE;
    }
    
    // At the end of rebalance(), state.skipRight0 is true IFF state.right == EMPTY_NODE
    // In higher level concatSubTree, state.right may stay EMPTY_NODE (if now above root), or state.right = parentRight
    // On subsequent rebalance:
    //   If state.right == EMPTY_NODE (skipRight0 is true):
    //     If no rebalancing needed, both stay same
    //     If rebalancing needed, both stay same
    //   If state.right != EMPTY_NODE and skipRight0 is true:
    //     If no rebalancing needed, right shifts and refreshes sizes, right/skipRight0 recalculated
    //     If rebalancing needed, rightDeathRow != 0, right shifts and refreshes sizes, right/skipRight0 recalculated
    //   If state.right != EMPTY_NODE and skipRight0 is false:
    //     If no rebalancing needed, right refreshes sizes, skipRight0 = false
    //     If rebalancing needed, right possibly shifts and always refreshes sizes, right/skipRight0 recalculated
    
    // TODO: We should only call Node.refreshSizes() if at least one child changed
    // TODO: [O] We should only call Node.shiftChildren() if deathRow != 0 OR skip != 0 OR take != 0
    //  - If we don't need refreshSizes(), we don't need shiftChildren()
    //  - We only want state to indicate that a node changed if it actually did (in a way meaningful to higher level)
    //  - [X] We don't want to copy (in shiftChildren()) if no children changed
    //  - We ideally don't want to scan children (in refreshSizes()) if only one known child changed
    
    // "changed" -> "my parent needs to call refreshSizes"
    // I don't know what my parent is though
    // If it is SizedNode, then any change might be a reason for it to recalculate sizes
    //  - Except maybe if I was Node and stayed Node ?
    // If it is Node, then only if I changed to SizedNode
    //  - Again may be fine if I was Node and stayed Node
    
    // TODO: Update left/right:
    //  - Apply effects of resultant children from lower-level concatSubTree() (may affect parent size / sized-ness)
    //  - Remove deathRow children from left/right, and empty right to left if possible
    //    - If isRightMost, this can make left lose sized-ness
    //  - Mark whether left/right updated, for higher-level (ignore right if empty - will be handled by death row)
    
    // A: If we shift over as much as possible, we ensure that the left node will not be revisited in higher rebalancing
    //  - but we might not need higher rebalancing anyway, or, we might need to revisit the right node
    // B: If we only shift if we can empty, we avoid doing additional work now - particularly if one side is unchanged
    //  - but we might need to revisit one or both children in higher rebalancing
    //
    // Assuming we will need higher rebalancing:
    //  A avoids revisiting left (but may need to revisit right) (+0/1)
    //  B may need to revisit left and/or right (it will have visited one or both already) (+0/1/2)
    // Assuming we will NOT need higher rebalancing:
    //  A may visit left or right when it didn't need to (left was unchanged but not-full, or right was unchanged) (+0/1)
    //  B avoids unnecessary visits (+0)
    //
    // B wins if we don't need higher rebalancing, OR only visit one side
    // A wins if we do need higher rebalancing AND need to visit both sides
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
        
        Node copySized(boolean isOwned, int shift) {
            return copy(isOwned);
        }
        
        Node copySized(boolean isOwned, int len, int shift) {
            return copy(isOwned, len);
        }
        
        Node copy(boolean isOwned, int skip, int keep, int take, Node from,
                  boolean isFromOwned, boolean isRightMost, int shift) {
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
        short owns; // Not used on root leaf node (ie when rootShift == 0)
        
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
        
        // When does a direct-child end up a SizedNode?
        //  - For all but the last child:
        //    - It is not full after rebalancing
        //    - It was already a SizedNode AND didn't give away its non-full children, OR
        //    - It picked up N children from a SizedNode AND their cumulative size < expected (N << shift)
        //      - (This also means a SizedNode can become a normal Node, if its non-full children are shifted off)
        //  - For the last child:
        //    - It is not full after rebalancing AND is not the rightmost node at its level in the tree
        //    - It was already a SizedNode AND didn't give away its non-full children, OR
        
        @Override
        ParentNode copy(boolean isOwned, int skip, int keep, int take, Node from,
                        boolean isFromOwned, boolean isRightMost, int shift) {
            assert take > 0;
            int newLen = keep + take;
            
            // Handle sizes
            Sizes newSizes = null;
            if (from instanceof SizedParentNode sn) {
                Sizes fromSizes = sn.sizes();
                // if isRightMost, we must be sized because we're taking all of from, and from is sized
                // else if newLen < SPAN (and !isRightMost), we must be sized because we're leaving a gap
                // else if taken children are not full (and !isRightMost), we must be sized because we're leaving a gap
                if (isRightMost || newLen < SPAN || fromSizes.get(take-1) != (take << shift)) {
                    newSizes = Sizes.of(shift, newLen);
                    int lastSize = newSizes.fill(0, keep, shift);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize += fromSizes.get(i));
                    }
                }
            }
            else if (!isRightMost && newLen < SPAN) {
                newSizes = Sizes.of(shift, newLen);
                newSizes.fill(0, newLen, shift);
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
        
        // When do I end up Sized?
        //  - If currently not Sized, then when:
        //     1. any of my children become Sized, OR
        //     2. !rightMost and !full, OR
        //     3. I take from Sized, and (rightMost or !full or amount taken != expected)
        //
        //  - If currently Sized, then when:
        //     1. any of my children stay Sized [*], OR
        //     2. !rightMost and !full, OR
        //     3. I take from Sized, and (rightMost or !full or amount taken != expected)
        //
        // When do I end up not Sized?
        //  - If currently not Sized, then when:
        //     1. all of my children stay not Sized, AND
        //     2. rightMost or full, AND
        //     3. I don't take from Sized, or (!rightMost and full and amount taken == expected)
        //
        //  - If currently Sized, then when:
        //     1. all of my children become not Sized [*], AND
        //     2. rightMost or full, AND
        //     3. I don't take from Sized, or (!rightMost and full and amount taken == expected)
        //
        // [*] If currently Sized, how to tell if any of my children stay Sized / all become not Sized?
        //  - all are not Sized if (rightMost ? full-up-to-last and last-is-not-Sized : full)
        //  - else at least one is Sized
        
        // TODO: We can't actually count on from being Sized/!Sized, because its child changing may have changed that
        
        // For MARGIN > 0, !SIZED right node's leftmost child will never become SIZED, because either the child is
        // fully-dense - in which case rebalancing would find no nodes to reclaim from it - or it is leftwise-dense
        // - in which case rebalancing would find at most one node to reclaim from it, which would not be necessary to
        // reclaim if MARGIN > 0
        
        // SIZED right node's leftmost child will never become !SIZED (if not already),
        
        // After rebalancing, we don't even know where the sized-ness is - eg left node's rightmost child may have
        // transferred all of its sized-ness (Sized children) to the preceding node, such that it is no longer Sized,
        // but the preceding node is.
        
        // it seems most realistic to just pass over the children again to decide if left/right is Sized
        
        void shiftChildren(long deathRow, int skip, int take, ParentNode from, long fromDeathRow) {
            // Assume this and from are owned
            // At most one of skip/take will be non-zero
            // If take == 0, from/fromDeathRow are unused
            
            Object[] oldChildren = children;
            int len = children.length;
            
            // Remove deleted children
            int keep, newLen;
            short newOwns = owns;
            Object[] newChildren;
            if (deathRow != 0) {
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
                newChildren = Arrays.copyOfRange(oldChildren, skip, newLen = (keep = len - skip) + take);
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
        
        static Sizes computeSizes(Object[] children, boolean isRightMost, int shift) {
            // Scan children to determine if we are now sized + compute sizes if so
            int len = children.length;
            Sizes sizes = null;
            if (!isRightMost && len < SPAN) {
                // There is a gap after us, so we must be sized
                sizes = Sizes.of(shift, len);
                int lastSize = 0;
                if (shift == SHIFT) {
                    for (int i = 0; i < len; i++) {
                        int currSize = ((Node) children[i]).children.length;
                        sizes.set(i, lastSize += currSize);
                    }
                }
                else {
                    for (int i = 0; i < len; i++) {
                        int currSize = children[i] instanceof SizedParentNode sn ? sn.sizes().get(sn.children.length-1) : (1 << shift);
                        sizes.set(i, lastSize += currSize);
                    }
                }
            }
            else if (shift == SHIFT) {
                // We are sized if any children are not full (excepting last, if rightmost)
                for (int i = 0; i < len; i++) {
                    Node child = (Node) children[i];
                    if (child.children.length < SPAN && (!isRightMost || i < len-1)) {
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
                // We are sized if any children are sized
                for (int i = 0; i < len; i++) {
                    if (children[i] instanceof SizedParentNode sn0) {
                        sizes = Sizes.of(shift, len);
                        int lastSize = sizes.fill(0, i, shift);
                        sizes.set(i, lastSize += sn0.sizes().get(sn0.children.length-1));
                        while (++i < len) {
                            Object child = children[i];
                            int currSize = child instanceof SizedParentNode sn ? sn.sizes().get(sn.children.length-1)
                                : (i == len-1 && isRightMost) ? sizeSubTree((Node) child, shift - SHIFT)
                                : (1 << shift);
                            sizes.set(i, lastSize += currSize);
                        }
                        break;
                    }
                }
            }
            return sizes;
        }
        
        ParentNode refreshSizes(boolean isRightMost, int shift) {
            Sizes newSizes = computeSizes(children, isRightMost, shift);
            if (newSizes != null) {
                return new SizedParentNode(owns, children, newSizes);
            }
            return this;
        }
        
        ParentNode refreshSizesIfRightmostChildChanged(boolean isRightMost, int shift) {
            // Only called on a left node
            if (shift == SHIFT) {
                // Leaf-level rebalance is a no-op - child is not changed
                return this;
            }
            Object[] oldChildren = children;
            int len = oldChildren.length;
            if (oldChildren[len-1] instanceof SizedParentNode sn) {
                Sizes newSizes = Sizes.of(shift, len);
                int lastSize = newSizes.fill(0, len-1, shift);
                newSizes.set(len-1, lastSize + sn.sizes().get(sn.children.length-1));
                return new SizedParentNode(owns, oldChildren, newSizes);
            }
            return this;
        }
        
        ParentNode refreshSizesIfLeftmostChildChanged(boolean isRightMost, int shift) {
            // Only called on a right node
            if (shift == SHIFT) {
                // Leaf-level rebalance is a no-op - child is not changed
                return this;
            }
            Object[] oldChildren = children;
            if (oldChildren[0] instanceof SizedParentNode sn) {
                int len = oldChildren.length;
                Sizes newSizes = Sizes.of(shift, len);
                newSizes.set(0, sn.sizes().get(sn.children.length-1));
                if (isRightMost && len > 1) {
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
                       boolean isFromOwned, boolean isRightMost, int shift) {
            children[i] = ((Node) children[i]).copy(claim(i), skip, keep, take, from, isFromOwned, isRightMost, shift);
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
        SizedParentNode copySized(boolean isOwned, int shift) {
            return copy(isOwned);
        }
        
        @Override
        SizedParentNode copySized(boolean isOwned, int len, int shift) {
            return copy(isOwned, len);
        }
        
        @Override
        ParentNode copy(boolean isOwned, int skip, int keep, int take, Node from,
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
            
            if (from instanceof SizedParentNode sn) {
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
                    lastSize = newSizes.fill(0, keep, shift);
                    for (int i = 0; i < take; i++) {
                        newSizes.set(keep+i, lastSize += fromSizes.get(i));
                    }
                }
            }
            else if (newSizes != null) {
                if (isRightMost) {
                    // Rightmost non-sized node may not be full - need to look down the tree
                    lastSize = newSizes.fill(keep, newLen-1, shift);
                    newSizes.set(newLen-1, lastSize + sizeSubTree((Node) from.children[take-1], shift - SHIFT));
                }
                else {
                    newSizes.fill(keep, newLen, shift);
                }
            }
            else if (!isRightMost && newLen < SPAN) {
                newSizes = Sizes.of(shift, newLen);
                newSizes.fill(0, newLen, shift);
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
        ParentNode refreshSizes(boolean isRightMost, int shift) {
            Sizes newSizes = computeSizes(children, isRightMost, shift);
            if (newSizes == null) {
                return new ParentNode(owns, children);
            }
            sizes = newSizes.unwrap();
            return this;
        }
        
        @Override
        ParentNode refreshSizesIfRightmostChildChanged(boolean isRightMost, int shift) {
            // Only called on a left node
            if (shift == SHIFT) {
                // Leaf-level rebalance is a no-op - child is not changed
                return this;
            }
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
            else if (isRightMost) {
                if (prevSize == ((len-1) << shift)) {
                    // Now full
                    return new ParentNode(owns, oldChildren);
                }
                newSize = sizeSubTree(child, shift - SHIFT);
            }
            else {
                newSize = (1 << shift);
                if (prevSize + newSize == (SPAN << shift)) {
                    // Now full
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
        ParentNode refreshSizesIfLeftmostChildChanged(boolean isRightMost, int shift) {
            // Only called on a right node
            if (shift == SHIFT) {
                // Leaf-level rebalance is a no-op - child is not changed
                return this;
            }
            Object[] oldChildren = children;
            int len = oldChildren.length;
            Node child = (Node) oldChildren[0];
            Sizes oldSizes = sizes();
            int currSize = oldSizes.get(0);
            int newSize;
            if (child instanceof SizedParentNode sn) {
                newSize = sn.sizes().get(sn.children.length-1);
            }
            else if (isRightMost) {
                if (len == 1) {
                    // Now full
                    return new ParentNode(owns, oldChildren);
                }
                newSize = (1 << shift);
                if (!(oldChildren[len-1] instanceof SizedParentNode) &&
                    oldSizes.get(len-2) - currSize + newSize == ((len-1) << shift)) {
                    // Now full
                    return new ParentNode(owns, oldChildren);
                }
            }
            else {
                newSize = (1 << shift);
                if (oldSizes.get(len-1) - currSize + newSize == (SPAN << shift)) {
                    // Now full
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
    
    // If I have to copy a node, that means I didn't own it, which means I didn't own its children either
    // If I own a node, but replace it, I might be able to transfer ownership of its children, either by
    // (a) updating the parentId of the children, OR (b) reissuing the parent.id to the replacement parent.
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // -> If I am not sized, none of my progeny are sized
}
