package io.avery.util;

import java.lang.ref.WeakReference;
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
    // addAll(collection) - Use collection.toArray()
    // addAll(index, collection) - Use collection.toArray()
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
    
    
    // slice_left[_rec]
    // slice_right[_rec]
    
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
        if (tailSize < SPAN) {
            (tail = tail.ensureEditableWithLen(this, SPAN)).children[tailSize++] = e;
            size++;
            return;
        }
        
        pushDownTail();
        (tail = new Node(new WeakReference<>(this), new Object[SPAN])).children[0] = e;
        tailSize = 1;
        size++;
    }
    
    private void pushDownTail() {
        Node oldRoot = root;
        if (oldRoot == null) {
            root = tail;
            return;
        }
        
        int nodesToMutate = 0;
        int nodesVisited = 0;
        int shift = rootShift;
        
        // TODO: Simplify whatever is going on here
        countNodesToMutate: {
            Node curr = oldRoot;
            for (int index = size-1; shift > SHIFT; shift -= SHIFT) {
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
                SizeTable tab = new SizeTable(new WeakReference<>(this), new int[]{ size - tailSize, size });
                newRoot = new SizedNode(new WeakReference<>(this), new Object[2], tab);
            }
            else {
                newRoot = new Node(new WeakReference<>(this), new Object[2]);
            }
            newRoot.children[0] = oldRoot;
            oldRoot.tryTransferOwnership(this, newRoot);
            pushDownTailThroughNewPath(newRoot, 1, nodesVisited);
            root = newRoot;
            rootShift += SHIFT;
        }
        else {
            pushDownTailThroughExistingPath(nodesToMutate, nodesVisited - nodesToMutate);
        }
    }
    
    private void pushDownTailThroughExistingPath(int prefixHeight, int suffixHeight) {
        Node node = root;
        Object parent = this;
        int childIdx = -1;
        int index = size-1;
        int oldTailSize = tailSize;
        
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
                sizes[sizes.length-1] = sizes[sizes.length - offset] + oldTailSize;
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
        
        pushDownTailThroughNewPath(parent, childIdx, suffixHeight);
    }
    
    private void pushDownTailThroughNewPath(Object parent, int childIdx, int height) {
        for (int i = 0; i < height; i++) {
            Node curr = new Node(new WeakReference<>(parent), new Object[1]);
            parent = setChild(parent, childIdx, curr);
            childIdx = 0;
        }
        setChild(parent, childIdx, tail);
        tail.tryTransferOwnership(this, parent);
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
    //      - I think this can only happen due to a co-mod anyway
    
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
    
    // TODO: Verify join() behaves well in the case of a non-full rightmost leaf (including leaf root) on the left
    //  Presumably the concat algorithm fixes things via size tables
    
    // TODO: Nodes with free space?
    
    // /rrb_concat
    // /concat_sub_tree
    // /rebalance
    // -create_concat_plan
    // /execute_concat_plan
    
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
            return true;
        }
        
        if (right.root == null) {
            int leftTailSize = tailSize, rightTailSize = right.tailSize;
            Node rightTail = right.tail;
            
            if (leftTailSize == SPAN) {
                // Push down tail and adopt right tail
                pushDownTail();
                tail = rightTail;
                tailSize = rightTailSize;
                size += right.size;
                return true;
            }
            
            int newTailSize = leftTailSize + rightTailSize;
            Node leftTail = tail = tail.ensureEditableWithLen(this, SPAN);
            
            if (newTailSize <= SPAN) {
                // Merge tails
                System.arraycopy(rightTail.children, 0, leftTail.children, leftTailSize, rightTailSize);
                tailSize = newTailSize;
                size += right.size;
                return true;
            }
            
            // Fill left tail from right tail; Push down; Adopt remaining right tail
            pushDownTail();
            // TODO? Makes a new node - assumes right tail would need copied anyway
            Node newTail = tail = new Node(new WeakReference<>(this), new Object[SPAN]);
            int suffixSize = SPAN - leftTailSize;
            System.arraycopy(rightTail.children,  0, leftTail.children, leftTailSize, suffixSize);
            System.arraycopy(rightTail.children, suffixSize, newTail.children, 0, tailSize = rightTailSize - suffixSize);
            size += right.size;
            return true;
        }
        
        // TODO: Since right is forked, all attempts to transfer ownership of nodes under it will fail
        //  Calls to ensureEditable() on right nodes should always copy
        //   - This assumes that the _new_ parent is passed as expected, not the old parent
        //   - child = child.ensureEditable(parent); parent = child; child = child[idx];
        //   - node.tryTransferOwnership(from, to) should be called after node.ensureEditable(from);
        //     - ie: only try if we know we own it
        //   - DO NOT tryTransferOwnership(from, to) unless we own the whole chain of parents leading to from,
        //     else we may modify shared nodes!!!
        //  It is unclear if there will be cases where right is not forked (insert?)
        
        // To reduce wasted space, trim tail to size before push-down
        if (tail.children.length != tailSize) {
            tail = tail.ensureEditableWithLen(this, tailSize);
        }
        pushDownTail();
        root = concatSubTree(this, root, rootShift, right, right.root, right.rootShift, true, true);
        tail = right.tail;
        tailSize = right.tailSize;
        size += right.size;
        return true;
    }
    
    // TODO: merge create+execute concat plan
    // TODO: skip more work if no redistribution needed
    // TODO: avoid creating size tables / sized nodes if not needed
    //  - NOTE: unless isTop, the (direct) Node returned from concatSubTree()/rebalance() is just going to be
    //    destructured anyway, so no point making a SizedNode
    //  - When would concatSubTree not need size tables?
    //    - At level 0, never need size tables
    //    - At level N, no need if wide-node is full (contains exactly M or 2M full children), OR wide-node is leftwise full with no right sibling
    //      - "full children" => all size M (works for all levels), or no sized nodes (works for level != 0)
    //      - "no right sibling" => keep track of this as we go down the tree initially
    
    private Node concatSubTree(Object leftParent, Node leftNode, int leftShift,
                               Object rightParent, Node rightNode, int rightShift,
                               boolean isTop, boolean isRightMost) {
        // TODO: Pipe parents down to ensureEditable; transfer ownership to new parents where applicable
        // TODO: Can we reuse existing nodes instead of returning new ones here?
        if (leftShift > rightShift) {
            Node centerNode = concatSubTree(leftNode, (Node) leftNode.children[leftNode.children.length-1], leftShift - SHIFT, rightParent, rightNode, rightShift, false, isRightMost);
            return rebalance(leftNode, centerNode, null, leftShift, isTop, isRightMost);
        }
        if (leftShift < rightShift) {
            Node centerNode = concatSubTree(leftParent, leftNode, leftShift, rightNode, (Node) rightNode.children[0], rightShift - SHIFT, false, isRightMost && rightNode.children.length == 1);
            return rebalance(null, centerNode, rightNode, rightShift, isTop, isRightMost);
        }
        if (leftShift > 0) {
            Node centerNode = concatSubTree(leftNode, (Node) leftNode.children[leftNode.children.length-1], leftShift - SHIFT, rightNode, (Node) rightNode.children[0], rightShift - SHIFT, false, isRightMost && rightNode.children.length == 1);
            return rebalance(leftNode, centerNode, rightNode, leftShift, isTop, isRightMost);
        }
        Node node;
        if (isTop) {
            int leftSize = leftNode.children.length, rightSize = rightNode.children.length, mergedSize = leftSize + rightSize;
            if (mergedSize == SPAN + SPAN) {
                node = new Node(new WeakReference<>(leftParent), new Object[]{ leftNode, rightNode });
                leftNode.tryTransferOwnership(leftParent, node);
                rightNode.tryTransferOwnership(rightParent, node);
                return node;
            }
            SizedNode sn;
            if (mergedSize <= SPAN) {
                // TODO: How would this case happen in practice?
                //  We know that !this.isEmpty(), so this.tailSize >= 1
                //  We know that this.rootShift == 0, so root = tail (we already pushed down tail), so leftSize = this.tailSize.
                //  We know that right.rootShift == 0 and right.root != null, so rightSize >= SPAN
                //  So mergedSize = leftSize + rightSize >= 1 + SPAN > SPAN ??
                //  Literature implied that slicing might mess things up ??
                leftNode = leftNode.ensureEditableWithLen(leftParent, mergedSize);
                System.arraycopy(rightNode.children, 0, leftNode.children, leftSize, rightSize);
                if (mergedSize == SPAN) {
                    return leftNode;
                }
                // Need another level with a size table just to indicate that the tree is not leftwise dense.
                SizeTable tab = new SizeTable(new WeakReference<>(this), new int[1]);
                sn = new SizedNode(new WeakReference<>(this), new Object[]{ leftNode }, tab);
                leftNode.tryTransferOwnership(leftParent, sn);
            }
            else {
                SizeTable tab = new SizeTable(new WeakReference<>(this), new int[2]);
                sn = new SizedNode(new WeakReference<>(this), new Object[]{ leftNode, rightNode }, tab);
                leftNode.tryTransferOwnership(leftParent, sn);
                rightNode.tryTransferOwnership(rightParent, sn);
            }
            setSizes(sn, rootShift = SHIFT);
            return sn;
        }
        return new Node(new WeakReference<>(null), new Object[]{ leftNode, rightNode }); // TODO: set children parents
    }
    
    private static class RebalanceState {
        Object[] in;
        Object[] out;
        
        RebalanceState(Node left, Node center, Node right) {
            int leftLen = left != null ? left.children.length : 0;
            int rightLen = right != null ? right.children.length : 0;
            int centerLen = center.children.length;
            in = new Node[leftLen + centerLen + rightLen];
            if (left != null) {
                System.arraycopy(left.children, 0, in, 0, leftLen);
            }
            System.arraycopy(center.children, 0, in, leftLen, centerLen);
            if (right != null) {
                System.arraycopy(right.children, 0, in, leftLen + centerLen, rightLen);
            }
        }
    }
    
    private Node rebalance(Node leftNode, Node centerNode, Node rightNode, int shift, boolean isTop, boolean isRightMost) {
        RebalanceState state = new RebalanceState(leftNode, centerNode, rightNode);
        int[] counts = createConcatPlan(state);
        executeConcatPlan(state, counts, shift);
        Object[] out = state.out;
        if (out.length <= SPAN) {
            // TODO: avoid creating size tables / sized nodes if not needed
            SizeTable tab = new SizeTable(new WeakReference<>(null), new int[out.length]);
            SizedNode sn = new SizedNode(new WeakReference<>(null), out, tab); // TODO: set children parent
            setSizes(sn, shift);
            if (isTop) {
                sn.tryTransferOwnership(null, this); // TODO: update
                rootShift = shift;
                return sn;
            }
            return new Node(new WeakReference<>(null), new Object[]{ sn }); // TODO: set child parent
        }
        int leftLen = SPAN;
        int rightLen = out.length - SPAN;
        Object[] left = new Object[leftLen];
        Object[] right = new Object[rightLen];
        System.arraycopy(out, 0, left, 0, leftLen);
        System.arraycopy(out, leftLen, right, 0, rightLen);
        // TODO: avoid creating size tables / sized nodes if not needed
        SizeTable leftTab = new SizeTable(new WeakReference<>(null), new int[leftLen]); // TODO: parent
        SizeTable rightTab = new SizeTable(new WeakReference<>(null), new int[rightLen]); // TODO: parent
        SizedNode leftSn = new SizedNode(new WeakReference<>(null), left, leftTab); // TODO: parent
        SizedNode rightSn = new SizedNode(new WeakReference<>(null), right, rightTab); // TODO: parent
        setSizes(leftSn, shift);
        setSizes(rightSn, shift);
        if (isTop) {
            rootShift = shift += SHIFT;
            SizeTable tab = new SizeTable(new WeakReference<>(this), new int[2]);
            SizedNode sn = new SizedNode(new WeakReference<>(this), new Object[]{ leftSn, rightSn }, tab);
            setSizes(sn, shift);
            return sn;
        }
        return new Node(new WeakReference<>(null), new Object[]{ leftSn, rightSn }); // TODO: [X] parent
    }
    
    private int[] createConcatPlan(RebalanceState state) {
        Object[] in = state.in;
        int[] counts = new int[in.length];
        int totalNodes = 0;
        
        for (int i = 0; i < in.length; i++) {
            totalNodes += counts[i] = ((Node) in[i]).children.length;
        }
        
        int minLength = ((totalNodes-1) / SPAN) + 1;
        int maxLength = minLength + MARGIN;
        int finalLength = in.length;
        for (int i = 0; finalLength > maxLength; ) {
            while (counts[i] >= DO_NOT_REDISTRIBUTE) {
                i++;
            }
            int toRedistribute = counts[i];
            do {
                // Move children to the next node, up to SPAN
                int oldNext = counts[i+1];
                int newNext = counts[i] = Math.min(oldNext + toRedistribute, SPAN);
                toRedistribute -= newNext - oldNext;
                i++;
            } while (toRedistribute > 0);
            System.arraycopy(counts, i+1, counts, i, --finalLength + --i);
        }
        
        state.out = new Node[finalLength];
        return counts;
    }
    
    private void executeConcatPlan(RebalanceState state, int[] counts, int shift) {
        Object[] in = state.in;
        Object[] out = state.out;
        int j = 0, offset = 0;
        
        for (int i = 0; i < out.length; i++) {
            Node oldNode = (Node) in[j];
            int newSize = counts[i];
            int oldSize = oldNode.children.length;
            
            if (offset == 0 && newSize == oldSize) {
                j++;
                out[i] = oldNode;
            }
            else {
                Object[] newNodeChildren = new Object[newSize];
                for (int curSize = 0; curSize < newSize; ) {
                    oldNode = (Node) in[j];
                    int remainingOldSize = oldSize - offset;
                    int remainingNewSpace = newSize - curSize;
                    
                    if (remainingNewSpace >= remainingOldSize) {
                        // Empty the rest of old node into new node
                        System.arraycopy(oldNode.children, offset, newNodeChildren, curSize, remainingOldSize);
                        curSize += remainingOldSize;
                        offset = 0;
                        j++;
                    }
                    else {
                        // Fill the rest of new node with old node
                        System.arraycopy(oldNode.children, offset, newNodeChildren, curSize, remainingNewSpace);
                        offset += remainingNewSpace;
                        break;
                    }
                }
                if (shift > 0) {
                    // TODO: avoid creating size tables / sized nodes if not needed
                    SizeTable tab = new SizeTable(new WeakReference<>(null), new int[newSize]); // TODO: parent
                    SizedNode sn = new SizedNode(new WeakReference<>(null), newNodeChildren, tab); // TODO: parent
                    setSizes(sn, shift - SHIFT);
                    out[i] = sn;
                }
                else {
                    out[i] = new Node(new WeakReference<>(null), newNodeChildren); // TODO: parent
                }
            }
        }
    }
    
    private static void setSizes(SizedNode node, int shift) {
        Object[] children = node.children;
        int[] sizes = node.sizeTable.sizes; // TODO: Owned?
        int childShift = shift - SHIFT;
        int sum = 0;
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = sum += sizeSubTree((Node) children[i], childShift);
        }
    }
    
    private static int sizeSubTree(Node node, int shift) {
        if (node instanceof SizedNode sn) {
            return sn.sizeTable.sizes[sn.sizeTable.sizes.length-1];
        }
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
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                if (parent.refersTo(expectedParent)) {
                    children = newChildren;
                    return this;
                }
                return new Node(new WeakReference<>(expectedParent), newChildren);
            }
            if (parent.refersTo(expectedParent)) {
                return this;
            }
            return new Node(new WeakReference<>(expectedParent), children.clone());
        }
    }
    
    private static class SizedNode extends Node {
        SizeTable sizeTable;
        
        SizedNode(WeakReference<Object> parent, Object[] children, SizeTable sizeTable) {
            super(parent, children);
            this.sizeTable = sizeTable;
        }
        
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
            // We ensure that sizeTable.sizes.length == children.length
            // We do not ensure that sizeTable.parent.get() == parent.get()
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                SizeTable newSizeTable = new SizeTable(new WeakReference<>(expectedParent), Arrays.copyOf(sizeTable.sizes, len));
                if (parent.refersTo(expectedParent)) {
                    children = newChildren;
                    sizeTable = newSizeTable;
                    return this;
                }
                return new SizedNode(new WeakReference<>(expectedParent), newChildren, newSizeTable);
            }
            if (parent.refersTo(expectedParent)) {
                return this;
            }
            return new SizedNode(new WeakReference<>(expectedParent), children.clone(), sizeTable);
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
            if (sizes.length != len) {
                int[] newSizes = Arrays.copyOf(sizes, len);
                if (parent.refersTo(expectedParent)) {
                    sizes = newSizes;
                    return this;
                }
                return new SizeTable(new WeakReference<>(expectedParent), newSizes);
            }
            if (parent.refersTo(expectedParent)) {
                return this;
            }
            return new SizeTable(new WeakReference<>(expectedParent), sizes.clone());
        }
    }
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // -> If I am not sized, none of my progeny are sized
}
