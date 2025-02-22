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
    private static final Node INITIAL_TAIL = new Node(null, new Object[SPAN]);
    
    private int size;
    private int tailSize;
    private int rootShift;
    private Node root;
    private Node tail;
    private final Id listId = new Id();
    
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
                if (node instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes;
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
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
            node = tail = tail.ensureEditable(listId);
            index -= tailOffset;
        }
        else {
            node = root;
            Object parent = this;
            Id parentId = listId;
            int childIdx = -1;
            
            for (int shift = rootShift; ; shift -= SHIFT) {
                node = node.ensureEditable(parentId);
                parent = setChild(parent, childIdx, node);
                if (shift == 0) {
                    break;
                }
                parentId = node.id();
                childIdx = index >>> shift;
                if (node instanceof SizedParentNode sn) {
                    Sizes sizes = sn.sizes;
                    while (sizes.get(childIdx) <= index) {
                        childIdx++;
                    }
                    if (childIdx > 0) {
                        index -= sizes.get(childIdx-1);
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
        
        int oldSize = size;
        int oldTailSize = tailSize;
        Node oldTail = tail;
        @SuppressWarnings("unchecked")
        E old = (E) oldTail.children[oldTailSize-1];
        
        if (oldSize == 1 && oldTail.parentId != listId) {
            tail = INITIAL_TAIL;
            tailSize = size = 0;
            return old;
        }
        
        size--;
        if (oldTailSize > 1 || oldSize == 1) {
            (tail = oldTail.ensureEditableWithLen(listId, SPAN)).children[--tailSize] = null;
            return old;
        }
        
        // oldTailSize == 1 && oldSize > 1  -->  promote new tail
        int height = rootShift/SHIFT;
        if (height == 0) {
            tailSize = (tail = root).children.length;
            root = null;
            return old;
        }
        
        // If we remove a node's only child, we remove the node itself
        // Else, we edit the node (copying it if not owned)
        // So, we don't know whether we will copy until we get to the bottom (or a node with >1 child)
        
        Id lastParentId = listId;
        Node[] path = new Node[height+1];
        path[0] = root;
        for (int i = 0; i < height; i++) {
            // Track last owned node id, in case we can transfer ownership of new tail
            lastParentId = lastParentId != null && path[i].parentId == lastParentId ? path[i].id() : null;
            path[i+1] = (Node) path[i].children[path[i].children.length-1];
        }
        
        Node newTail = path[height];
        int newTailSize = newTail.children.length;
        Object lastSurvivingParent = this;
        Id lastSurvivingParentId = listId;
        int lastSurvivingParentIdx = 0;
        int childIdx = -1;
        
        for (int i = 0; i < height; i++) {
            // We may remove this node if (len == 1) or (i == 0 and len <= 2)
            // Else, we know we will not be removing this node or its ancestors, so can ensureEditable on them
            int len = path[i].children.length;
            if (len > 2 || (len > 1 && i > 0)) {
                for (; lastSurvivingParentIdx < i; lastSurvivingParentIdx++) {
                    // Ensure the path up to current node
                    Node child = path[lastSurvivingParentIdx].ensureEditable(lastSurvivingParentId);
                    if (child instanceof SizedParentNode sn) {
                        (sn.sizes = sn.sizes.ensureEditable(lastSurvivingParentId)).inc(len-1, -newTailSize);
                    }
                    lastSurvivingParent = setChild(lastSurvivingParent, childIdx, child);
                    lastSurvivingParentId = child.id();
                    childIdx = child.children.length-1;
                }
            }
        }
        
        // Remove the path after last surviving node
        Node oldRoot;
        if (lastSurvivingParentIdx == 0 && (oldRoot = root).children.length <= 2) {
            if (oldRoot.children.length == 2) {
                Node newRoot = (Node) oldRoot.children[0];
                root = oldRoot.parentId == listId ? newRoot.tryTransferOwnership(oldRoot.id(), listId) : newRoot;
                rootShift -= SHIFT;
            }
            else {
                root = null;
                rootShift = 0; // TODO: Needed?
            }
        }
        else {
            Node child = path[lastSurvivingParentIdx].ensureEditableWithLen(lastSurvivingParentId, path[lastSurvivingParentIdx].children.length-1);
            setChild(lastSurvivingParent, childIdx, child);
        }
        
        // Promote last node to tail
        tailSize = (tail = lastParentId != null ? newTail.tryTransferOwnership(lastParentId, listId) : newTail).children.length;
        return old;
        
//        Node[] path = new Node[height+1];
//        path[0] = root;
//        for (int i = 0; i < height; i++) {
//            path[i+1] = (Node) path[i].children[path[i].children.length-1];
//        }
//        // TODO: No! Need to ensure parent is owned before we tryTransferOwnership.
//        //  "owned" => parentId == parent.id, parent.parentId == grandparent.id ... ancestor.parentId == listId
//        (theTail = tail = path[height]).tryTransferOwnership(path[height-1].id(), listId);
//        theTailSize = tailSize = theTail.children.length;
//        path[height] = null;
//
//        for (int i = height; i-- > 0; ) {
//            Node curr = path[i];
//            if (path[i+1] == null && curr.children.length == 1) {
//                path[i] = null;
//            }
//            else if (i == 0 && path[i+1] == null && curr.children.length == 2) {
//                path[i] = (Node) curr.children[0];
//                rootShift -= SHIFT;
//            }
//            else {
//                // TODO: Incorrect - parent can change on next iteration
//                Object expectedParent = i == 0 ? this : path[i-1];
//                int len = curr.children.length;
//                if (path[i+1] == null) {
//                    path[i] = curr.ensureEditableWithLen(expectedParent, --len);
//                }
//                else {
//                    // Note: This only has an effect if we copied the i+1 node
//                    (path[i] = curr.ensureEditableWithLen(expectedParent, len)).children[len-1] = path[i+1];
//                    path[i+1].tryTransferOwnership(curr, path[i]);
//                }
//                if (path[i] instanceof SizedParentNode sn) {
//                    (sn.sizes = sn.sizes.ensureEditable(expectedParent)).inc(len-1, -theTailSize);
//                }
//            }
//        }
//
//        root = path[0];
//
//        return old;
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
            if (node instanceof SizedParentNode sn) {
                Sizes tab = sn.sizes = sn.sizes.ensureEditable(parent);
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
            SizedParentNode sn;
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
                Sizes sizes = Sizes.of(this, SHIFT, 1);
                sn = new SizedParentNode(this, new Object[]{ leftNode }, sizes);
                leftNode.tryTransferOwnership(leftParent, sn);
            }
            else {
                Sizes sizes = Sizes.of(this, SHIFT, 2);
                sn = new SizedParentNode(this, new Object[]{ leftNode, rightNode }, sizes);
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
            Sizes sizes = Sizes.of(null, shift, out.length);
            SizedParentNode sn = new SizedParentNode(new WeakReference<>(null), out, sizes); // TODO: set children parent
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
        Sizes leftSizes = Sizes.of(null, shift, leftLen); // TODO: parent
        Sizes rightSizes = Sizes.of(null, shift, rightLen); // TODO: parent
        SizedParentNode leftSn = new SizedParentNode(null, left, leftSizes); // TODO: parent
        SizedParentNode rightSn = new SizedParentNode(null, right, rightSizes); // TODO: parent
        setSizes(leftSn, shift);
        setSizes(rightSn, shift);
        if (isTop) {
            rootShift = shift += SHIFT;
            Sizes sizes = Sizes.of(this, shift, 2);
            SizedParentNode sn = new SizedParentNode(this, new Object[]{ leftSn, rightSn }, sizes);
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
                    Sizes sizes = Sizes.of(null, shift, newSize); // TODO: parent
                    SizedParentNode sn = new SizedParentNode(null, newNodeChildren, sizes); // TODO: parent
                    setSizes(sn, shift - SHIFT);
                    out[i] = sn;
                }
                else {
                    out[i] = new Node(new WeakReference<>(null), newNodeChildren); // TODO: parent
                }
            }
        }
    }
    
    private static void setSizes(SizedParentNode node, int shift) {
        Object[] children = node.children;
        Sizes sizes = node.sizes; // TODO: Owned?
        int childShift = shift - SHIFT;
        int sum = 0;
        for (int i = 0, len = sizes.length(); i < len; i++) {
            sizes.set(i, sum += sizeSubTree((Node) children[i], childShift));
        }
    }
    
    private static int sizeSubTree(Node node, int shift) {
        if (node instanceof SizedParentNode sn) {
            return sn.sizes.get(sn.sizes.length()-1);
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
    
    private static class Id { }
    
    private static class Node {
        Id parentId;
        Object[] children;
        
        Node(Id parentId, Object[] children) {
            this.parentId = parentId;
            this.children = children;
        }
        
        Id id() {
            // Should only be called on a ParentNode
            throw new AssertionError();
        }
        
        Node ensureEditable(Id expectedParentId) {
            if (parentId == expectedParentId) {
                return this;
            }
            return new Node(expectedParentId, children.clone());
        }
        
        Node ensureEditableWithLen(Id expectedParentId, int len) {
            if (children.length != len) {
                Object[] newChildren = children.clone();
                if (parentId == expectedParentId) {
                    children = newChildren;
                    return this;
                }
                return new ParentNode(new Id(), expectedParentId, newChildren);
            }
            return ensureEditable(expectedParentId);
        }
        
        Node tryTransferOwnership(Id from, Id to) {
            if (parentId == from) {
                parentId = to;
            }
            return this;
        }
    }
    
    private static class ParentNode extends Node {
        Id id;
        
        ParentNode(Id id, Id parentId, Object[] children) {
            super(parentId, children);
            this.id = id;
        }
        
        @Override
        Id id() {
            return id;
        }
        
        @Override
        ParentNode ensureEditable(Id expectedParentId) {
            if (parentId == expectedParentId) {
                return this;
            }
            return new ParentNode(new Id(), expectedParentId, children.clone());
        }
        
        @Override
        ParentNode ensureEditableWithLen(Id expectedParentId, int len) {
            return (ParentNode) super.ensureEditableWithLen(expectedParentId, len);
        }
        
        @Override
        ParentNode tryTransferOwnership(Id from, Id to) {
            if (parentId == from) {
                parentId = to;
            }
            return this;
        }
    }
    
    private static class SizedParentNode extends ParentNode {
        Sizes sizes;
        
        SizedParentNode(Id id, Id parentId, Object[] children, Sizes sizes) {
            super(id, parentId, children);
            this.sizes = sizes;
        }
        
        @Override
        SizedParentNode ensureEditable(Id expectedParentId) {
            if (parentId == expectedParentId) {
                return this;
            }
            return new SizedParentNode(new Id(), expectedParentId, children.clone(), sizes);
        }
        
        @Override
        SizedParentNode ensureEditableWithLen(Id expectedParentId, int len) {
            if (children.length != len) {
                Object[] newChildren = Arrays.copyOf(children, len);
                Sizes newSizes = sizes.resize(expectedParentId, len);
                if (parentId == expectedParentId) {
                    children = newChildren;
                    sizes = newSizes;
                    return this;
                }
                return new SizedParentNode(new Id(), expectedParentId, newChildren, newSizes);
            }
            return ensureEditable(expectedParentId);
        }
        
        @Override
        SizedParentNode tryTransferOwnership(Id from, Id to) {
            if (parentId == from) {
                parentId = to;
            }
            sizes.tryTransferOwnership(from, to);
            return this;
        }
    }
    
    private static abstract class Sizes {
        Id parentId;
        
        Sizes(Id parentId) {
            this.parentId = parentId;
        }
        
        abstract int length();
        abstract int get(int i);
        abstract void set(int i, int size);
        abstract Sizes ensureEditable(Id expectedParentId);
        abstract Sizes resize(Id expectedParentId, int len);
        
        void inc(int i, int size) {
            set(i, get(i) + size);
        }
        
        Sizes tryTransferOwnership(Id from, Id to) {
            if (parentId == from) {
                parentId = to;
            }
            return this;
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
        
        static Sizes of(Id parentId, int shift, int len) {
            if ((SPAN<<shift) <= 256) {
                return new Sizes.OfByte(parentId, new byte[len]);
            }
            if ((SPAN<<shift) <= 65536) {
                return new Sizes.OfShort(parentId, new char[len]);
            }
            return new Sizes.OfInt(parentId, new int[len]);
        }
        
        static class OfByte extends Sizes {
            byte[] sizes;
            OfByte(Id parentId, byte[] sizes) {
                super(parentId);
                this.sizes = sizes;
            }
            int length() { return sizes.length; }
            int get(int i) { return Byte.toUnsignedInt(sizes[i])+1; }
            void set(int i, int size) { sizes[i] = (byte) (size-1); }
            
            OfByte ensureEditable(Id expectedParentId) {
                if (parentId == expectedParentId) {
                    return this;
                }
                return new OfByte(expectedParentId, sizes.clone());
            }
            
            OfByte resize(Id expectedParentId, int len) {
                byte[] newSizes = Arrays.copyOf(sizes, len);
                if (parentId == expectedParentId) {
                    sizes = newSizes;
                    return this;
                }
                return new OfByte(expectedParentId, newSizes);
            }
        }
        
        static class OfShort extends Sizes {
            char[] sizes;
            OfShort(Id parentId, char[] sizes) {
                super(parentId);
                this.sizes = sizes;
            }
            int length() { return sizes.length; }
            int get(int i) { return sizes[i]+1; }
            void set(int i, int size) { sizes[i] = (char) (size-1); }
            
            OfShort ensureEditable(Id expectedParentId) {
                if (parentId == expectedParentId) {
                    return this;
                }
                return new OfShort(expectedParentId, sizes.clone());
            }
            
            OfShort resize(Id expectedParentId, int len) {
                char[] newSizes = Arrays.copyOf(sizes, len);
                if (parentId == expectedParentId) {
                    sizes = newSizes;
                    return this;
                }
                return new OfShort(expectedParentId, newSizes);
            }
        }
        
        static class OfInt extends Sizes {
            int[] sizes;
            OfInt(Id parentId, int[] sizes) {
                super(parentId);
                this.sizes = sizes;
            }
            int length() { return sizes.length; }
            int get(int i) { return sizes[i]+1; }
            void set(int i, int size) { sizes[i] = size-1; }
            
            OfInt ensureEditable(Id expectedParentId) {
                if (parentId == expectedParentId) {
                    return this;
                }
                return new OfInt(expectedParentId, sizes.clone());
            }
            
            OfInt resize(Id expectedParentId, int len) {
                int[] newSizes = Arrays.copyOf(sizes, len);
                if (parentId == expectedParentId) {
                    sizes = newSizes;
                    return this;
                }
                return new OfInt(expectedParentId, newSizes);
            }
        }
    }
    
    
    // children-elements:
    //  - for SPAN=16: 16(byte=1) OR 32(short=2) OR 64(int=4)
    // Node sizes:
    //  - total/each = 52 + children-elements
    //  - sharing cost = 16
    //    - cost for 1@byte = 68
    //    - cost for 2@byte = 84
    //    - cost for 3@byte = 100 -- best
    //    - cost for 1@short = 84
    //    - cost for 2@short = 100 -- best
    //    - cost for 3@short = 116 -- best
    //    - cost for 1@int = 116
    //    - cost for 2@int = 132 -- best(tie)
    //    - cost for 3@int = 148 -- best
    //  - 16-byte pointer-to-Node
    //    - 16-byte parentId
    //    - 16-byte pointer-to-children
    //      - 4-byte children-length
    //      - children-elements
    // flat sizes with parentId:
    //  - total/each = 36 + children-elements
    //  - sharing cost = 32
    //    - cost for 1@byte = 52
    //    - cost for 2@byte = 84
    //    - cost for 3@byte = 116
    //    - cost for 1@short = 68
    //    - cost for 2@short = 104
    //    - cost for 3@short = 140
    //    - cost for 1@int = 100
    //    - cost for 2@int = 132 -- best(tie)
    //    - cost for 3@int = 164
    //  - 16-byte parentId
    //  - 16-byte pointer-to-children
    //    - 4-byte children-length
    //    - children-elements
    //  - extra 16 bytes on each Node, compared to alternatives
    // flat sizes without parentId:
    //  - total/each = 20 + children-elements
    //  - no sharing
    //    - cost for 1@byte = 36 -- best
    //    - cost for 2@byte = 72 -- best
    //    - cost for 3@byte = 108
    //    - cost for 1@short = 52 -- best
    //    - cost for 2@short = 104
    //    - cost for 3@short = 156
    //    - cost for 1@int = 84 -- best
    //    - cost for 2@int = 148
    //    - cost for 3@int = 212
    //  - 16-byte pointer-to-children
    //    - 4-byte children-length
    //    - children-elements
    //
    // Sharing only saves when children are updated (added/removed mutates sizes, so cannot share)
    
    // Plain array memory:
    //  + 4-byte length
    //  + elements
    // Practical least memory:
    //  + 16-byte parentId per node
    //  + 4-byte length per node
    //  + size table per sized parent node (16-64 bytes)
    //  + elements
    // Actual additional memory:
    //  + 16-byte pointer-to-children per node
    //  + 16-byte id per parent node
    //  + 16-byte pointer-to-size-table per sized parent node
    //  + 16-byte pointer-to-size-table-children per sized parent node
    //  + 16-byte size table parentId per sized parent node
    //  + 4-byte size table length per sized parent node
    
    // If I have to copy a node, that means I didn't own it, which means I didn't own its children either
    // If I own a node, but replace it, I might be able to transfer ownership of its children, either by
    // (a) updating the parentId of the children, OR (b) reissuing the parent.id to the replacement parent.
    
    // If I am sized, all of my ancestors are sized, but my progeny may not be
    // -> If I am not sized, none of my progeny are sized
}
