#-------------------------------------------------------------------------#
# SyncNet                                                     by Bob Hood #
#                                                 Copyright 2024 Bob Hood #
#                                                                         #
# Module: LinkedList                                                      #
#                                                                         #
# Generic single- and double-linked list implementation for efficient     #
# insertion and deletions                                                 #
#-------------------------------------------------------------------------#

__author__     = "Bob Hood"
__copyright__  = "Copyright (C) 2024 Bob Hood"
__version__    = "1.0"
__maintainer__ = "Bob Hood"
__email__      = "Bob.Hood.2@gmail.com"
__status__     = "Prototype"

class Node:
    def __init__(self, data):
        self.data = data
        self.next = None
        self.prev = None

    def __repr__(self):
        return self.data

class SingleLinkedList:
    def __init__(self):
        self.head = None

    def __repr__(self):
        node = self.head
        nodes = []
        while node is not None:
            nodes.append(node.data)
            node = node.next
        nodes.append("None")
        return " -> ".join(nodes)

    def __iter__(self):
        node = self.head
        while node is not None:
            yield node
            node = node.next

    def append(self, node):
        if self.head is None:
            self.head = node
        else:
            for current_node in self:
                pass
            current_node.next = node

    def prepend(self, node):
        node.next = self.head
        self.head = node

    def insert(self, node, after):
        if self.head is None:
            raise Exception("List is empty")

        for n in self:
            if n.data == after.data:
                next = n.next
                n.next = node
                node.next = next
                return

        raise Exception("Target node was not found")

    def remove(self, node):
        if self.head is None:
            raise Exception("List is empty")

        if self.head == node:
            self.head = self.head.next
        else:
            prev = self.head
            for n in self:
                if n == node:
                    prev.next = node.next
                    return
                prev = n

            raise Exception("Node with data '%s' not found" % node)

    def head(self):
        return self.head

    def tail(self):
        for node in self:
            pass
        return node

    def pop_head(self):
        if self.head is None:
            raise Exception("List is empty")

        self.head = self.head.next

    def pop_tail(self):
        prev = self.head
        for n in self:
            if n.next is None:
                prev.next = None
                break
            prev = n

class DoubleLinkedList:
    def __init__(self):
        self._head = None
        self._tail = None
        self._length = 0

    def __len__(self):
        return self._length

    def __repr__(self):
        node = self._head
        nodes = []
        while node is not None:
            nodes.append(node.data)
            node = node.next
        nodes.append("None")
        return " <-> ".join(nodes)

    def __iter__(self):
        node = self._head
        while node is not None:
            yield node
            node = node.next

    def append(self, node):
        if self._head is None:
            self._head = node
            self._head.prev = None
            self._tail = self._head
        else:
            self._tail.next = node
            node.prev = self._tail
            self._tail = node

        self._tail.next = None
        self._length += 1

    def prepend(self, node):
        if self._head is None:
            self._head = node
            self._head.prev = None
            self._head.next = None
            self._tail = self._head
        else:
            node.next = self._head
            self._head.prev = node
            self._head = node

        self._length += 1

    def insert(self, node, after):
        if self._head is None:
            raise Exception("List is empty")

        node.next = after.next
        node.prev = after
        after.next = node

        self._length += 1

    def remove(self, node=None, data=None):
        def _remove(node):
            if self._head == node:
                self._head = self._head.next
                if self._head is not None:
                    self._head.prev = None
            elif self._tail == node:
                self._tail = self._tail.prev
                if self._tail is not None:
                    self._tail.next = None
            else:
                node.prev.next = node.next
                node.next.prev = node.prev

        if self._head is None:
            raise Exception("List is empty")
        if (node is None) and (data is None):
            raise Exception("Data is empty")

        if node is None:
            # find it using data
            for n in self:
                if n.data == data:
                    _remove(n)
                    break
        else:
            _remove(node)

        self._length -= 1

    def is_head(self, node):
        if (node is None) or (self._head is None):
            return False
        return self._head == node

    def is_tail(self, node):
        if (node is None) or (self._tail is None):
            return False
        return self._tail == node

    def head(self):
        return self._head

    def tail(self):
        return self._tail

    def pop_head(self):
        self.remove(self._head)     # remove() decrements _length

    def pop_tail(self):
        self.remove(self._tail)     # remove() decrements _length
