
class TreeWalker(object):

    def __init__(self, tree):
        self.input_tree = tree

    def is_a_node(self, node):    pass
    def get_children(self, node): pass
    def process_node(self, node): pass
    def process_leaf(self, node): pass
    def recursed(self):           pass
    def derecursed(self):         pass
    def walk(self):               self._walk(self.input_tree)

    def _walk(self, node):
            if self.is_a_node(node):
                self.process_node(node)
                for child in self.get_children(node):
                    self.recursed()
                    self._walk(child)
                    self.derecursed()

            else:
                self.process_leaf(node)
                return



class MaxDepthFinder(TreeWalker):
    def __init__(self, tree):
        TreeWalker.__init__(self, tree)
        self.current_depth = 0
        self.max_depth     = 0


    def recursed(self):
        self.current_depth += 1
        if self.current_depth > self.max_depth:
            self.max_depth = self.current_depth


    def derecursed(self):
        self.current_depth -= 1



class TupleMaxDepthFinder(MaxDepthFinder):
    '''Simple example implementation of a MaxDepthFinder.'''

    def __init__(self, tree):
        MaxDepthFinder.__init__(self, tree)


    def is_a_node(self, node):
        if isinstance(node, tuple):
            return True

        return False


    def get_children(self, node):
        return node



class RequestMaxDepthFinder(MaxDepthFinder):
    def __init__(self, tree):
        MaxDepthFinder.__init__(self, tree)


    def is_a_node(self, node):
        if 'requests' in node:
            return True

        return False


    def get_children(self, node):
        return node['requests']

