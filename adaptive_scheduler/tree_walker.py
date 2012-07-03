#!/usr/bin/env python

'''
tree_walker.py - Functional programming implementation of a tree walking algorithm.

Simple example of a general recursive tree-walking function.

Author: Eric Saunders
June 2012
'''

class TreeCollapser(object):

    def __init__(self, input_tree):
        self.input_tree     = input_tree
        self.first_time     = True
        # We assume the tree will collapse, until proven otherwise. Note that a
        # collapse may not change the tree, if it is already as small as it can be.
        self.is_collapsible = True
        self.collapsed_tree = {
                                'type'     : None,
                                'requests' : []
                              }

        return


    def validate_node(self, node):

        # Only allow a restricted set of types
        valid_types = ('single', 'and', 'oneof')
        if node['type'] not in valid_types:
            error = "Provided type '%s' is not one of %s." % (
                                                               node['type'],
                                                               valid_types
                                                             )
            raise InvalidTreeError(error)

        # Singles should only have one child
        if node['type'] == 'single':
            n_children = len(self.get_children(node))
            if n_children != 1:
                error = "Nodes of type 'single' must have exactly 1 child (got %d)." % (
                                                                          n_children )
                raise InvalidTreeError(error)

        # As far as we can tell, it's valid
        return True


    def is_a_node(self, node):
        if 'requests' in node:
            return True

        return False


    def get_children(self, node):
        return node['requests']


    def collapse_node(self, node):

        if self.first_time:
            self.collapsed_tree['type'] = node['type']
            self.first_time = False

        else:
            if ( node['type'] != self.collapsed_tree['type'] and
                 node['type'] != 'single' ):
                # The tree has differing types, and can't be collapsed. Give up.
               self.is_collapsible = False


    def collapse_leaf(self, node):
        self.collapsed_tree['requests'].append(node)


    def collapse_tree(self):

        self.collapse(self.input_tree)

        return


    def collapse(self, node):
        # Only continue if we still believe the tree can be collapsed
        if self.is_collapsible:

            if self.is_a_node(node):
                self.validate_node(node)
                self.collapse_node(node)

                # If the tree has now been determined to be uncollapsible, abort.
                if not self.is_collapsible:
                    # Reset the tree, which may be partially collapsed
                    self.collapsed_tree = self.input_tree
                    return

                # Otherwise, continue collapsing recursively down the tree
                for child in self.get_children(node):
                    self.collapse(child)

            # It's a node - deal with it appropriately
            else:
                self.collapse_leaf(node)
                return



class InvalidTreeError(Exception):
    '''Raised when the tree is found to be not parseable.'''

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value







if __name__ == '__main__':

    import pprint

    pp = pprint.PrettyPrinter()

    tree2 = {
             'type'     : 'and',
             'requests' : [
                              {
                                'type'     : 'single',
                                'requests' : [
                                                 { 'target' : 'blah' },
                                             ]
                              },
                              {
                                'type'     : 'single',
                                'requests' : [
                                                 { 'target' : 'blah' },
                                             ]
                              },
                          ]
           }

    tree = tree2

    walk(tree, is_a_node, get_children, process_node, process_leaf)

    print "Found %d nodes." % node_total
    print "Found %d leaves." % leaf_total

    print node_types

    if len(node_types) == 1:
        if 'and' in node_types:
            print "All node types were of type AND. We can collapse this tree!"

            print "Collapsing tree..."
            collapsed_tree = collapse_tree(tree, is_a_node, get_children,
                                           collapse_node, collapse_leaf)
            print "Old tree was:"
            pp.pprint(tree)
            print "\nNew tree looks like this:"
            pp.pprint(collapsed_tree)

        else:
            print "All nodes were of a type other than AND. Doing nothing."

    else:
        print "Multiple types found, no collapse possible."
