#!/usr/bin/env python

'''
request_parser.py - Objects for parsing UserRequests.

This module provides:
    * TreeCollapser - Minimises the nesting of a UserRequest.

Author: Eric Saunders
June 2012
'''

class TreeCollapser(object):
    '''
        Takes a User Request and reduces it to the smallest possible nesting that
        preserves the semantics of that request. Usage:

        * Provide the User Request as a constructor argument.
        * Call the collapse_tree() method.
        * Pick up the minimal tree at self.collapsed_tree.
        * If the tree could not be collapsed, self.input_tree == self.collapsed_tree,
          and self.is_collapsible will be false.

        A tree can be collapsed if all of its CompoundRequests are either:
            * ANDs or SINGLEs
            * ONEOFs or SINGLEs

        In these cases, these trees are equivalent to flat structures, with a single
        AND or ONEOF at the top-level.

        Future work: This code does not currently minimise sub-trees. If any part of
        the tree violates the above constraints, the tree will not be collapsed.
    '''

    def __init__(self, input_tree):
        self.input_tree     = input_tree
        self.first_time     = True

        # We assume the tree will collapse, until proven otherwise. Note that a
        # collapse may not change the tree, if it is already as small as it can be.
        self.is_collapsible = True
        self.collapsed_tree = {
                                'operator' : None,
                                'requests' : []
                              }

        return


    def validate_node(self, node):
        '''
            Ensure that nodes conform to a restricted set of allowed types, and
            that nodes of type single only have one child. Raise an exception if
            this isn't the case, since the tree is then not parseable.
        '''

        # Only allow a restricted set of types
        valid_operators = ('single', 'and', 'oneof')
        if node['operator'] not in valid_operators:
            error = "Provided operator '%s' is not one of %s." % (
                                                                   node['operator'],
                                                                   valid_operators
                                                                 )
            raise InvalidTreeError(error)

        # Singles should only have one child
        if node['operator'] == 'single':
            n_children = len(self.get_children(node))
            if n_children != 1:
                error = "Nodes of type 'single' must have exactly 1 child (got %d)." % (
                                                                          n_children )
                raise InvalidTreeError(error)

        # As far as we can tell, it's valid
        return True


    def is_a_node(self, node):
        '''
            Introspect the node to decide if it's a node (CompoundRequest) or a
            leaf (Request).
        '''

        if 'requests' in node:
            return True

        return False


    def get_children(self, node):
        '''
            Return all the children of this node. Only call this if you have
            previously determined this is a node, not a leaf.
        '''

        return node['requests']


    def collapse_node(self, node):
        '''
            Nodes aren't actually collapsible. What we actually do is set the tree's
            type on the root node of the tree, and then on subsequent nodes, we verify
            that each child node is of a type that still permits it's sub-leaves to
            be collapsed. If this is ever not the case, we set a flag allowing a
            caller to know that continuing this process is futile.
        '''

        if self.first_time:
            # This is the top-level, UserRequest
            self.collapsed_tree['type']            = node['type']
            self.collapsed_tree['expires']         = node['expires']
            self.collapsed_tree['timestamp']       = node['timestamp']
            self.collapsed_tree['tracking_number'] = node['tracking_number']
            self.collapsed_tree['proposal']        = node['proposal']
            self.collapsed_tree['operator']        = node['operator']

            self.first_time = False

        else:
            # Handle the case where the top-level is a single, but the next level isn't
            # They can be combined, since singles can always combine with any operator
            # Setting them equal throws away the single, and ensures the next if isn't
            # triggered
            if ( node['operator'] != self.collapsed_tree['operator'] and
                 self.collapsed_tree['operator'] == 'single' ):
                # Set the top-level to the operator of the lower level
                self.collapsed_tree['operator'] = node['operator']

            # NOTE THAT THIS IS NOT AN ELIF!
            # If the two levels don't match, and the lower level isn't a single
            # (it is implied that the top was not a single, because we would have
            # equated the operators in the previous if)
            if ( node['operator'] != self.collapsed_tree['operator'] and
                 node['operator'] != 'single' ):
                # The tree has differing types, and can't be collapsed. Give up.
                self.is_collapsible = False

        return


    def collapse_leaf(self, node):
        '''
            Take the provided node and append it to a new flat tree structure
            representing the collapsing tree.
        '''
        self.collapsed_tree['requests'].append(node)


    def collapse_tree(self):
        '''
            Public API to perform the tree collapsing. We want this because
            self.collapse is recursive on nodes, so must be passed a node as an
            argument.
        '''

        self._collapse(self.input_tree)

        return


    def _collapse(self, node):
        '''
            Recursively collapse the tree, node by node. We give up immediately if
            any node of the tree is shown to be non-collapsible.
        '''
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
                    self._collapse(child)

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

