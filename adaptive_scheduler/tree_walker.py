#!/usr/bin/env python

'''
tree_walker.py - Functional programming implementation of a tree walking algorithm.

Simple example of a general recursive tree-walking function.

Author: Eric Saunders
June 2012
'''


def is_a_node(node):
    if 'requests' in node:
        return True

    return False

def get_children(node):
    return node['requests']


node_total = 0
node_types = set()
def process_node(node):
    global node_total
    global node_types
    #print "Found a node:", node
    node_types.add(node['type'])
    node_total += 1


leaf_total = 0
def process_leaf(node):
    global leaf_total
    #print "Found a leaf:", node
    leaf_total += 1



def walk(node, is_a_node, get_children, process_node, process_leaf ):

        if is_a_node(node):
            process_node(node)
            for child in get_children(node):
                walk(child, is_a_node, get_children, process_node, process_leaf)

        else:
            process_leaf(node)
            return


def collapse_node(node):
    pass


collapsed_tree = {
                    'type'     : 'and',
                    'requests' : []
                 }
def collapse_leaf(node):
    global collapsed_tree
    collapsed_tree['requests'].append(node)




def collapse_tree(tree, is_a_node, get_children, collapse_node, collapse_leaf):

    walk(tree, is_a_node, get_children, collapse_node, collapse_leaf)

    return collapsed_tree




if __name__ == '__main__':

    import pprint

    pp = pprint.PrettyPrinter()

    tree1 = {
             'type'     : 'and',
             'requests' : [
                              {
                                'type'     : 'and',
                                'requests' : [
                                                 { 'target' : 'blah' },
                                                 { 'target' : 'blah' },
                                                 { 'target' : 'blah' },
                                             ]
                              },
                              {
                                'type'     : 'and',
                                'requests' : [
                                                 { 'target' : 'blah' },
                                                 { 'target' : 'blah' },
                                                 { 'target' : 'blah' },
                                             ]
                              },
                          ]
           }


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
