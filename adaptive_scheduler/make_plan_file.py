#!/usr/bin/env python

from __future__ import division

# Deprecated. Probably more work than is justified to get something useful.

telescopes = []
selected_telescope = ''

def new_telescope():
    print "Adding new telescope"
    tel_name  = raw_input("telescope (2m0a): " )
    enc_name  = raw_input("Enclosure (clma): ")
    site_name = raw_input("Site (ogg): ")

    telescopes.append(tel_name + '.' + enc_name + '.' + site_name)

    
def select_or_add_telescope():
    if len(telescopes):
        print "Existing telescopes:"
        for n, tel in enumerate(telescopes):
            print "   (%d) %s" % (n, tel)

    print "   (%d) Add new" % len(telescopes)
    tel_choice = int(raw_input('[n]> '))

    if tel_choice == len(telescopes):
        new_telescope()
    
    global(selected_telescope) = telescopes[tel_choice]
    print "Selected telescope '%s'" % selected_telescope

    

while ( True ):
    prompt = '%s[tax]> ' % selected_telescope
    choice = raw_input(prompt)
    
    if choice == 'x':
        print telescopes
        print "Goodbye."
        exit()

    elif choice == 't':    
        select_or_add_telescope()
