#!/usr/bin/env python

from __future__ import division

def new_telescope():
    print "Adding new telescope"
    tel_name  = raw_input("telescope (2m0a): " )
    enc_name  = raw_input("Enclosure (clma): ")
    site_name = raw_input("Site (ogg): ")

    telescopes.append(tel_name + '.' + enc_name + '.' + site_name)
    
    
    
telescopes = []
prompt = '[tax]> '

while ( True ):
    choice = raw_input(prompt)
    
    if choice == 'x':
        print telescopes
        print "Goodbye."
        exit()

    elif choice == 't':
        if len(telescopes):
            print "Existing telescopes:"
            for n, tel in enumerate(telescopes):
                print "   (%d) %s" % (n, tel)

            print "   (%d) Add new" % len(telescopes)
            tel_choice = raw_input('[n]> ')

            if tel_choice == len(telescopes):
                new_telescope()
            else:
                selected_tel_idx = tel_choice        
        else:
            new_telescope()
