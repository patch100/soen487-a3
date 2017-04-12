# SOEN 487 Assignment 3
#### Text file used: Grimm's Fairy Tales by Various
#### Found at https://www.gutenberg.org/ebooks/52521

## Instructions

### Copy files
##### Copy the text file and the jar file to the cloudera VM desktop.

### Copy text file to hadoop fs
##### Open terminal
##### hadoop fs -copyFromLocal grimm.txt /user/training/

### Run application
This command prints out the top 10 words in the book.
##### ﻿hadoop jar assignment3.jar assignment3.WordCount /user/training/grimm.txt grimm_top_N 10

### Top Words are:
#### the	    5896
#### and	    4260
#### was	    1374
#### she	    1162
#### her	    1112
#### you	    1022
#### had	    939
#### said    894
#### that    853
#### with    790
