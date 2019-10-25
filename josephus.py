#!/usr/bin/python

# Flavius Josephus was a roman historian of Jewish origin.
# During the Jewish-Roman wars of the first century AD, he was in a cave with fellow soldiers, 40 men in all, surrounded by enemy Roman troops.
# They decided to commit suicide by standing in a ring and counting off each third man. Each man so designated was to commit suicide...
# Josephus, not wanting to die, managed to place himself in the position of the last survivor.

# In the general version of the problem, there are n soldiers numbered from 1 to n and each k-th soldier will be eliminated.
# The count starts from the first soldier. What is the number of the last survivor?

class Person:
    def __init__(self,pos):
        self.pos = pos
        self.alive = 1
    def __str__(self):
        return "Person #%d, %s" % (self.pos, self.alive)
    
    # Creates a chain of linked people
    # Returns the last one in the chain
    def createChain(self,n):
        if n>0:
            self.succ = Person(self.pos+1)
            return self.succ.createChain(n-1)
        else:
            return self

    # Kills in a circle, getting every nth living person
    # When there is only one remaining, the lone survivor is returned
    def kill(self,pos,nth,remaining):
        if self.alive == 0: return self.succ.kill(pos,nth,remaining)
        if remaining == 1: return self
        if pos == nth:
            self.alive = 0
            pos=0
            remaining-=1
        return self.succ.kill(pos+1,nth,remaining)

# n people in a circle
# kill every mth person
n = 40
m = 3

first = Person(1)
last = first.createChain(n-1)
last.succ = first

print("In a circle of %d people, killing number %d" % (n, m))
winner = first.kill(1,m,n)
print("Winner: ", winner)
