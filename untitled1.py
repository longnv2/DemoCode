# -*- coding: utf-8 -*-
"""
Created on Wed Aug 30 10:14:13 2017

@author: LongNV
"""

def fahrenheit(T):
    return ((float(9)/5)*T + 32)



def celsius(T):
    return (float(5)/9)*(T-32)


temp = (36.5, 37, 37.5,39)

F = map(fahrenheit, temp)

C = map(celsius, F)

print(C)



print(F)


import Iterator

def map(line):
    fields = line.split(",")
    print(fields.isArtificial, 1)

def reduce(isArtificial, totals):
    print(isArtificial, sum(totals))

reduce('TRUE', Iterator(1, 1, 1, 1))
