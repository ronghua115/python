#!/usr/bin/python

def fibonacci(n) : # write Fibonacci series up to n
    """Print a Fibonacci series up to n."""
    result = []
    a, b = 0, 1
    while a < n :
        result.append(a)
        a, b = b, a + b
    return result

fib = fibonacci(5)
print fib
