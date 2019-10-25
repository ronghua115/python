#!/usr/bin/python

def fibonacci(n=5):  # write Fibonacci series up to n
    """Print a Fibonacci series up to n."""
    result = []
    a, b = 0, 1
    while a < n:
        result.append(a)
        a, b = b, a + b
    return result


# test
'''
fib = fibonacci(10)
print fib

fib = fibonacci()
print fib
'''


def doc_function():
    """Do nothing, but document it.

    No, really, it doesn't do anything.
    """
    pass


# test
'''
print doc_function.__doc__
'''
