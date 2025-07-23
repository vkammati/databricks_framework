# Coding Style & Best Practices

- [Index](../README.md)
- [Coding Style \& Best Practices](#coding-style--best-practices)
  - [General concepts](#general-concepts)
    - [Explicit code](#explicit-code)
      - [Bad](#bad)
      - [Good](#good)
    - [One statement per line](#one-statement-per-line)
      - [Bad](#bad-1)
      - [Good](#good-1)
    - [Function arguments](#function-arguments)
    - [Avoid the magical wand](#avoid-the-magical-wand)
    - [We are all responsible users](#we-are-all-responsible-users)
    - [Returning Values](#returning-values)
    - [Naming Conventions](#naming-conventions)
    - [Add meaningful comments](#add-meaningful-comments)
    - [Add Documentation strings](#add-documentation-strings)
      - [Examples](#examples)
    - [Delta-lake Best Practices](#delta-lake-best-practices)
    - [References](#references)

Python is  a highly readable and high level of readability is at the heart of the design of the Python language, following the recognized fact that code is read much more often than it is written.

One reason for the high readability of Python code is its relatively complete set of Code Style guidelines and “Pythonic” idioms.

## General concepts

### Explicit code

While any kind of black magic is possible with Python, the most explicit and straightforward manner is preferred.

#### Bad

```python
def make_complex(*args):
    x, y = args
    return dict(**locals())
```

#### Good

```python
def make_complex(x, y):
    return {'x': x, 'y': y}
```

In the good code above, x and y are explicitly received from the caller, and an explicit dictionary is returned. The developer using this function knows exactly what to do by reading the first and last lines, which is not the case with the bad example.

### One statement per line

While some compound statements such as list comprehensions are allowed and appreciated for their brevity and their expressiveness, it is bad practice to have two disjointed statements on the same line of code.

#### Bad

```python
print('one'); print('two')

if x == 1: print('one')

if <complex comparison> and <other complex comparison>:
    # do something
```

#### Good

```python
print('one')
print('two')

if x == 1:
    print('one')

cond1 = <complex comparison>
cond2 = <other complex comparison>
if cond1 and cond2:
    # do something
```

### Function arguments

Arguments can be passed to functions in four different ways.

1. **Positional arguments** are the simplest form of arguments, have no default values, can be used for the few function arguments that are fully part of the function’s meaning, and their order is natural. For instance, in `send(message, recipient)` or `point(x, y)` the user of the function has no difficulty remembering that those two functions require two arguments, and in which order.
In those two cases, it is possible to use argument names when calling the functions and, doing so, it is possible to switch the order of arguments, calling for instance `send(recipient='World', message='Hello')` and `point(y=2, x=1)` but this reduces readability and is unnecessarily verbose, compared to the more straightforward calls to `send('Hello', 'World')` and `point(1, 2)`.

1. **Keyword arguments** are not mandatory and have default values. They are often used for optional parameters sent to the function. When a function has more than two or three positional parameters, its signature is more difficult to remember and using keyword arguments with default values is helpful. For instance, a more complete send function could be defined as `send(message, to, cc=None, bcc=None)`. Here cc and bcc are optional, and evaluate to None when they are not passed another value.
Calling a function with keyword arguments can be done in multiple ways in Python; for example, it is possible to follow the order of arguments in the definition without explicitly naming the arguments, like in `send('Hello', 'World', 'Cthulhu', 'God')`, sending a blind carbon copy to God. It would also be possible to name arguments in another order, like in `send('Hello again', 'World', bcc='God', cc='Cthulhu')`. Those two possibilities are better avoided without any strong reason to not follow the syntax that is the closest to the function definition: `send('Hello', 'World', cc='Cthulhu', bcc='God')`.
As a side note, following the [YAGNI principle](https://en.wikipedia.org/wiki/You_aren%27t_gonna_need_it), it is often harder to remove an optional argument (and its logic inside the function) that was added “just in case” and is seemingly never used, than to add a new optional argument and its logic when needed.

3. **The arbitrary argument** list is the third way to pass arguments to a function. If the function intention is better expressed by a signature with an extensible number of positional arguments, it can be defined with the `*args` constructs. In the function body, args will be a tuple of all the remaining positional arguments. For example, `send(message,*args)` can be called with each recipient as an argument: `send('Hello', 'God', 'Mom', 'Cthulhu')`, and in the function body args will be equal to ('God', 'Mom', 'Cthulhu').
However, this construct has some drawbacks and should be used with caution. If a function receives a list of arguments of the same nature, it is often more clear to define it as a function of one argument, that argument being a list or any sequence. Here, if send has multiple recipients, it is better to define it explicitly: `send(message, recipients)` and call it with `send('Hello', ['God', 'Mom', 'Cthulhu'])`. This way, the user of the function can manipulate the recipient list as a list beforehand, and it opens the possibility to pass any sequence, including iterators, that cannot be unpacked as other sequences.

4. **The arbitrary keyword argument dictionary** is the last way to pass arguments to functions. If the function requires an undetermined series of named arguments, it is possible to use the **kwargs construct. In the function body, kwargs will be a dictionary of all the passed named arguments that have not been caught by other keyword arguments in the function signature.
The same caution as in the case of arbitrary argument list is necessary, for similar reasons: these powerful techniques are to be used when there is a proven necessity to use them, and they should not be used if the simpler and clearer construct is sufficient to express the function’s intention.
It is up to the programmer writing the function to determine which arguments are positional arguments and which are optional keyword arguments, and to decide whether to use the advanced techniques of arbitrary argument passing. If the advice above is followed wisely, it is possible and enjoyable to write Python functions that are:
   - easy to read (the name and arguments need no explanations)
   - easy to change (adding a new keyword argument does not break other parts of the code)

### Avoid the magical wand

A powerful tool for hackers, Python comes with a very rich set of hooks and tools allowing you to do almost any kind of tricky tricks. For instance, it is possible to do each of the following:

change how objects are created and instantiated
change how the Python interpreter imports modules
It is even possible (and recommended if needed) to embed C routines in Python.
However, all these options have many drawbacks and it is always better to use the most straightforward way to achieve your goal. The main drawback is that readability suffers greatly when using these constructs. Many code analysis tools, such as pylint or pyflakes, will be unable to parse this “magic” code.

We consider that a Python developer should know about these nearly infinite possibilities, because it instills confidence that no impassable problem will be on the way. However, knowing how and particularly when not to use them is very important.

### We are all responsible users

As seen above, Python allows many tricks, and some of them are potentially dangerous. A good example is that any client code can override an object’s properties and methods: there is no “private” keyword in Python. This philosophy, very different from highly defensive languages like Java, which give a lot of mechanisms to prevent any misuse, is expressed by the saying: “We are all responsible users”.
Python community prefers to rely on a set of conventions indicating that these elements should not be accessed directly.
    - The main convention for private properties and implementation details is to prefix all “internals” with an underscore.
    - Any method or property that is not intended to be used by client code should be prefixed with an underscore

### Returning Values

1. When a function grows in complexity, it is not uncommon to use multiple return statements inside the function’s body. However, in order to keep a clear intent and a sustainable readability level, **it is preferable to avoid returning meaningful values from many output points in the body**.
2. If you do not wish to raise exceptions for the second case, then returning a value, such as None or False, indicating that the function could not perform correctly might be needed.
3. It is better to return as early as the incorrect context has been detected

```python
def complex_function(a, b, c):
    if not a:
        return None  # Raising an exception might be better
    if not b:
        return None  # Raising an exception might be better
    # Some complex code trying to compute x from a, b and c
    # Resist temptation to return x if succeeded
    if not x:
        # Some Plan-B computation of x
    return x  # One single exit point for the returned value x will help
              # when maintaining the code.
```

### Naming Conventions

It is neccessary to follow naming conventions so that it'll be easier for all developer to navigate through code and debug.

Please refer to this guide for [PEP8 Naming Conventions](https://pep8.org/#naming-conventions)

### Add meaningful comments

Comments that contradict the code are worse than no comments. Always make a priority of keeping the comments up-to-date when the code changes!

Please refere to this guide for [PEP8 Comments](https://pep8.org/#comments)

### Add Documentation strings

Python uses docstring to document code.
Write docstrings for all public modules, functions, classes, and methods. Docstrings are not necessary for non-public methods, but you should have a comment that describes what the method does. This comment should appear after the **`def`** line.

#### Examples

```python
def fetch_smalltable_rows(table_handle: smalltable.Table,
                          keys: Sequence[Union[bytes, str]],
                          require_all_keys: bool = False,
) -> Mapping[bytes, tuple[str, ...]]:
    """Fetches rows from a Smalltable.

    Retrieves rows pertaining to the given keys from the Table instance
    represented by table_handle.  String keys will be UTF-8 encoded.

    Args:
        table_handle: An open smalltable.Table instance.
        keys: A sequence of strings representing the key of each table
          row to fetch.  String keys will be UTF-8 encoded.
        require_all_keys: If True only rows with values set for all keys will be
          returned.

    Returns:
        A dict mapping keys to the corresponding table row data
        fetched. Each row is represented as a tuple of strings. For
        example:

        {b'Serak': ('Rigel VII', 'Preparer'),
         b'Zim': ('Irk', 'Invader'),
         b'Lrrr': ('Omicron Persei 8', 'Emperor')}

        Returned keys are always bytes.  If a key from the keys argument is
        missing from the dictionary, then that row was not found in the
        table (and require_all_keys must have been False).

    Raises:
        IOError: An error occurred accessing the smalltable.
    """
```

```python
class SampleClass:
    """Summary of class here.

    Longer class information...
    Longer class information...

    Attributes:
        likes_spam: A boolean indicating if we like SPAM or not.
        eggs: An integer count of the eggs we have laid.
    """

    def __init__(self, likes_spam: bool = False):
        """Inits SampleClass with blah."""
        self.likes_spam = likes_spam
        self.eggs = 0

    def public_method(self):
        """Performs operation blah."""
```

```python
# Yes:
class CheeseShopAddress:
  """The address of a cheese shop.

  ...
  """

class OutOfCheeseError(Exception):
  """No more cheese is available."""
# No:
class CheeseShopAddress:
  """Class that describes the address of a cheese shop.

  ...
  """

class OutOfCheeseError(Exception):
  """Raised when no more cheese is available."""

```

### Delta-lake Best Practices

1. https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices

### References


1. https://google.github.io/styleguide/pyguide.html