<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
#   Table of Content

- [Name](#name)
- [Status](#status)
- [Synopsis](#synopsis)
- [Methods](#methods)
  - [dictutil.depth_iter](#dictutildepth_iter)
  - [dictutil.breadth_iter](#dictutilbreadth_iter)
  - [dictutil.make_getter](#dictutilmake_getter)
  - [dictutil.make_setter](#dictutilmake_setter)
    - [Synopsis](#synopsis-1)
- [Author](#author)
- [Copyright and License](#copyright-and-license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

#   Name

dictutil

It provides with several dict operation functions.

#   Status

This library is considered production ready.

#   Synopsis

Depth first search a dictionary

```python
from pykit import dictutil

mydict = {'a':
             {'a.a': 'v-a.a',
              'a.b': {'a.b.a': 'v-a.b.a'},
              'a.c': {'a.c.a': {'a.c.a.a': 'v-a.c.a.a'}}
             }
         }

# depth-first iterative the dict
for rst in dictutil.depth_iter(mydict):
    print rst

# output:
#     (['a', 'a.c', 'a.c.a', 'a.c.a.a'], 'v-a.c.a.a')
#     (['a', 'a.b', 'a.b.a'], 'v-a.b.a')
#     (['a', 'a.a'], 'v-a.a')
```

Breadth first search a dictionary

```python
for rst in dictutil.breadth_iter(mydict):
    print rst

# output:
#     (['a'],                            {'a.c': {'a.c.a': {'a.c.a.a': 'v-a.c.a.a'}}, 'a.b': {'a.b.a': 'v-a.b.a'}, 'a.a': 'v-a.a'})
#     (['a', 'a.a'],                     'v-a.a')
#     (['a', 'a.b'],                     {'a.b.a': 'v-a.b.a'})
#     (['a', 'a.b', 'a.b.a'],            'v-a.b.a')
#     (['a', 'a.c'],                     {'a.c.a': {'a.c.a.a': 'v-a.c.a.a'}})
#     (['a', 'a.c', 'a.c.a'],            {'a.c.a.a': 'v-a.c.a.a'})
#     (['a', 'a.c', 'a.c.a', 'a.c.a.a'], 'v-a.c.a.a')

#
```

Make a predefined dictionary item getter.

```python
import dictutil

records = [
    {"event": 'log in',
     "time": {"hour": 10, "minute": 30, }, },

    {"event": 'post a blog',
     "time": {"hour": 10, "minute": 40, }, },

    {"time": {"hour": 11, "minute": 20, }, },

    {"event": 'log out',
     "time": {"hour": 11, "minute": 20, }, },
]

get_event = dictutil.make_getter('event', default="NOTHING DONE")
get_time = dictutil.make_getter('time.$field')

for record in records:

    ev = get_event(record)

    tm = "%d:%d" % (get_time(record, {"field": "hour"}),
                    get_time(record, {"field": "minute"}))

    print "{ev:<12}   at {tm}".format(ev=ev, tm=tm)

# output:
# log in         at 10:30
# post a blog    at 10:40
# NOTHING DONE   at 11:20
# log out        at 11:20
```

#   Methods

## dictutil.depth_iter

**syntax**:
`dictutil.depth_iter(mydict, ks=None, maxdepth=10240, intermediate=False)`

**arguments**:

-   `mydict`:
    the dict that you want to iterate on.

-   `ks`:
    the argument could be a `list`,  it would be seted ahead of key's list in
    results of iteration

    ```python
    for rst in dictutil.depth_iter(mydict, ks=['mykey1','mykey2']):
        print rst

    # output:
    #     (['mykey1', 'mykey2', 'k1', 'k13', 'k131', 'k1311'], 'v-a.c.a.a')
    #     (['mykey1', 'mykey2', 'k1', 'k12', 'k121'], 'v-a.b.a')
    #     (['mykey1', 'mykey2', 'k1', 'k11'], 'v-a.a')

   ```

-   `maxdepth`:
    specifies the max depth of iteration.

    ```python
    for rst in dictutil.depth_iter(mydict, maxdepth=2):
        print rst

    # output
    #     (['k1', 'k13'], {'k131': {'k1311': 'v-a.c.a.a'}})
    #     (['k1', 'k12'], {'k121': 'v-a.b.a'})
    #     (['k1', 'k11'], 'v-a.a')
    ```

-   `intermediate`:
    if it is `True`, the method will show the intermediate key path those
    points to a non-leaf descendent.
    By default it is `False`.

   ```python
    mydict = {'a':
                 {'a.a': 'v-a.a',
                  'a.b': {'a.b.a': 'v-a.b.a'},
                  'a.c': {'a.c.a': {'a.c.a.a': 'v-a.c.a.a'}}
                 }
             }
   for keys, vals in dictutil.depth_iter(mydict, intermediate=True):
       print keys

   # output:
   #     ['a']                              # intermediate
   #     ['a', 'a.a']
   #     ['a', 'a.b']                       # intermediate
   #     ['a', 'a.b', 'a.b.a']
   #     ['a', 'a.c']                       # intermediate
   #     ['a', 'a.c', 'a.c.a']              # intermediate
   #     ['a', 'a.c', 'a.c.a', 'a.c.a.a']

   ```

**return**:
an iterator. Each element it yields is a tuple of keys and value.

## dictutil.breadth_iter

**syntax**:
`dictutil.breadth_iter(mydict)`

**arguments**:

-   `mydict`:
    the dict you want to iterative

**return**:
an iterator, each element it yields is a tuple that contains keys and value.

##  dictutil.make_getter

**syntax**:
`dictutil.make_getter(key_path, default=0)`

It creates a lambda that returns the value of the item specified by
`key_path`.

```python
get_hour = dictutil.make_getter('time.hour')
print get_hour({"time": {"hour": 11, "minute": 20}})
# 11

get_minute = dictutil.make_getter('time.minute')
print get_minute({"time": {"hour": 11, "minute": 20}})
# 20

get_second = dictutil.make_getter('time.second', default=0)
print get_second({"time": {"hour": 11, "minute": 20}})
# 0
```


**arguments**:

-   `key_path`:
    is a dot separated path string of key hierarchy to get an item from a
    dictionary.

    Example: `foo.bar` is same as `some_dict["foo"]["bar"]`.

-   `default`:
    is the default value if the item is not found.
    For example when `foo.bar` is used on a dictionary `{"foo":{}}`.

    It must be a primitive value such as `int`, `float`, `bool`, `string` or `None`.

**return**:
the item value found by key_path, or the default value if not found.

##  dictutil.make_setter

**syntax**:
`dictutil.make_setter(key_path, default=None, incr=False)`

It creates a function `setter(dic, value=None, vars={})` that can be used to
set(or increment) the item value specified by `key_path` in a dictionary `dic`.

### Synopsis

```python
tm = {"time": {"hour": 0, "minute": 0}}

set_hour = dictutil.make_setter('time.hour')
set_hour(tm, 12)
print tm
# {"time": {"hour": 12, "minute": 0}}

incr_minute = dictutil.make_setter('time.minute', incr=True)
incr_minute(tm, 1)
print tm
# {"time": {"hour": 12, "minute": 1}}

incr_minute(tm, 2)
print tm
# {"time": {"hour": 12, "minute": 3}}
```

**arguments**:

-   `key_path`:
    is a dot separated key path to locate an item in a dictionary.

    Example: `foo.bar` is same as `some_dict["foo"]["bar"]`.

-   `value`:
    is the value to use if `setter` is called with its own `value` set to `None`.

    `value` can be a `callable`, such as `function` or `lambda`.
    If it is a `callable`, it must be able to accept one argument `vars`.

    `vars` is passed to the `setter` by the caller.

    ```python
    set_minute = dictutil.make_setter('time.minute', value=lambda vars: int(time.time()) % 3600 / 60)
    tm = {"time": {"hour": 11, "minute": 20}}
    print set_minute(tm)
    # current time minute
    ```

-   `incr`:
    specifies whether the value should be overwritten(`incr=False`) or
    added to present value(`incr=True`).

    If `incr=True`, `value` must supports plus operation: `+`, such as a `int`,
    `float`, `string`, `tuple` or `list`.

**return**:
a function `setter(dic, value=None, vars={})` that can be used to set an item
value in a dictionary to `value`(or to the `value` that is passed to
`make_setter`, if the `value` passed to setter is `None`).

`vars` is a dictionary that contains dynamic item keys.

`setter` returns the result value.

```python
_set = dictutil.make_setter('time.$subfield')
tm = {"time": {"hour": 0, "minute": 0}}

# set minute:
print _set(tm, 22, vars={'subfield': 'minute'})
# {"time": {"hour": 0, "minute": 22}}

# set hour:
print _set(tm, 15, vars={'subfield': 'hour'})
# {"time": {"hour": 15, "minute": 0}}
```


#   Author

break wang (王显宝) <breakwang@outlook.com>

#   Copyright and License

The MIT License (MIT)

Copyright (c) 2017 break wang (王显宝) <breakwang@outlook.com>