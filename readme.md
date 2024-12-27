# Logbasic

This package contains some simple helper functions for logging. There are 6 log levels:

* `debug()`
* `warning()`
* `info()`
* `error()`
* `special()`
* `success()`

They work the same way as the default `print()` function. To get just the formatted string add `_string` to the function, e.g. `warning_string()`.

Dictionaries, `timedelta`s and `datetime`s are specially formatted so keep that in mind. dictionaries are formatted like JSON files, timedeltas when negative are formatted as if they were positive but with a minus sign (-) prefixed.
