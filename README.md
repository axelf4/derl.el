# derl

Derl is an Emacs Lisp library for communicating with Erlang nodes,
that provides an Erlang-like process concurrency abstraction.
In other words, it is full rewrite of the library part of [Distel],
differing in the following ways:

* Processes run generator functions defined via the `generator` package,
  meaning they do not have to be manually written in trampolined style.

  This makes `receive` expressions easier to write
  since the `saved-vars` argument becomes unnecessary.
* Pattern matching for `receive` expressions uses `pcase` patterns,
  instead of a bespoke syntax.
* Processes are not backed by Emacs buffers.

  Therefore, the main Emacs Lisp execution context can be an implicit process,
  meaning it is possible to use `receive` everywhere
  instead of only in functions passed to `spawn`.
* Works with newer Erlang/OTP releases (at least 25 and 26).
* Modernized code base (e.g. uses lexical binding).

## Planned features

Some features of the Distel extended Erlang editing mode are planned:

* [ ] Expression evaluation
* [ ] Completion-at-point function
* [ ] Xref backend
* [ ] Eldoc documentation function
* [ ] Reloading modified modules

Everything else, such as debugger and profiler integration, is out of scope,
with the intention that the public API should allow
for those things to be implemented separately.

[Distel]: https://github.com/massemanet/distel
